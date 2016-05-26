# -*- coding: utf-8 -*-
# Copyright © 2014-2016 Digital Catapult and The Copyright Hub Foundation
# (together the Open Permissions Platform Coalition)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

"""
Responsible for periodically polling repository services for data to populate
the index.
"""
from datetime import datetime
import heapq
import logging
import os
import Queue
import shelve
import random

import dateutil.parser
from tornado.gen import coroutine, sleep, Return
from tornado.ioloop import IOLoop
from tornado.options import define, options
from tornado.httpclient import HTTPError
import koi
from koi.configure import log_config, configure_syslog, ssl_server_options
from chub import API
from chub.oauth2 import Read, get_token

from .models import db

define('url_accounts', help='The accounts service URL')
define('accounts_poll_interval', default=60 * 60 * 24,
       help='Seconds between requests to get repository services registered in'
            ' the accounts service')
define('default_poll_interval', default=60 * 60 * 6,
       help='Default seconds between requests to a repository service')
define('max_poll_error_delay_factor', default=5,
       help='Maximum multiplication factor to delay requests to repositories '
       'that responded with an error')
define('notification_poll_interval', default=0.1,
       help='Time to wait before rechecking for notifications from other threads')
define('concurrency', default=2,
       help='Number of workers making requests to repositories')
define('notify_queue_overload_warning', default=2,
       help='Display a warning if the notification queue starts to a grow (min value should be close to concurrency)')
define('local_db', default='repos_shelf.db', help='Path to shelf database')
define('notify_min_delay', default=(60 * 60 * 6) / 10, help='Min delay between checks with notify')
define('open_service', default=True, help='If the service is open then repositories not associated with '
                                          'this index can send notification to this index')


class Notification(object):
    """Responsible for notifying the scheduler"""

    def __init__(self, notification_q=None):
        self.notification_q = notification_q
        self._scheduler = None

    def connect_with(self, scheduler):
        """
        Connect with a scheduler that handle that accepts jobs for repositories
        sending notifications
        """
        self._scheduler = scheduler

    def put_nowait(self, repo_id):
        """Put (no wait) dropping silently"""
        try:
            self.notification_q.put_nowait(repo_id)
        except Queue.Full:
            logging.warning('Notification from repository service {} dropped '
                            'because the queue is full'.format(repo_id))
            pass

    def get_nowait(self):
        """Get (no wait) returning None if empty"""
        try:
            return self.notification_q.get_nowait()
        except Queue.Empty:
            pass

    def start(self):
        """
        Starts the process of the scheduler - linking
        notifications received from webworkers with the
        scheduler.
        """
        io_loop = IOLoop.current()
        io_loop.add_callback(self.check_for_notifications)

    @coroutine
    def check_for_notifications(self):
        """
        Coroutine thread that
        receives notifications and integrate them in the scheduling.
        """
        while True:
            yield self._check_for_notifications()
            yield sleep(options.notification_poll_interval)

    @coroutine
    def _check_for_notifications(self, max_notifications=20):
        """
        Receives notifications and integrate them in the scheduling.
        """
        repo_id = self.get_nowait()
        while repo_id and max_notifications > 0:
            yield self._scheduler.reschedule(repo_id, options.notify_min_delay)
            logging.info("Received notification from %s " % (repo_id,))
            repo_id = self.get_nowait()
            max_notifications -= 1

        try:
            qsize = self.notification_q.qsize()
            if qsize >= options.notify_queue_overload_warning:
                logging.info("%d elements in the notification queue %s " % (qsize,))
        except NotImplementedError:
            # qsize not implemented for OSX
            # see: https://docs.python.org/2/library/multiprocessing.html#multiprocessing.Queue.qsize
            pass


class Scheduler(object):
    """Responsible for scheduling"""

    REMOVED = '/task removed/'

    def __init__(self):
        self._items = {}
        self._priority_q = []
        self._time = IOLoop.current().time

    def _remove_item(self, item_id):
        """Flag an item in the _priority_q as removed"""
        try:
            item = self._items[item_id]
        except KeyError:
            return
        item[-1] = self.REMOVED

    @coroutine
    def reschedule(self, item_id, when=None):
        """Reschedule an item (act only if not already scheduled before)"""
        now = self._time()
        item = self._items.get(item_id)
        if not item or (item and item[0] > (now + when)):
            yield self.schedule(item_id, when)

    @coroutine
    def schedule(self, item_id, when=None):
        """Schedule an item to be available for work"""
        if when is None:
            # If when is none the job is scheduled as not urgent
            when = random.uniform(0, options.default_poll_interval)
        when = self._time() + when

        if item_id in self._items:
            self._remove_item(item_id)

        item = [when, item_id]
        heapq.heappush(self._priority_q, item)
        self._items[item_id] = item

    @coroutine
    def get(self, n=1):
        """
        Gets the next n items

        :param n: maximum number of items to get, defaults to 1. If items are
            scheduled in the future less than n items might be returned.
        """
        items = []
        now = self._time()

        while len(items) < n:
            try:
                item = heapq.nsmallest(1, self._priority_q)
                time, repo_id = item[0]
            except IndexError:
                break

            if time > now:
                break

            heapq.heappop(self._priority_q)

            if repo_id != self.REMOVED:
                items.append(repo_id)

        raise Return(items)


class RepositoryStore(object):
    """Responsible for storing information about repositories"""

    def __init__(self, api_client=None):
        if api_client is None:
            api_client = API(options.url_accounts,
                             ssl_options=ssl_server_options())

        self._api = api_client
        self._endpoint = self._api.accounts.repositories
        self._shelf = shelve.open(options.local_db, writeback=True)
        self.on_new_repo = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        self._shelf.close()

    def start(self):
        """
        Starts a worker that periodically requests information about
        repositories from the account service
        """
        io_loop = IOLoop.current()
        io_loop.add_callback(self._fetch_repositories_forever)

    @coroutine
    def _fetch_repositories_forever(self):
        """
        Worker periodically asking the account service about new repositories.
        """

        while True:
            yield self._fetch_repositories()
            yield sleep(options.accounts_poll_interval)

    @coroutine
    def _fetch_repositories(self):
        """
        Get repositories from the accounts service and publish new ones to the
        queue

        TODO: Decide what should happen if a repo is removed from accounts service?
        """
        logging.info('Getting repositories from accounts service')

        try:
            repositories = yield self._endpoint.get()
        except Exception:
            logging.exception('Error fetching repositories')
            raise Return()

        for repo in repositories['data']:
            repo_id = str(repo['id'])

            if repo_id not in self.get_repositories():
                self._set_repository(repo_id, repo)
                if self.on_new_repo:
                    logging.info("Adding new repository " + repo_id)
                    yield self.on_new_repo(repo_id)

    def _set_repository(self, repo_id, repository):
        """Stores shared metadata
        :param repo_id: The repository identifier
        :param repository: The repository record
        """
        self._shelf[str(repo_id)] = repository

    def fail(self, repo_id, reason=None):
        """
        Record failure to fetch identifiers from a repository

        If a reason is provided then it is used when logging the failure

        :param repo_id: the repository ID
        :param reason: (optional) reason for the failure
        :returns: repository dict
        :raises KeyError: if an unknown repository
        """
        repository = self._get_repository(repo_id)
        repository['errors'] = repository.get('errors', 0) + 1

        self._set_repository(repo_id, repository)

        if not reason:
            reason = 'Error while fetching repo {}'.format(repo_id)

        logging.warning(reason)

        return repository

    def success(self, repo_id, next_query_start=None):
        """
        Record success fetching identifiers from a repository

        Resets the error count to 0

        :param repo_id: the repository ID
        :param next_query_start: (optional) datetime, the start of the query
            range the next time the repository is queried
        :returns: repository dict
        :raises KeyError: if an unknown repository
        """
        repository = self._get_repository(repo_id)
        changes = {
            'next': next_query_start,
            'last': IOLoop.current().time(),
            'errors': 0,
            'successful_queries': repository.get('successful_queries', 0) + 1
        }
        repository.update(changes)

        self._set_repository(repo_id, repository)

        logging.info('Succesfully queried repo {}'.format(repo_id))

        return repository

    def get_repositories(self):
        """
        Returns a list of information related to the the repositories
        that are to be indexed by this service.
        """
        return self._shelf.keys()

    def _get_repository(self, repo_id):
        """
        Returns information about a specific repository already known by this service.
        """
        return self._shelf[str(repo_id)]

    @coroutine
    def _fetch_repository(self, repo_id):
        """
        Fetches a repository from the accounts service and updates the
        internal data store

        :param repo_id: a repository ID
        :returns: a dictionary, or None if unable to fetch the repository
        """
        try:
            response = yield self._endpoint[repo_id].get()
            try:
                repo = self._get_repository(repo_id)
            except Exception:
                repo = {}

            repo.update(response['data'])
            self._set_repository(repo_id, repo)
        except HTTPError as exc:
            logging.warning('Could not fetch repository {repo}: {exception}'
                            .format(repo=repo_id, exception=exc))
            repo = None

        raise Return(repo)

    @coroutine
    def get_repository(self, repo_id):
        """
        Get the repository from the internal data store

        If a repository is not in the data store and the service is
        options.open_service is True, then the accounts service is queried

        :param repo_id: a repository ID
        :returns: repository dictionary or None
        """
        repo = None
        try:
            repo = self._get_repository(repo_id)
        except KeyError as exc:
            logging.warning("Scheduled unknown repository '{repo}'"
                            .format(repo=repo_id))

            if options.open_service:
                repo = yield self._fetch_repository(repo_id)

            if not repo:
                raise exc

        raise Return(repo)


class Manager(object):
    """
    Periodically fetches repositories from the accounts service and populates
    the index database with new data from each repository service. If a
    notification of new data is received from a repository service, then the
    service may be polled sooner than scheduled.

    The timestamp of the most recent data in the repsoitory service is
    persisted to disk using the standard library's shelve module. The timestamp
    is used to limit the date range in future requests to the repository
    service.
    """
    DEFAULT_FROM_TIME = datetime(2000, 1, 1)

    def __init__(self, database, repositories, scheduler):
        self.db = database
        self.repositories = repositories
        self.scheduler = scheduler
        self.repositories.on_new_repo = scheduler.schedule

        interval = options.default_poll_interval
        self.poll_interval_range = (0.5 * interval, interval)

        if options.max_poll_error_delay_factor < 1:
            raise ValueError('max_poll_error_delay_factor must be >= 1')

        self.max_error_delay_factor = options.max_poll_error_delay_factor

    def start(self):
        """
        Start workers that will fetch identifiers from repositories
        """
        io_loop = IOLoop.current()
        io_loop.add_callback(self._schedule_all_repositories)
        io_loop.add_callback(self.fetch_forever)

    @coroutine
    def _schedule_all_repositories(self):
        """
        Ensures all repository known by the repository manager are scheduled.
        """
        repos = self.repositories.get_repositories()
        for repo_id in repos:
            yield self.scheduler.schedule(repo_id)

    @coroutine
    def fetch_identifiers(self, repo_id):
        """
        Fetch identifiers from a repository service

        :param repo_id: the repository id
        :returns: metadata about the service
        """

        repo = yield self.repositories.get_repository(repo_id)

        location = repo.get('service', {}).get('location')
        if not location:
            msg = 'Repository {} has an unknown location'.format(repo_id)
            meta = self.repositories.fail(repo_id, msg)
            raise Return(meta)

        from_time = repo.get('next')
        result_to = yield self._fetch_identifiers(repo_id, from_time, location)

        if result_to:
            result_to = dateutil.parser.parse(result_to)

        meta = self.repositories.success(repo_id, result_to)
        raise Return(meta)

    @coroutine
    def _fetch_identifiers(self, repo_id, from_time, location):
        result_to = None
        client = yield repository_service_client(location)
        endpoint = client.repository.repositories[repo_id].assets.identifiers
        from_time = from_time or self.DEFAULT_FROM_TIME
        logging.info('Getting IDs for {} from {}'.format(repo_id, location))
        query_dict = {'page': 0, 'from': from_time.isoformat()}
        while True:
            query_dict['page'] += 1
            result = yield endpoint.get(**query_dict)
            data = result.get('data')

            if not data:
                break

            yield self.db.add_entities('asset', data, repo_id)
            # Store the end of the range for the last query so that it can be
            # used as the start for the next time the endpoint is queried
            _, result_to = result['metadata'].get('result_range', (None, None))
        raise Return(result_to)

    @coroutine
    def fetch_forever(self):
        """Fetch entities from repository services and reschedule"""
        while True:
            ids = yield self.scheduler.get(options.concurrency)
            yield [self.fetch(repo_id) for repo_id in ids]
            yield sleep(min(options.notification_poll_interval, 1))

    @coroutine
    def fetch(self, repo_id):
        if repo_id is None:
            raise Return()

        try:
            repo_meta = yield self.fetch_identifiers(repo_id)
        except KeyError:
            # unknown repositories are not rescheduled
            raise Return(None)
        except Exception:
            repo_meta = self.repositories.fail(repo_id)

        self.scheduler.schedule(repo_id, self._next_poll_interval(repo_meta))
        raise Return(repo_id)

    def _next_poll_interval(self, repo_meta):
        """
        Compute the time to the next poll for this repo.

        If there were no errors this function will return a random float
        in the poll_interval_range. If there was an error, the interval
        will be multiplied by the number of errors up to the
        max_error_delay_factor
        """
        errors = repo_meta.get('errors', 0)
        delay_factor = min(errors or 1, self.max_error_delay_factor)
        return delay_factor * random.uniform(*self.poll_interval_range)


@coroutine
def repository_service_client(location):
    """
    get an api client for a repository service
    :params location: base url of the repository
    """
    token = yield get_token(
        options.url_auth, options.service_id,
        options.client_secret, scope=Read(),
        ssl_options=ssl_server_options()
    )
    client = API(location, token=token, ssl_options=ssl_server_options())
    raise Return(client)


def main(database, notification=None):
    """
    Main entry point for the process crawling the repositories
    """
    configure_syslog()
    log_config()
    io_loop = IOLoop.current()

    scheduler = Scheduler()
    with RepositoryStore() as repositorystore:
        manager = Manager(database, repositorystore, scheduler)

        repositorystore.start()
        manager.start()

        if notification:
            notification.connect_with(scheduler)
            notification.start()

        io_loop.start()


if __name__ == '__main__':
    # Load application config
    CONF_DIR = os.path.join(os.path.dirname(__file__), '../config')
    koi.load_config(CONF_DIR)

    db = db.DbInterface(
        options.url_index_db,
        options.index_db_port,
        options.index_db_path,
        options.index_schema)

    notification_q = Queue.Queue()
    notification = Notification(notification_q)
    main(db, notification)
