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

from datetime import datetime
import Queue

import pytest
from mock import call, patch, Mock, MagicMock
from koi.test_helpers import make_future, gen_test
from tornado.options import define

from index import repositories


define('ssl_ca_cert', default='')


def frange(x, y, jump):
    while x < y:
        yield x
        x += jump


def mock_manager(use_clock=False):
    """
    Returns an instance of repositories.Manager with a mock database, an
    empty shelf (using a dict instead of an actual shelf instance), and
    an example repository
    """
    repostore = repositories.RepositoryStore({})
    repostore._shelf = {
        'repo_a': {'id': 'repo_a', 'service': {'location': 'http://a.test'}}
    }
    queue = Queue.Queue()
    notification_q = repositories.Notification(queue)
    scheduler = repositories.Scheduler()
    scheduler._use_clock = use_clock
    if use_clock:
        scheduler._time = iter(frange(0, 100000, 0.5)).next
    notification_q.connect_with(scheduler)
    manager = repositories.Manager(Mock(), repostore, scheduler)
    manager.db.add_entities.return_value = make_future({'errors': []})
    return scheduler, repostore, manager


def mock_identifiers(pages=None):
    """
    Return a function that will return the date from each page in the pages
    dictionary

    Defaults to a page with no data.

    :param pages: a dictionary with page numbers as keys and example responses
    as values.
    """
    if pages is None:
        pages = {}

    def func(page=1, **kwargs):
        return make_future(pages.get(page, {'data': [], 'metadata': {}}))

    return func


@patch('index.repositories.API')
@patch('index.repositories.shelve', MagicMock())
@patch('index.repositories.koi', MagicMock())
@gen_test
def test_unknown_repository(API):
    """
    If a repository is not in the RepositoryStore then the manager should
    not try to fetch the repository, not add it to the repository store and
    not reschedule it. We don't want to keep trying to fetch unknown
    repositories.

    This scenario can arise because a notification is always scheduled to be
    fetched on the assumption that we will be able to find an unknown
    repository in the accounts service.
    """
    scheduler = MagicMock()
    scheduler.get.return_value = make_future('repo1')
    store = repositories.RepositoryStore({})
    store._shelf = {}
    # get_repository raises a KeyError if repo is unknown
    store.get_repository = MagicMock(side_effect=KeyError)

    manager = repositories.Manager(MagicMock(), store, scheduler)
    yield manager.fetch('repo1')

    assert not API().repository.repositories[''].assets.identifiers.called
    assert not scheduler.schedule.called
    assert store._shelf == {}


@patch('index.repositories.logging')
@patch('index.repositories.koi')
@patch('index.repositories.API')
@gen_test
def test_not_called_if_repo_missing_location(API, koi, logging):
    """Don't insert data if the response was empty"""
    scheduler, repostore, manager = mock_manager()
    repostore._shelf['repo_a']['service']['location'] = None

    yield manager.fetch_identifiers('repo_a')

    assert not manager.db.add_entities.called
    assert not API().repository.repositories.__getitem__().assets.identifiers.get.called
    assert logging.warning.called


@patch('index.repositories.repository_service_client')
@patch('index.repositories.koi')
@gen_test
def test_db_not_called_if_no_data(koi, repository_service_client):
    """Don't insert data if the response was empty"""
    client = MagicMock()
    repository_service_client.return_value = make_future(client)
    endpoint = client.repository.repositories.__getitem__().assets.identifiers
    endpoint.get.side_effect = mock_identifiers()
    scheduler, repostore, manager = mock_manager()

    yield manager.fetch_identifiers('repo_a')

    from_time = manager.DEFAULT_FROM_TIME.isoformat()
    endpoint.get.assert_called_once_with(**{'page': 1, 'from': from_time})
    assert not manager.db.add_entities.called


@patch('index.repositories.repository_service_client')
@patch('index.repositories.koi')
@gen_test
def test_single_page_with_data(koi, repository_service_client):
    """
    Store data in the database and check if data in the next page. The result
    date range should be stored so that it can be used as the start of the date
    range the next time the repository is queried.
    """
    client = MagicMock()
    repository_service_client.return_value = make_future(client)
    endpoint = client.repository.repositories.__getitem__().assets.identifiers
    endpoint.get.side_effect = mock_identifiers({
        1: {'data': ['some data'],
            'metadata': {'result_range': ('2000-01-01', '2010-01-01')}}
    })
    scheduler, repostore, manager = mock_manager()

    yield manager.fetch_identifiers('repo_a')

    # Should have called API twice, stopped after page 2 because there was
    # no further data
    from_time = manager.DEFAULT_FROM_TIME.isoformat()
    endpoint.get.assert_has_calls([call(**{'page': 1, 'from': from_time}),
                                   call(**{'page': 2, 'from': from_time})])

    # Only one set of data should have been inserted into the database
    manager.db.add_entities.assert_called_once_with('asset',
                                                    ['some data'],
                                                    'repo_a')

    # Check date was stored for the next time the endpoint is scheduled
    assert repostore._shelf['repo_a']['next'] == datetime(2010, 1, 1)


@patch('index.repositories.repository_service_client')
@patch('index.repositories.koi')
@gen_test
def test_three_pages_with_data(koi, repository_service_client):
    """
    Store data in the database and check if data in the next page after each
    page. The last result date range should be stored so that it can be used
    as the start of the date range the next time the repository is queried.
    """
    client = MagicMock()
    repository_service_client.return_value = make_future(client)
    endpoint = client.repository.repositories.__getitem__().assets.identifiers
    endpoint.get.side_effect = mock_identifiers({
        1: {'data': ['first page'],
            'metadata': {'result_range': ('2000-01-01', '2001-01-01')}},
        2: {'data': ['second page'],
            'metadata': {'result_range': ('2001-01-01', '2002-01-01')}},
        3: {'data': ['third page'],
            'metadata': {'result_range': ('2002-01-01', '2003-01-01')}}
    })
    scheduler, repostore, manager = mock_manager()

    yield manager.fetch_identifiers('repo_a')

    from_time = manager.DEFAULT_FROM_TIME.isoformat()
    endpoint.get.assert_has_calls([
        call(**{'page': 1, 'from': from_time}),
        call(**{'page': 2, 'from': from_time}),
        call(**{'page': 3, 'from': from_time}),
        call(**{'page': 4, 'from': from_time}),
    ])
    manager.db.add_entities.assert_has_calls([
        call('asset', ['first page'], 'repo_a'),
        call('asset', ['second page'], 'repo_a'),
        call('asset', ['third page'], 'repo_a'),
    ])
    assert repostore._shelf['repo_a']['next'] == datetime(2003, 1, 1)


@patch('index.repositories.repository_service_client')
@patch('index.repositories.koi')
@gen_test
def test_fetch_max_pages(koi, repository_service_client):
    """Test that we don't fetch more that the max_repository_pages option"""
    define('max_repository_pages', default=5)

    client = MagicMock()
    repository_service_client.return_value = make_future(client)
    endpoint = client.repository.repositories.__getitem__().assets.identifiers
    endpoint.get.side_effect = mock_identifiers({
        1: {'data': ['first page'],
            'metadata': {'result_range': ('2000-01-01', '2001-01-01')}},
        2: {'data': ['second page'],
            'metadata': {'result_range': ('2001-01-01', '2002-01-01')}},
        3: {'data': ['third page'],
            'metadata': {'result_range': ('2002-01-01', '2003-01-01')}},
        4: {'data': ['fourth page'],
            'metadata': {'result_range': ('2003-01-01', '2004-01-01')}},
        5: {'data': ['fifth page'],
            'metadata': {'result_range': ('2004-01-01', '2005-01-01')}},
        6: {'data': ['sixth page'],
            'metadata': {'result_range': ('2005-01-01', '2006-01-01')}},
    })
    scheduler, repostore, manager = mock_manager()

    yield manager.fetch_identifiers('repo_a')

    from_time = manager.DEFAULT_FROM_TIME.isoformat()
    endpoint.get.assert_has_calls([
        call(**{'page': 1, 'from': from_time}),
        call(**{'page': 2, 'from': from_time}),
        call(**{'page': 3, 'from': from_time}),
        call(**{'page': 4, 'from': from_time}),
        call(**{'page': 5, 'from': from_time}),
    ])
    manager.db.add_entities.assert_has_calls([
        call('asset', ['first page'], 'repo_a'),
        call('asset', ['second page'], 'repo_a'),
        call('asset', ['third page'], 'repo_a'),
        call('asset', ['fourth page'], 'repo_a'),
        call('asset', ['fifth page'], 'repo_a'),
    ])
    assert repostore._shelf['repo_a']['next'] == datetime(2005, 1, 1)



@patch('index.repositories.repository_service_client')
@patch('index.repositories.koi')
@gen_test
def test_fetch_api_call_with_saved_timestamp(koi, repository_service_client):
    """
    The 'from' query parameter should be taken from the shelf if it's populated
    """
    client = MagicMock()
    repository_service_client.return_value = make_future(client)
    endpoint = client.repository.repositories.__getitem__().assets.identifiers
    endpoint.get.side_effect = mock_identifiers()
    scheduler, repostore, manager = mock_manager()

    from_time = datetime(2016, 1, 1)
    repostore._shelf['repo_a']['next'] = from_time

    yield manager.fetch_identifiers('repo_a')

    endpoint.get.assert_called_once_with(
        **{'page': 1, 'from': from_time.isoformat()})


@gen_test
def test_next_poll_interval_after_successful():
    """
    If successful the next poll interval, should be a random number in the
    manager's interval range.
    """
    scheduler = MagicMock()
    scheduler.get.return_value = make_future('repo1')
    manager = repositories.Manager(MagicMock(), MagicMock(), scheduler)
    manager.fetch_identifiers = MagicMock(return_value=make_future({'errors': 0}))

    yield manager.fetch('repo1')

    (repo_id, next_poll_interval), _ = scheduler.schedule.call_args

    min_interval, max_interval = manager.poll_interval_range
    assert min_interval <= next_poll_interval <= max_interval
    assert repo_id == 'repo1'


@gen_test
def test_next_poll_interval_after_error():
    """
    If there was an error, the next poll interval, should be a random number
    in the manager's interval range multiplied by the number errors up to
    the max_error_delay_factor
    """
    scheduler = MagicMock()
    scheduler.get.return_value = make_future('repo1')
    manager = repositories.Manager(MagicMock(), MagicMock(), scheduler)
    errors = manager.max_error_delay_factor - 1
    manager.fetch_identifiers = MagicMock(return_value=make_future(
        {'errors': errors}
    ))

    yield manager.fetch('repo1')

    (repo_id, next_poll_interval), _ = scheduler.schedule.call_args

    min_interval, max_interval = manager.poll_interval_range
    assert min_interval * errors <= next_poll_interval
    assert max_interval * errors >= next_poll_interval


@gen_test
def test_next_poll_interval_max_error_delay():
    """
    If there was an error, the next poll interval, should be a random number
    in the manager's interval range multiplied by the number errors up to
    the max_error_delay_factor
    """
    scheduler = MagicMock()
    scheduler.get.return_value = make_future('repo1')
    manager = repositories.Manager(MagicMock(), MagicMock(), scheduler)
    manager.fetch_identifiers = MagicMock(return_value=make_future(
        {'errors': manager.max_error_delay_factor + 1}
    ))

    yield manager.fetch('repo1')

    (repo_id, next_poll_interval), _ = scheduler.schedule.call_args

    min_interval, max_interval = manager.poll_interval_range
    assert min_interval * manager.max_error_delay_factor <= next_poll_interval
    assert max_interval * manager.max_error_delay_factor >= next_poll_interval


@patch('index.repositories.options')
@gen_test
def test_invalid_max_error_delay_factor(options):
    options.default_poll_interval = 1
    for i in [-1, 0]:
        options.max_poll_error_delay_factor = i

        with pytest.raises(ValueError):
            repositories.Manager(MagicMock(), MagicMock(), MagicMock())
