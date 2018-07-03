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

"""API Repositories handler.
Return information about the repositories that an entity can be found in
"""
from tornado.gen import coroutine
from tornado.options import options, define

from koi.base import BaseHandler
from koi import exceptions

from urllib import quote_plus

import logging

define('max_related_depth', default=5,
       help='Maximum recursion on ids allowed for related ids queries')


class RepositoriesHandler(BaseHandler):  # pragma: no cover
    """Querying for repositories

    This endpoint allows to know in which repository an asset may be found
    """

    def initialize(self, database):
        self.database = database

    @coroutine
    def get(self, entity_type, source_id_type, source_id):
        """
        Obtain the list of repositories knowing information about an asset
        based on the id

        :param entity_type: the type of the entity to get data for
        :param source_id_type: the type of the id
        :param source_id: the id of the entity
        :return: JSON object containing requested information on an
        entity depending on the query_type
        """

        try:
            related_depth = int(self.get_argument("related_depth", default="0"))
        except ValueError:
            related_depth = 0
        related_depth = max(0, min(options.max_related_depth, related_depth))

        try:
            results = yield self.database.query(
                [{'source_id_type': quote_plus(source_id_type), 
                    'source_id': quote_plus(source_id)}],
                related_depth)
        except exceptions.HTTPError:
            # Raise a 404 because the URL contains an invaild ID that does
            # not exist
            raise exceptions.HTTPError(404, 'Not found (%r,%r)'%(source_id_type, source_id))

        data = results[0]

        if not data.get('repositories') and not data.get('relations'):
            raise exceptions.HTTPError(404, 'Not found')

        result = {
            'status': 200,
            'data': data
        }

        self.finish(result)

class RepositoryHandler(BaseHandler):  # pragma: no cover
    """Delete support for UPSERTs

    This endpoint supports deleting from the index as part of the UPSERT support in onboarding
    """
    METHOD_ACCESS = {
        'DELETE': BaseHandler.UNAUTHENTICATED_ACCESS
    }

    def initialize(self, database):
        self.database = database

    @coroutine
    def delete(self, entity_type, source_id_type, source_id, repository_id):
        """
        Obtain the list of entities matching source_id/type in a repository
        and delete them

        :param entity_type: the type of the entity to get data for
        :param source_id_type: the type of the id (this can be a list)
        :param source_id: the id of the entity (this can be a list)
        :param repository_id: the id of the repository to delete the entity from
        :return: status 204 No Content on success or error code
        """
        logging.debug('hello')

        id_types = source_id_type.split(',')
        ids = source_id.split(',')

        if len(id_types) != len(ids):
            raise exceptions.HTTPError(404, 'Inconsistent source_id/source_type_id lengths (%r,%r)'%(source_id_type, source_id, repository_id))

        all_ids = []

        for id_type, id in zip(id_types, ids):
            item = {'source_id_type': quote_plus(id_type), 
                    'source_id': quote_plus(id)}
            all_ids.append(item)

        try:
            yield self.database.delete(
                entity_type,
                all_ids,
                repository_id)
        except exceptions.HTTPError:
            # Raise a 404 because the URL contains an invaild ID that does
            # not exist
            raise exceptions.HTTPError(404, 'Not found (%r,%r,%r)'%(source_id_type, source_id, repository_id))

        result = {
            'status': 204
        }

        self.finish(result)


class BulkRepositoriesHandler(BaseHandler):  # pragma: no cover
    """Querying for repositories for multiple assets

    This endpoint allows to know in which repository an asset may be found
    """

    METHOD_ACCESS = {
        "POST": BaseHandler.READ_WRITE_ACCESS
    }

    def initialize(self, database):
        self.database = database

    @coroutine
    def post(self, entity_type):
        """
        Obtain the list of repositories for requested assets

        The request body should be an array of objects with source_id & source_id_type, e.g.:
            [{"source_id": 1, "source_id_type": "a_registered_id_type"},
            {"source_id": "https://chub.org/s0/hub1/asset/testco/my_id_type/my_id",
             "source_id_type": "hub_key"}]

        :param entity_type: the type of the entity to get data for
        :return: JSON array containing requested information on an
        entity depending on the entity_type
        """
        ids = self.get_json_body()
        try:
            related_depth = int(self.get_argument("related_depth", default="0"))
        except ValueError:
            related_depth = 0
        related_depth = max(0, min(options.max_related_depth, related_depth))

        # NOTE: currently the query ignores entity type. There is only one
        # so it should be OK...
        repositories = yield self.database.query(ids, related_depth)
        result = {
            'status': 200,
            'data': repositories
        }

        self.finish(result)

class RepositoryIndexedHandler(BaseHandler):
    """Return timestamp of last indexed time for repository"""
    def initialize(self, repositories, **kwargs):
        self.repositories = repositories

    @coroutine
    def get(self, repository_id):
        repository = self.repositories.get(repository_id)
        if not repository:
            raise exceptions.HTTPError(404, 'Not found')

        last_indexed = repository.get('last')
        if last_indexed:
            last_indexed = last_indexed.isoformat()

        self.finish({
            'status': 200,
            'last_indexed': last_indexed
        })
