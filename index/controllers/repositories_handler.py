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
                [{'source_id_type': source_id_type, 'source_id': source_id}],
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
