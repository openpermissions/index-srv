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


"""API Root handler. Return basic information about the service.
"""

from koi.base import BaseHandler
from tornado.options import options


class RootHandler(BaseHandler):
    """Basic information about the service, like:
    - its name
    - current minor version
    """

    METHOD_ACCESS = {
        'GET': BaseHandler.UNAUTHENTICATED_ACCESS
    }

    def initialize(self, **kwargs):
        try:
            self.version = kwargs['version']
        except KeyError:
            raise KeyError('version is required')

    def get(self):
        """ Query basic information about the service.

        Respond with JSON containing service name and current minor version
        of the service.
        """
        msg = {
            'status': 200,
            'data': {
                'service_name': 'Open Permissions Platform Index Service',
                'service_id': options.service_id,
                'version': '{}'.format(self.version)
            }
        }

        self.finish(msg)
