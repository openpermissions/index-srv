# -*- coding: utf-8 -*-
# Copyright 2016 Open Permissions Platform Coalition
# 
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
# 
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA


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
