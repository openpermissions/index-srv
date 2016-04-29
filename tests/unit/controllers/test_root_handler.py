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

import uuid

from mock import MagicMock

from index.controllers.root_handler import RootHandler, options

from index import __version__


def test_get_service_status():
    service_id = '{}'.format(uuid.uuid4()).replace('-', '')
    options.define('service_id', default=service_id)
    root = RootHandler(MagicMock(), MagicMock(), version=__version__)
    root.finish = MagicMock()

    # MUT
    root.get()
    msg = {
        'status': 200,
        'data': {
            'service_name': 'Open Permissions Platform Index Service',
            'service_id': service_id,
            'version': '{}'.format(__version__)
        }
    }

    root.finish.assert_called_once_with(msg)
