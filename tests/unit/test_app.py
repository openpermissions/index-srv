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

"""Unit tests for the main application code"""
from mock import patch

import index.app


@patch('index.app.options')
@patch('tornado.ioloop.IOLoop.instance')
@patch('index.app.koi.make_server')
@patch('index.app.koi.load_config')
def test_main_configure_and_run_service(load_config, make_server,
                                        instance, options):
    server = make_server.return_value
    options.processes = 1
    # MUT
    index.app.main()

    assert load_config.called
    assert make_server.called
    server.start.assert_called_once_with(1)
    assert instance.called
