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

import os

import pytest
from mock import patch, Mock
from koi.test_helpers import make_future

from index.commands.load_data import cli, do_load_data

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')


@patch('index.models.db.DbInterface')
def test_load_data_valid_ttl(db_interface):
    db = db_interface.return_value
    db.store = Mock(return_value=make_future(None))
    do_load_data(db, os.path.abspath(
        os.path.join(FIXTURE_DIR, '../../fixtures/test.ttl')))

    assert db_interface().store.call_count == 1


@patch('index.models.db.DbInterface')
def test_load_data_valid_xml(db_interface):
    db = db_interface.return_value
    db.store = Mock(return_value=make_future(None))
    do_load_data(db, os.path.abspath(os.path.join(FIXTURE_DIR, 'valid.xml')))

    assert db_interface().store.call_count == 1


@patch('index.models.db.DbInterface')
def test_load_data_invalid_file_type(db_interface):
    db = db_interface('http://domain', '8080', '/path/', 'schema')
    do_load_data(db, os.path.abspath(os.path.join(FIXTURE_DIR, 'invalid.txt')))

    assert not db_interface.store.called


@patch('index.commands.load_data.options')
@patch('index.commands.load_data.do_load_data')
@patch('index.commands.load_data.DbInterface')
def test_cli(db_interface, load_data, options):
    db_interface.return_value.create_namespace = Mock(return_value=make_future(lambda: None))

    with pytest.raises(SystemExit) as exc:
        cli([os.path.abspath(os.path.join(FIXTURE_DIR, 'valid.xml')),
             os.path.abspath(os.path.join(FIXTURE_DIR, 'valid.xml')),
             os.path.abspath(os.path.join(FIXTURE_DIR, 'valid.xml')),
             os.path.abspath(os.path.join(FIXTURE_DIR, 'valid.xml'))
             ])

    assert db_interface.called
    assert db_interface().create_namespace.call_count == 1
    assert load_data.call_count == 4
    assert exc.value.code == 0
