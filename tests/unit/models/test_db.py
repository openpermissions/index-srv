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

from __future__ import unicode_literals
import re
import json
from functools import partial
import pytest
from koi import exceptions
from koi.test_helpers import make_future
from mock import patch, Mock
from tornado import ioloop, gen
from index.models.db import DbInterface, HTTPError


VALID_ENTITY_ID1 = '37cd1397e0814e989fa22da6b15fec60'
VALID_ENTITY_ID2 = '37cd1397e0814e989fa22da6b15fec61'
INVALID_ENTITY_ID = 'ZK#/123Z'

VALID_HUBKEY1 = "https://opp.org/s1/hub1/37cd1397e0814e989fa22da6b15fec50/asset/37cd1397e0814e989fa22da6b15fec60"
VALID_HUBKEY2 = "https://opp.org/s1/hub1/37cd1397e0814e989fa22da6b15fec51/asset/37cd1397e0814e989fa22da6b15fec61"
INVALID_HUBKEY = "https://opp.org/s1/hub1/invalidrepo/asset/invalidentityid"


def test_set_database_url_port():
    db_interface = DbInterface('url', '8080', '/path/', 'schema')
    assert "8080" in db_interface.db_url


def test_set_database_url_url():
    db_interface = DbInterface('url', '8080', '/path/', 'schema')
    assert "url" in db_interface.db_url


def test_set_database_url_path():
    db_interface = DbInterface('url', '8080', '/path/', 'schema')
    assert "/path/" in db_interface.db_url


def test_set_database_url_schema():
    db_interface = DbInterface('url', '8080', '/path/', 'schema')
    assert "schema" in db_interface.db_url


@patch('index.models.db.logging.error')
@patch('index.models.db.AsyncHTTPClient')
def test_create_namespace(async, error):
    namespace = """\
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">
<properties>
  <entry key="com.bigdata.rdf.sail.namespace">schema</entry>
</properties>"""
    db_interface = DbInterface('url', '8080', '/path/', 'schema')
    async().fetch.return_value = make_future(None)
    db_interface.create_namespace()
    async().fetch.assert_called_once_with(
        db_interface.db_namespace_url,
        method='POST',
        body=namespace,
        headers={'Content-Type': 'application/xml'})
    assert not error.called


@patch('index.models.db.logging.error')
@patch('index.models.db.AsyncHTTPClient')
def test_create_namespace_generic_error(async, error):
    db_interface = DbInterface('url', '8080', '/path/', 'schema')

    async().fetch.side_effect = Exception
    db_interface.create_namespace()

    assert error.called


@patch('index.models.db.logging.error')
@patch('index.models.db.AsyncHTTPClient')
def test_create_namespace_already_exists(async, error):
    db_interface = DbInterface('url', '8080', '/path/', 'schema')
    async().fetch.side_effect = HTTPError(409)
    db_interface.create_namespace()
    assert not error.called


@patch('index.models.db.logging.error')
@patch('index.models.db.AsyncHTTPClient')
def test_create_namespace_http_error(async, error):
    db_interface = DbInterface('url', '8080', '/path/', 'schema')
    async().fetch.side_effect = HTTPError(401)
    db_interface.create_namespace()
    assert error.called


DATA0 = [
    {"source_id_type": "my_id_type",
     "source_id": "my_id",
     "entity_id": VALID_ENTITY_ID1
     },
    {"source_id_type": "foo_id_type",
     "source_id": "flibble",
     "entity_id": VALID_ENTITY_ID2
     }
]

DATA0BAD = [
    {"source_id_type": "my_id_type",
     "source_id": "my_id",
     "entity_id": INVALID_ENTITY_ID
     },
    {"source_id_type": "foo_id_type",
     "source_id": "flibble",
     "entity_id": VALID_ENTITY_ID1
     }
]


@patch('index.models.db.DbInterface.store')
def test_add_entities(store):
    store.return_value = make_future([])

    db_interface = DbInterface('url', '8080', '/path/', 'schema')

    @gen.coroutine
    def add_entities():
        res = yield db_interface.add_entities('asset', DATA0, "repo2")
        raise gen.Return(res)

    errors = ioloop.IOLoop().run_sync(add_entities)

    assert (len(store.call_args[0][0].strip()) > 0)
    assert errors == {'errors': [], 'records': 2}


@patch('index.models.db.DbInterface.store')
def test_add_partial_invalid_entities(store):
    store.return_value = make_future([])

    db_interface = DbInterface('url', '8080', '/path/', 'schema')

    @gen.coroutine
    def add_entities():
        res = yield db_interface.add_entities('asset', DATA0BAD, "repo2")
        raise gen.Return(res)

    errors = ioloop.IOLoop().run_sync(add_entities)

    assert (len(store.call_args[0][0].strip()) > 0)
    assert errors['records'] == 2
    assert len(errors['errors']) == 1


@patch('index.models.db.DbInterface.store')
def test_add_entities_empty_data(store):
    store.return_value = make_future([])

    db_interface = DbInterface('url', '8080', '/path/', 'schema')

    @gen.coroutine
    def add_entities():
        res = yield db_interface.add_entities('asset', [], "repo2")
        raise gen.Return(res)

    errors = ioloop.IOLoop().run_sync(add_entities)

    assert (len(store.call_args[0][0].strip()) > 0)
    assert errors['errors'] == []


def test_bulk_repositories():
    """Test that the internal method is called correctly"""
    ids = [
        {'source_id': VALID_HUBKEY1, 'source_id_type': 'hub_key'},
        {'source_id': 'b', 'source_id_type': 'something'},
        {'source_id': VALID_HUBKEY2, 'source_id_type': 'hub_key'},
        {'source_id': 'd', 'source_id_type': 'other'},
    ]
    db = DbInterface('url', '8080', '/path/', 'schema')
    # mock methods that should be called
    db._query_ids = Mock()
    db._query_ids.return_value = make_future([1, 2])

    func = partial(db.query, ids)
    result = ioloop.IOLoop().run_sync(func)

    assert result == [1, 2]
    db._query_ids.assert_called_once_with(ids, 0)


def test_bulk_repositories_with_errors():
    """Test errors are collected"""
    ids = [
        {'source_id': VALID_HUBKEY1, 'source_id_type': 'hub_key'},
        {'source_id': 'b'},
        {'source_id': INVALID_HUBKEY, 'source_id_type': 'hub_key'},
        {'source_id_type': 'other'},
    ]
    db = DbInterface('url', '8080', '/path/', 'schema')
    # mock methods that should be called
    db._query_ids = Mock()

    func = partial(db.query, ids)
    with pytest.raises(exceptions.HTTPError) as exc:
        ioloop.IOLoop().run_sync(func)

    assert exc.value.status_code == 400
    assert exc.value.errors == [
        {'source_id': 'b'},
        {'source_id': INVALID_HUBKEY, 'source_id_type': 'hub_key'},
        {'source_id_type': 'other'},
    ]
    assert not db._query_ids.called


def test_get_repositories_for_ids():
    """
    Test the query result is formatted correctly

    Not checking the db query string because the query will be covered by our
    system tests
    """
    ids = [
        {'source_id': 'asset1',
         'source_id_type': 'type1',
         },
        {'source_id': 'b',
         'source_id_type': 'something',
         }
    ]
    db = DbInterface('url', '8080', '/path/', 'schema')
    # mock methods that should be called
    db._run_query = Mock()
    db._run_query.return_value = make_future([
        {'source_id': 'asset1', 'source_id_type': 'type1',
         'repositories': json.dumps([{'repository_id': 'repo1'}]),
         'relations': json.dumps([])
         },
        {'source_id': 'b', 'source_id_type': 'something',
         'repositories': json.dumps([{'repository_id': 'repo2'}]),
         'relations': json.dumps([])
         }
    ])

    func = partial(db._query_ids, ids)
    res = ioloop.IOLoop().run_sync(func)

    expected_results = [
        {
            'source_id': 'asset1',
            'source_id_type': 'type1',
            'repositories': [{'repository_id': 'repo1'}],
            'relations': []
        },
        {
            'source_id': 'b',
            'source_id_type': 'something',
            'repositories': [{'repository_id': 'repo2'}],
            'relations': []
        }
    ]

    assert len(res) == 2
    assert res[0] in expected_results
    assert res[1] in expected_results


def test_get_repositories_for_ids_with_keys_not_found():
    """
    Test the query result is formatted correctly and covers the case where some elements
    of the query are not found.

    Not checking the db query string because the query will be covered by our
    system tests
    """
    ids = [
        {'source_id': 'asset1',
         'source_id_type': 'type1',
         },
        {'source_id': 'b',
         'source_id_type': 'something',
         }
    ]
    db = DbInterface('url', '8080', '/path/', 'schema')

    # mock methods that should be called
    db._run_query = Mock()
    db._run_query.return_value = make_future([{
        'source_id': 'asset1',
        'source_id_type': 'type1',
        'repositories': json.dumps([{'id': 'repo1'}]),
        'relations': json.dumps([])
    }])

    func = partial(db._query_ids, ids)
    res = ioloop.IOLoop().run_sync(func)

    assert res == [
        {'source_id': 'asset1',
         'source_id_type': 'type1',
         'repositories': [{'id': 'repo1'}],
         'relations': []
         },
        {'source_id': 'b',
         'source_id_type': 'something',
         'repositories': [],
         'relations': []}
    ]


def test_get_repositories_with_utf8_ids():
    """
    Test the query result is formatted correctly and covers the case where
    some elements of the query are not found.

    Not checking the db query string because the query will be covered by our
    system tests
    """
    id_value = 'b%E2%82%AC'
    id_type = u'som%E2%82%ACthing'
    ids = [
        {'source_id': id_value, 'source_id_type': id_type}
    ]
    db = DbInterface('url', '8080', '/path/', 'schema')
    # mock methods that should be called
    db._run_query = Mock()
    db._run_query.return_value = make_future([{
        'source_id': 'b%E2%82%AC',
        'source_id_type': 'som%E2%82%ACthing',
        'repositories': json.dumps([]),
        'relations': json.dumps([])
    }])

    func = partial(db._query_ids, ids)
    res = ioloop.IOLoop().run_sync(func)

    assert res == [{
        'source_id': id_value,
        'source_id_type': id_type,
        'repositories': [],
        'relations': []
    }]
