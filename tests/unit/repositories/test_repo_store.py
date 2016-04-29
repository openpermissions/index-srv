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

from datetime import datetime
import pytest
from mock import call, patch, Mock, MagicMock
from tornado.httpclient import HTTPError
from koi.test_helpers import make_future, gen_test
from tornado.options import define

from index import repositories


define('ssl_ca_cert', default='')


@patch('index.repositories.shelve')
@patch('index.repositories.API')
@patch('index.repositories.koi')
@gen_test
def test_get_repositories(koi, API, shelve):
    API().accounts.repositories.get.return_value = make_future({
        'data': [{'id': 'a', 'location': 'http://a.test'},
                 {'id': 'b', 'location': 'http://b.test'}]
    })

    schedule_fetch = Mock()
    schedule_fetch.return_value = make_future([])

    repostore = repositories.RepositoryStore()
    repostore.on_new_repo = schedule_fetch
    repostore._shelf = {}

    yield repostore._fetch_repositories()

    assert API().accounts.repositories.get.called
    schedule_fetch.assert_has_calls([call('a'), call('b')])
    assert repostore._shelf == {
        'a':  {'id': 'a', 'location': 'http://a.test'},
        'b':  {'id': 'b', 'location': 'http://b.test'}
    }


@patch('index.repositories.shelve')
@patch('index.repositories.API')
@patch('index.repositories.koi')
@gen_test
def test_get_repositories_are_registered_once(koi, API, shelve):
    API().accounts.repositories.get.return_value = make_future({
        'data': [{'id': 'a', 'location': 'http://a.test'}]
    })

    schedule_fetch = Mock()
    schedule_fetch.return_value = make_future([])

    repostore = repositories.RepositoryStore()
    repostore.on_new_repo = schedule_fetch
    repostore._shelf = {
        'a':  {'id': 'a', 'location': 'http://a.test'},
        'b':  {'id': 'b', 'location': 'http://b.test'}
    }

    repostore._fetch_repositories()

    assert not schedule_fetch.called
    assert repostore._shelf == {
        'a':  {'id': 'a', 'location': 'http://a.test'},
        'b':  {'id': 'b', 'location': 'http://b.test'}
    }


@patch('index.repositories.shelve')
@patch('index.repositories.logging')
@patch('index.repositories.API')
@patch('index.repositories.koi')
@gen_test
def test_get_repositories_swallow_exceptions(koi, API, logging, shelve):
    """Log but don't raise exceptions"""
    API().accounts.repositories.get.side_effect = Exception('Test')

    schedule_fetch = Mock()
    schedule_fetch.return_value = make_future([])

    repostore = repositories.RepositoryStore()
    repostore.on_new_repo = schedule_fetch

    yield repostore._fetch_repositories()

    assert logging.exception.call_count == 1
    assert not schedule_fetch.called


@patch('index.repositories.shelve')
@gen_test
def test_get_repository(shelve):
    shelf = {'repo1': {'id': 1}}
    shelve.open.return_value = shelf

    repo_store = repositories.RepositoryStore(api_client=Mock())
    result = yield repo_store.get_repository('repo1')

    assert result == shelf['repo1']


@patch('index.repositories.options')
@patch('index.repositories.shelve')
@gen_test
def test_get_unknown_repository_closed_service(shelve, options):
    """When closed service don't fetch unknown repository"""
    shelf = {}
    shelve.open.return_value = shelf
    options.open_service = False

    api_client = MagicMock()
    endpoint = api_client.accounts.repositories.__getitem__()
    endpoint().get.return_value = make_future({'data': {'id': 1}})
    endpoint.reset_mock()

    repo_store = repositories.RepositoryStore(api_client=api_client)
    with pytest.raises(KeyError):
        yield repo_store.get_repository('repo1')

    assert not endpoint.called
    assert not endpoint().get.called


@patch('index.repositories.shelve')
@gen_test
def test_get_unknown_repository_exists_in_accounts(shelve):
    """Query for a service that exists in the accounts service"""
    shelf = {}
    shelve.open.return_value = shelf

    repo_id = 'repo1'
    repo = {'id': repo_id}
    api_client = MagicMock()
    endpoint = api_client.accounts.repositories.__getitem__
    endpoint().get.return_value = make_future({'data': repo})
    endpoint.reset_mock()

    repo_store = repositories.RepositoryStore(api_client=api_client)
    result = yield repo_store.get_repository(repo_id)

    endpoint.assert_called_once_with(repo_id)
    assert endpoint().get.called
    assert result == repo


@patch('index.repositories.shelve')
@gen_test
def test_get_unknown_repository_does_not_exist_in_accounts(shelve):
    """Query for a service that doesn't exist in the accounts service"""
    shelf = {}
    shelve.open.return_value = shelf

    api_client = MagicMock()
    endpoint = api_client.accounts.repositories.__getitem__
    endpoint().get.side_effect = HTTPError(404, 'Unknown resource')

    repo_store = repositories.RepositoryStore(api_client=api_client)
    with pytest.raises(KeyError):
        yield repo_store.get_repository('repo1')

    assert endpoint.called
    assert endpoint().get.called


@patch('index.repositories.logging')
@patch('index.repositories.shelve')
@gen_test
def test_fail(shelve, logging):
    """Test recording failed fetch"""
    shelf = {'repo1': {}}
    shelve.open.return_value = shelf

    store = repositories.RepositoryStore(api_client=MagicMock())

    store.fail('repo1', 'An error')
    assert shelf['repo1'] == {'errors': 1}

    store.fail('repo1', 'An error')
    assert shelf['repo1'] == {'errors': 2}

    logging.warning.assert_has_calls([call('An error'), call('An error')])


@patch('index.repositories.logging')
@patch('index.repositories.shelve')
@gen_test
def test_fail_without_reason(shelve, logging):
    """Test recording failed fetch without specifying a reason"""
    shelf = {'repo1': {}}
    shelve.open.return_value = shelf

    store = repositories.RepositoryStore(api_client=MagicMock())

    store.fail('repo1')
    assert shelf['repo1'] == {'errors': 1}

    assert logging.warning.call_count == 1


@patch('index.repositories.shelve')
@gen_test
def test_fail_unknown_(shelve):
    """
    Test KeyError raised if unknown repository

    We don't want to keep a record of unknown repositories
    """
    shelf = {'repo1': {}}
    shelve.open.return_value = shelf

    store = repositories.RepositoryStore(api_client=MagicMock())

    with pytest.raises(KeyError):
        yield store.fail('repo0')

    assert shelf['repo1'] == {}


@patch('index.repositories.IOLoop')
@patch('index.repositories.shelve')
@gen_test
def test_success(shelve, IOLoop):
    """Test recording successful fetch"""
    shelf = {'repo1': {}}
    shelve.open.return_value = shelf

    store = repositories.RepositoryStore(api_client=MagicMock())

    now = datetime.now()
    store.success('repo1', now)
    assert shelf['repo1'] == {
        'next': now,
        'last': IOLoop.current().time(),
        'errors': 0,
        'successful_queries': 1
    }

    later = datetime.now()
    store.success('repo1', later)
    assert shelf['repo1'] == {
        'next': later,
        'last': IOLoop.current().time(),
        'errors': 0,
        'successful_queries': 2
    }


@patch('index.repositories.IOLoop')
@patch('index.repositories.shelve')
@gen_test
def test_success_resets_errors(shelve, IOLoop):
    """Test recording successful fetch resets errors"""
    shelf = {'repo1': {'errors': 10}}
    shelve.open.return_value = shelf

    store = repositories.RepositoryStore(api_client=MagicMock())

    now = datetime.now()
    store.success('repo1', now)
    assert shelf['repo1'] == {
        'next': now,
        'last': IOLoop.current().time(),
        'errors': 0,
        'successful_queries': 1
    }


@patch('index.repositories.IOLoop')
@patch('index.repositories.shelve')
@gen_test
def test_success_without_next(shelve, IOLoop):
    """Test recording successful fetch resets errors"""
    shelf = {'repo1': {}}
    shelve.open.return_value = shelf

    store = repositories.RepositoryStore(api_client=MagicMock())

    store.success('repo1')
    assert shelf['repo1'] == {
        'next': None,
        'last': IOLoop.current().time(),
        'errors': 0,
        'successful_queries': 1
    }


@patch('index.repositories.IOLoop')
@patch('index.repositories.shelve')
@gen_test
def test_success_replace_next_with_none(shelve, IOLoop):
    """Test recording successful fetch resets errors"""
    shelf = {'repo1': {'next': 'something'}}
    shelve.open.return_value = shelf

    store = repositories.RepositoryStore(api_client=MagicMock())

    store.success('repo1')
    assert shelf['repo1'] == {
        'next': None,
        'last': IOLoop.current().time(),
        'errors': 0,
        'successful_queries': 1
    }
