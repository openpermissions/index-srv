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

from koi.test_helpers import gen_test

from index import repositories


@gen_test
def test_schedule_fetch_in_the_future():
    scheduler = repositories.Scheduler()
    scheduler._time = lambda: 1000
    scheduler._items = {'a': [2000, 'a']}
    scheduler._priority_q = [[2000, 'a']]

    yield scheduler.schedule('a', 100)

    assert scheduler._items['a'][0] == 1100


@gen_test
def test_scheduler_get_empty():
    scheduler = repositories.Scheduler()

    result = yield scheduler.get()

    assert result == []


@gen_test
def test_scheduler_schedule_new_repo():
    scheduler = repositories.Scheduler()
    scheduler._time = lambda: 0

    yield scheduler.schedule('repo0', 0)

    result = yield scheduler.get()
    assert result == ['repo0']


@gen_test
def test_scheduler_respects_order():
    scheduler = repositories.Scheduler()
    # time is 0 when scheduling and then goes up by one
    scheduler._time = iter([0, 0, 0, 1, 2, 3]).next
    scheduler._sleep = lambda x: None

    yield scheduler.schedule('repo0', 3)
    yield scheduler.schedule('repo1', 1)
    yield scheduler.schedule('repo2', 2)

    result = yield scheduler.get()
    assert result == ['repo1']
    result = yield scheduler.get()
    assert result == ['repo2']
    result = yield scheduler.get()
    assert result == ['repo0']


@gen_test
def test_scheduler_reschedule():
    scheduler = repositories.Scheduler()
    # time is 0 when scheduling and then goes up by one
    scheduler._time = iter([0, 0, 0, 1, 2, 3]).next
    scheduler._sleep = lambda x: None

    # schedule repo0 after repo1
    # then reschedule repo0 before repo1
    yield scheduler.schedule('repo0', 3)
    yield scheduler.schedule('repo1', 2)
    yield scheduler.schedule('repo0', 1)

    result = yield scheduler.get()
    assert result == ['repo0']
    result = yield scheduler.get()
    assert result == ['repo1']
    result = yield scheduler.get()
    assert result == []


@gen_test
def test_scheduler_waits():
    scheduler = repositories.Scheduler()
    scheduler._time = lambda: 0

    # schedule repo0 after repo1
    # then reschedule repo0 before repo1
    yield scheduler.schedule('repo0', 1.0)
    yield scheduler.schedule('repo1', 0.75)
    yield scheduler.schedule('repo0', 0.5)

    scheduler._time = lambda: 0
    result = yield scheduler.get()
    assert result == []

    scheduler._time = lambda: 0.6
    result = yield scheduler.get()
    assert result == ['repo0']

    scheduler._time = lambda: 0.65
    result = yield scheduler.get()
    assert result == []

    scheduler._time = lambda: 0.8
    result = yield scheduler.get()
    assert result == ['repo1']

    scheduler._time = lambda: 1.1
    result = yield scheduler.get()
    assert result == []

    scheduler._time = lambda: 20
    result = yield scheduler.get()
    assert result == []


@gen_test
def test_get_multiple_items():
    scheduler = repositories.Scheduler()
    scheduler._time = lambda: 0

    yield scheduler.schedule('repo0', 1)
    yield scheduler.schedule('repo1', 2)

    scheduler._time = lambda: 2
    result = yield scheduler.get(2)

    assert result == ['repo0', 'repo1']


@gen_test
def test_not_enough_items():
    scheduler = repositories.Scheduler()
    scheduler._time = lambda: 0

    yield scheduler.schedule('repo0', 1)

    scheduler._time = lambda: 2
    result = yield scheduler.get(5)

    assert result == ['repo0']


@gen_test
def test_get_limited_multiple_items():
    scheduler = repositories.Scheduler()
    scheduler._time = lambda: 0

    yield scheduler.schedule('repo0', 1)
    yield scheduler.schedule('repo1', 2)
    yield scheduler.schedule('repo2', 3)

    scheduler._time = lambda: 3
    result = yield scheduler.get(2)

    assert result == ['repo0', 'repo1']

    # check 3 would have been included next
    result = yield scheduler.get(2)
    assert result == ['repo2']


@gen_test
def test_get_rescheduled_multiple_items():
    scheduler = repositories.Scheduler()
    scheduler._time = lambda: 0

    yield scheduler.schedule('repo0', 10)
    yield scheduler.schedule('repo1', 2)
    yield scheduler.schedule('repo2', 3)
    yield scheduler.schedule('repo0', 2)
    yield scheduler.schedule('repo1', 1)

    scheduler._time = lambda: 5
    result = yield scheduler.get(5)

    assert result == ['repo1', 'repo0', 'repo2']

    # check nothing would have been included next
    result = yield scheduler.get(2)
    assert result == []


@gen_test
def test_get_rescheduled_in_the_future():
    scheduler = repositories.Scheduler()
    scheduler._time = lambda: 0

    yield scheduler.schedule('repo0', 0)
    yield scheduler.schedule('repo1', 1)
    yield scheduler.schedule('repo0', 2)

    scheduler._time = lambda: 1
    result = yield scheduler.get(5)

    assert result == ['repo1']
