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

import Queue
from koi.test_helpers import gen_test

from index import repositories


@gen_test
def test_scheduler_receives_notification():
    queue = Queue.Queue()
    notification_q = repositories.Notification(queue)
    scheduler = repositories.Scheduler()
    notification_q.connect_with(scheduler)

    scheduler._time = lambda: 0
    notification_q.put_nowait('repo0')
    yield notification_q._check_for_notifications()

    scheduler._time = lambda: repositories.options.notify_min_delay
    result = yield scheduler.get()

    assert result == ['repo0']


@gen_test
def test_scheduler_notification_respects_order():
    queue = Queue.Queue()
    notification_q = repositories.Notification(queue)
    scheduler = repositories.Scheduler()
    scheduler._time = lambda: 0
    notification_q.connect_with(scheduler)

    notification_q.put_nowait('repo0')
    notification_q.put_nowait('repo1')

    yield notification_q._check_for_notifications()

    scheduler._time = lambda: repositories.options.notify_min_delay
    result = yield scheduler.get()

    assert result == ['repo0']
    result = yield scheduler.get()
    assert result == ['repo1']


@gen_test
def test_scheduler_notification_in_same_batch_get_merged():
    queue = Queue.Queue()
    notification_q = repositories.Notification(queue)
    scheduler = repositories.Scheduler()
    scheduler._time = lambda: 0
    notification_q.connect_with(scheduler)

    notification_q.put_nowait('repo0')
    notification_q.put_nowait('repo1')
    for i in range(10):
        notification_q.put_nowait('repo0')
    yield notification_q._check_for_notifications()

    scheduler._time = lambda: repositories.options.notify_min_delay
    result = yield scheduler.get(3)

    assert set(result) == {'repo0', 'repo1'}
