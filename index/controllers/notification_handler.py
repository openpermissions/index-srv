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

import logging
import Queue

from tornado.gen import coroutine
from koi.base import BaseHandler


class NotificationHandler(BaseHandler):
    """Handle notifications of new data from repositories"""

    def initialize(self, notification_q, **kwargs):
        self.notification_q = notification_q

    @coroutine
    def post(self):
        repo_id = self.get_json_body(required=('id', ))['id']
        # NOTE: we are assuming that we can trust the location sent in the
        # request. The request has been authenticated so this seems
        # reasonable for now.
        try:
            self.notification_q.put_nowait(repo_id)
        except Queue.Full:
            logging.warning('Notification from repository service {} dropped '
                            'because the queue is full'.format(repo_id))
            pass

        self.finish({'status': 200})
