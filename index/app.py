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

"""Configures and starts up the Index Service."""
from multiprocessing import Process, Queue
import os.path
import tornado.ioloop
import tornado.httpserver
from tornado.options import options
import koi
from . import __version__, repositories
from .controllers import (root_handler, repositories_handler,
                          notification_handler)
from .models.db import DbInterface

# directory containing the config files
CONF_DIR = os.path.join(os.path.dirname(__file__), '../config')

APPLICATION_URLS = [
    (r"", root_handler.RootHandler, {'version': __version__}),
    (r"/entity-types/{entity_type}/id-types/{source_id_type}/ids/{source_id}/repositories",
     repositories_handler.RepositoriesHandler),
    (r"/entity-types/{entity_type}/repositories",
     repositories_handler.BulkRepositoriesHandler),
]


def start_background_process(db):
    """Start background process to get data from repositories"""
    queue = Queue(maxsize=options.notifications_queue_max_size)
    notification = repositories.Notification(queue)
    Process(target=repositories.main, args=(db, notification)).start()

    APPLICATION_URLS.append((
        r"/notifications",
        notification_handler.NotificationHandler,
        {'notification_q': queue}
    ))


def main():
    """
    The entry point for the Index service.
    This will load the configuration files and start a Tornado webservice
    with one or more sub processes.

    NOTES:
    tornado.options.parse_command_line(final=True)
    Allows you to run the service with custom options.

    Examples:
        Change the logging level to debug:
            + python index --logging=DEBUG
            + python index --logging=debug

        Configure custom syslog server:
            + python index --syslog_host=54.77.151.169
    """
    koi.load_config(CONF_DIR)

    db = DbInterface(
        options.url_index_db,
        options.index_db_port,
        options.index_db_path,
        options.index_schema)

    if options.poll_repositories:
        start_background_process(db)

    app = koi.make_application(
        __version__,
        options.service_type,
        APPLICATION_URLS,
        {'database': db})

    server = koi.make_server(app, CONF_DIR)

    # Forks multiple sub-processes, one for each core
    server.start(int(options.processes))

    tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':  # pragma: no cover
    main()  # pragma: no cover
