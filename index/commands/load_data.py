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

"""
fixture loader
"""
from functools import partial
import logging

import click
from tornado.ioloop import IOLoop
from tornado.options import options

from index.models.db import DbInterface


def do_load_data(db, filename):
    """
    Load data to the index database
    :param db: The database to load to
    :param filename: The name of the file containg the data
    """
    with open(filename, 'r') as content_file:
        content = content_file.read()

    logging.info("XML/XSD validation for {0}".format(filename))

    if filename.endswith('.ttl'):
        IOLoop().run_sync(partial(db.store, content, 'text/turtle'))
    elif filename.endswith('.xml'):
        IOLoop().run_sync(partial(db.store, content, 'application/xml'))


@click.command(help='load fixture data')
@click.argument('files', nargs=-1, type=click.File('rb'))
def cli(files):
    """
    Command to load data to the database
    :param files: list of files with data to load
    """
    db = DbInterface(options.url_index_db,
                     options.index_db_port,
                     options.index_db_path,
                     options.index_schema)

    # Create namespace
    IOLoop().run_sync(db.create_namespace)

    for fixture in files:
        do_load_data(db, fixture.name)
