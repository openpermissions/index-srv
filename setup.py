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

from setuptools import find_packages, setup
import index

setup(
    name='open permissions platform index service',
    version=index.__version__,
    description='Open Permissions Platform Coalition Index Service',
    author='CDE Catapult',
    author_email='support@openpermissions.org',
    url='https://github.com/openpermissions/index-srv',
    packages=find_packages(exclude=['test']),
    entry_points={
        'console_scripts':
            ['open-permissions-platform-index-svr = index.app:main']},
)
