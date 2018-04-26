# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals


class Table(object):

    def __init__(self, full_name, container):
        self.name = full_name
        self.container = container
        self.schema = container.namespace.get_schema(full_name)

    def map(self, mapping):
        return self

