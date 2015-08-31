# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import

from pyLibrary.queries.expressions import query_get_all_vars


def is_deepop(es, query):
    vars = query_get_all_vars(query)
    columns = es.get_columns()
    if any(c for c in columns if c.depth and c.name in vars):
        return True
    return False


def es_deepop(es, query):
    # Generate esFilter
    # Generate Ruby filter on subsequence
        # split filter variables by depth?
    # Iterate through subsequence
    # Add to list of results
    #
    pass
