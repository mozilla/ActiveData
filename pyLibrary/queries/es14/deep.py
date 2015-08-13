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

from pyLibrary.dot import split_field, join_field
from pyLibrary.queries.qb import concat


def es_deepop(es, frum, query):
    # Generate esFilter
    # Generate Ruby filter on subsequence
        # split filter variables by depth?
    # Iterate through subsequence
    # Add to list of results
    #
    if len(split_field(frum.name)) > 1:

    real_path = []
    for p, n in concat(split_field(frum.name)):
        candidate = join_field(p)
        if len(real_path)==es.schema[candidate].depth:
            real_path += [candidate]




    header = "output = []\n"

    loop_headers = ["for {{var}} in {{expr}} do\n"]




    loop_footers = ["end"]

