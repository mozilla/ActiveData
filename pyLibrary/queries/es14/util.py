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

from pyLibrary.dot import wrap


def es_query_template():
    output = wrap({
        "query": {"match_all": {}},
        "from": 0,
        "size": 0,
        "sort": []
    })

    return output


def qb_sort_to_es_sort(sort):
    output = []
    for s in sort:
        if s.sort == 1:
            output.append(s.field)
        elif s.sort == -1:
            output.append({s.field: "desc"})
        else:
            pass
    return output


# FOR ELASTICSEARCH aggs
aggregates1_4 = {
    "none": "none",
    "one": "count",
    "sum": "sum",
    "add": "sum",
    "count": "value_count",
    "maximum": "max",
    "minimum": "min",
    "max": "max",
    "min": "min",
    "mean": "avg",
    "average": "avg",
    "avg": "avg",
    "N": "count",
    "X0": "count",
    "X1": "sum",
    "X2": "sum_of_squares",
    "std": "std_deviation",
    "stddev": "std_deviation",
    "var": "variance",
    "variance": "variance"
}

