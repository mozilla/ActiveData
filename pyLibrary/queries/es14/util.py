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

from pyLibrary.dot import wrap


def build_es_query(query):
    output = wrap({
        "query": {"match_all": {}},
        "from": 0,
        "size": 0,
        "sort": [],
        "facets": {
        }
    })

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

