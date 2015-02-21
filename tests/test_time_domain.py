# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from __future__ import division
from pyLibrary.dot import wrap
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import Duration

from tests.base_test_class import ActiveDataBaseTest

test_data = [
    {"a": "x", "t": Date("today").unix, "v": 2},
    {"a": "x", "t": Date("today-day").unix, "v": 2},
    {"a": "x", "t": Date("today-2day").unix, "v": 3},
    {"a": "x", "t": Date("today-3day").unix, "v": 5},
    {"a": "x", "t": Date("today-4day").unix, "v": 7},
    {"a": "x", "t": Date("today-5day").unix, "v": 11},
    {"a": "y", "t": None, "v": 27},
    {"a": "y", "t": Date("today-day").unix, "v": 13},
    {"a": "y", "t": Date("today-2day").unix, "v": 17},
    {"a": "y", "t": Date("today-4day").unix, "v": 19},
    {"a": "y", "t": Date("today-5day").unix, "v": 23}

]

expected = wrap([
    {"t": (Date.today() - Duration.WEEK).unix, "v": None},
    {"t": (Date.today() - 6 * Duration.DAY).unix, "v": None},
    {"t": (Date.today() - 5 * Duration.DAY).unix, "v": 34},
    {"t": (Date.today() - 4 * Duration.DAY).unix, "v": 26},
    {"t": (Date.today() - 3 * Duration.DAY).unix, "v": 5},
    {"t": (Date.today() - 2 * Duration.DAY).unix, "v": 20},
    {"t": (Date.today() - 1 * Duration.DAY).unix, "v": 15},
    {"v": 29}
])


class TestTime1(ActiveDataBaseTest):
    def test_time_variables(self):
        test = {
            "metadata": {},
            "data": test_data,
            "query": {
                "from": "unittest",
                "edges": [
                    {
                        "value": "t",
                        "domain": {
                            "type": "time",
                            "min": "today-week",
                            "max": "today",
                            "interval": "day"
                        }
                    }
                ],
                "select": {
                    "value": "v", "aggregate": "sum"
                }
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": expected
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["t", "v"],
                "data": [[r.t, r.v] for r in expected]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [{
                    "name": "t",
                    "domain": {
                        "type": "time",
                        "key": "min",
                        "min": Date("today-week").unix,
                        "max": Date.today().unix,
                        "interval": Duration.DAY.seconds,
                        "partitions": [{"min": r.t, "max": (Date(r.t) + Duration.DAY).unix} for r in expected if r.t != None]
                    }
                }],
                "data": {"v": [r.v for r in expected]}
            }
        }
        self._execute_es_tests(test)



        #
        #
        #
        # test = {
        # "metadata": {},
        # "data": test_data,
        # "query": {
        #     "from": "unittest",
        #     "edges": [
        #         "build.branch",
        #         "result.ok",
        #         {
        #             "value": "run.start",
        #             "domain": {
        #                 "type": "time",
        #                 "min": "today-2month",
        #                 "max": "today",
        #                 "interval": "day"
        #             }
        #         }
        #     ],
        #     "select": {
        #         "aggregate": "count"
        #     },
        #     "where": {
        #         "and": [
        #             {
        #                 "term": {
        #                     "build.branch": "mozilla-central"
        #                 }
        #             }
        #         ]
        #     }
        # }
