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
import base_test_class
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import DAY

from tests.base_test_class import ActiveDataBaseTest

FROM_DATE = Date.today()-7*DAY
TO_DATE = Date.today()

simple_test_data =[
    {"_run":{"timestamp": Date("now-4day"), "value": 1}},
    {"_run":{"timestamp": Date("now-4day"), "value": 2}},
    {"_run":{"timestamp": Date("now-4day"), "value": 3}},
    {"_run":{"timestamp": Date("now-4day"), "value": 4}},
    {"_run":{"timestamp": Date("now-3day"), "value": 5}},
    {"_run":{"timestamp": Date("now-3day"), "value": 6}},
    {"_run":{"timestamp": Date("now-3day"), "value": 7}},
    {"_run":{"timestamp": Date("now-2day"), "value": 8}},
    {"_run":{"timestamp": Date("now-2day"), "value": 9}},
    {"_run":{"timestamp": Date("now-1day"), "value": 0}},
    {"_run":{"timestamp": Date("now-5day"), "value": 1}},
    {"_run":{"timestamp": Date("now-5day"), "value": 2}},
    {"_run":{"timestamp": Date("now-5day"), "value": 3}},
    {"_run":{"timestamp": Date("now-5day"), "value": 4}},
    {"_run":{"timestamp": Date("now-5day"), "value": 5}},
    {"_run":{"timestamp": Date("now-6day"), "value": 6}},
    {"_run":{"timestamp": Date("now-6day"), "value": 7}},
    {"_run":{"timestamp": Date("now-6day"), "value": 8}},
    {"_run":{"timestamp": Date("now-6day"), "value": 9}},
    {"_run":{"timestamp": Date("now-6day"), "value": 0}},
    {"_run":{"timestamp": Date("now-6day"), "value": 1}},
    {"_run":{"timestamp": Date("now-0day"), "value": 2}},
    {"_run":{"timestamp": Date("now-0day"), "value": 3}},
    {"_run":{"timestamp": Date("now-0day"), "value": 4}},
    {"_run":{"timestamp": Date("now-0day"), "value": 5}}
]


class TestEdge1(ActiveDataBaseTest):

    def test_count_over_time(self):
        test = {
            "data": simple_test_data,
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "edges": [
                    {
                        "value": "_run.timestamp",
                        "allowNulls": False,
                        "domain": {
                            "type": "time",
                            "min": "today-week",
                            "max": "today",
                            "interval": "day"
                        }
                    }
                ]
            },
            # "expecting_list": {
            #     "meta": {"format": "list"},
            #     "data": [
            #         {"a": "b", "count": 2},
            #         {"a": "c", "count": 3},
            #         {"a": None, "count": 1}
            #     ]},
            # "expecting_table": {
            #     "meta": {"format": "table"},
            #     "header": ["a", "count"],
            #     "data": [
            #         ["b", 2],
            #         ["c", 3],
            #         [None, 1]
            #     ]
            # },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "_run.timestamp",
                        "domain": {
                            "type": "time",
                            "key": "min",
                            "partitions": [{"dataIndex": i, "min": m.unix, "max": (m + DAY).unix} for i, m in enumerate(Date.range(FROM_DATE, TO_DATE, DAY))],
                            "min": FROM_DATE.unix,
                            "max": TO_DATE.unix,
                            "interval": DAY.seconds
                        }
                    }
                ],
                "data": {
                    "count": [0, 6, 5, 4, 3, 2, 1]
                }
            }
        }
        self._execute_es_tests(test)


