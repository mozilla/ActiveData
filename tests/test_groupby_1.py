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
from pyLibrary.dot import wrap

from tests.base_test_class import ActiveDataBaseTest


class TestgroupBy1(ActiveDataBaseTest):

    def test_no_select(self):
        test = {
            "data": simple_test_data,
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "groupby": "a"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b", "count": 2},
                    {"a": "c", "count": 3},
                    {"a": None, "count": 1}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "count"],
                "data": [
                    ["b", 2],
                    ["c", 3],
                    [None, 1]
                ]
            }
        }
        self._execute_es_tests(test)

    def test_count_rows(self):
        test = {
            "name": "count rows, 1d",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"aggregate": "count"},
                "groupby": ["a"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b", "count": 2},
                    {"a": "c", "count": 3},
                    {"a": None, "count": 1}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "count"],
                "data": [
                    ["b", 2],
                    ["c", 3],
                    [None, 1]
                ]
            }
        }
        self._execute_es_tests(test)

    def test_count_self(self):
        test = {
            "name": "count column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"name": "count_a", "value": "a", "aggregate": "count"},
                "groupby": ["a"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b", "count_a": 2},
                    {"a": "c", "count_a": 3},
                    {"count_a": 0}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "count_a"],
                "data": [
                    ["b", 2],
                    ["c", 3],
                    [None, 0]
                ]
            }
        }
        self._execute_es_tests(test)

    def test_count_other(self):
        test = {
            "name": "count column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"name": "count_v", "value": "v", "aggregate": "count"},
                "groupby": ["a"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b", "count_v": 1},
                    {"a": "c", "count_v": 3},
                    {"count_v": 1}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "count_v"],
                "data": [
                    ["b", 1],
                    ["c", 3],
                    [None, 1]
                ]
            }
        }
        self._execute_es_tests(test)

    def test_select_2(self):
        test = {
            "name": "count column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": [
                    {"name": "count", "value": "v", "aggregate": "count"},
                    {"name": "avg", "value": "v", "aggregate": "average"}
                ],
                "groupby": ["a"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b", "count": 1, "avg": 2},
                    {"a": "c", "count": 3, "avg": 31 / 3},
                    {"count": 1, "avg": 3}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "count", "avg"],
                "data": [
                    ["b", 1, 2],
                    ["c", 3, 31 / 3],
                    [None, 1, 3]
                ]
            }
        }
        self._execute_es_tests(test)

    def test_sum_column(self):
        test = {
            "name": "sum column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"value": "v", "aggregate": "sum"},
                "groupby": ["a"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b", "v": 2},
                    {"a": "c", "v": 31},
                    {"a": None, "v": 3}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "v"],
                "data": [
                    ["b", 2],
                    ["c", 31],
                    [None, 3]
                ]
            }
        }
        self._execute_es_tests(test)


    def test_where(self):
        test = {
            "name": "sum column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"value": "v", "aggregate": "max"},
                "groupby": ["a"],
                "where": {"term": {"a": "c"}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "c", "v": 13},
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "v"],
                "data": [
                    ["c", 13]
                ]
            }
        }
        self._execute_es_tests(test)

    def test_where_w_dimension(self):
        test = {
            "name": "sum column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"value": "v", "aggregate": "max"},
                "groupby": "a",
                "where": {"term": {"a": "c"}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "c", "v": 13}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "v"],
                "data": [
                    ["c", 13]
                ]
            }
        }
        self._execute_es_tests(test)

    def test_bad_groupby(self):
        test = {
            "data": [],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"value": "v", "aggregate": "max"},
                "groupby": [
                    {"value": "a", "allowNulls": False, "domain": {"type": "set", "partitions": ["b", "c"]}}
                ]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": []
            }
        }
        self.assertRaises(Exception, self._execute_es_tests, test)

    def test_empty_default_domain(self):
        test = {
            "name": "sum column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"value": "v", "aggregate": "max"},
                "groupby": ["a"],
                "where": {"term": {"a": "d"}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "v"],
                "data": []
            }
        }
        self._execute_es_tests(test)


    def test_many_aggs_on_one_column(self):
        # ES WILL NOT ACCEPT TWO (NAIVE) AGGREGATES ON SAME FIELD, COMBINE THEM USING stats AGGREGATION
        test = {
            # d = {"a": a, "v", v}
            "data": [{"_a": {"_b": {"_c": d.v}}, "_b": d.a} for d in wrap(simple_test_data)],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": [
                    {"name": "maxi", "value": "_a._b._c", "aggregate": "max"},
                    {"name": "mini", "value": "_a._b._c", "aggregate": "min"}
                ],
                "groupby": "_b"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"_b": "b", "mini": 2, "maxi": 2},
                    {"_b": "c", "mini": 7, "maxi": 13},
                    {"_b": None, "mini": 3, "maxi": 3}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["_b", "mini", "maxi"],
                "data": [
                    ["b", 2, 2],
                    ["c", 7, 13],
                    [None, 3, 3]
                ]
            }
        }
        self._execute_es_tests(test)


    # {
    #     "from": "unittest",
    #     "select": [
    #         {
    #             "value": "run.stats.bytes",
    #             "aggregate": "max"
    #         },
    #         {
    #             "value": "run.stats.bytes",
    #             "aggregate": "count"
    #         }
    #     ],
    #     "groupby": [
    #         "machine.platform"
    #     ],
    #     "where": {
    #         "and": [
    #             {
    #                 "eq": {
    #                     "etl.id": 0
    #                 }
    #             },
    #             {
    #                 "gt": {
    #                     "run.stats.bytes": 600000000
    #                 }
    #             }
    #         ]
    #     }
    # }




simple_test_data = [
    {"a": "c", "v": 13},
    {"a": "b", "v": 2},
    {"v": 3},
    {"a": "b"},
    {"a": "c", "v": 7},
    {"a": "c", "v": 11}
]


