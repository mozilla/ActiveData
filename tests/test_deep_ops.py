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


lots_of_data = wrap([{"a": i} for i in range(30)])


class TestDeepOps(ActiveDataBaseTest):

    def test_deep_select_column(self):
        test = {
            "data": [
                {"a": [
                    {"b": "x", "v": 2},
                    {"b": "y", "v": 3}
                ]},
                {"a": {"b": "x", "v": 5}},
                {"a": [
                    {"b": "x", "v": 7},
                ]},
                {"c": "x"}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index+".a",
                "select": {"value": "a.v", "aggregate": "sum"},
                "edges": ["a.b"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": {"b": "x", "v": 14}},
                    {"a": {"b": "y", "v": 3}},
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a.b", "a.v"],
                "data": [
                    ["x", 14],
                    ["y", 3]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "a.b",
                        "allowNulls": False,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [{"value": "x"}, {"value": "y"}]
                        }
                    }
                ],
                "data": {
                    "a.v": [14, 3]
                }
            }
        }
        self._execute_es_tests(test)

    def test_deep_select_column_w_groupby(self):
        test = {
            "data": [
                {"a": [
                    {"b": "x", "v": 2},
                    {"b": "y", "v": 3}
                ]},
                {"a": {"b": "x", "v": 5}},
                {"a": [
                    {"b": "x", "v": 7},
                ]},
                {"c": "x"}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index+".a",
                "select": {"value": "a.v", "aggregate": "sum"},
                "groupby": ["a.b"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": {"b": "x", "v": 14}},
                    {"a": {"b": "y", "v": 3}},
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a.b", "a.v"],
                "data": [
                    ["x", 14],
                    ["y", 3]
                ]
            }
        }
        self._execute_es_tests(test)

    def test_bad_deep_select_column_w_groupby(self):
        test = {
            "data": [  # WE NEED SOME DATA TO MAKE A NESTED COLUMN
                {"a": {"b": "x"}}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"value": "a.v", "aggregate": "sum"},
                "groupby": ["a.b"]
            },
            "expecting_list": {  # DUMMY: SO AN QUERY ATTEMPT IS MADE
                "meta": {"format": "list"},
                "data": []
            }
        }
        self.assertRaises(Exception, self._execute_es_tests, test)

    def test_abs_shallow_select(self):
        # TEST THAT ABSOLUTE COLUMN NAMES WORK (WHEN THEY DO NOT CONFLICT WITH RELATIVE PROPERTY NAME)
        test = {
            "data": [
                {"o": 3, "_a": [
                    {"b": "x", "v": 2},
                    {"b": "y", "v": 3}
                ]},
                {"o": 1, "_a": {"b": "x", "v": 5}},
                {"o": 2, "_a": [
                    {"b": "x", "v": 7},
                ]},
                {"o": 4, "c": "x"}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index+"._a",
                "select": ["b", "o"],
                "where": {"eq": {"b": "x"}},
                "sort": ["o"]
            },
            "es_query": {  # FOR REFERENCE
                "query": {"nested": {
                    "path": "_a",
                    "inner_hits": {},
                    "filter": {"term": {"_a.b": "x"}}
                }},
                "fields": ["o"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"o": 3, "b": "x"},
                    {"o": 3, "b": "y"},
                    {"o": 1, "b": "x"},
                    {"o": 2, "b": "x"},
                    {"o": 4}
                ]},
            # "expecting_table": {
            #     "meta": {"format": "table"},
            #     "header": ["o", "a", "c"],
            #     "data": [
            #         [1, {"b": "x", "v": 5}, None],
            #         [2, {"b": "x", "v": 7}, None],
            #         [3,
            #             [
            #                 {"b": "x", "v": 2},
            #                 {"b": "y", "v": 3}
            #             ],
            #             None
            #         ],
            #         [4, None, "x"]
            #     ]
            # },
            # "expecting_cube": {
            #     "meta": {"format": "cube"},
            #     "edges": [
            #         {
            #             "name": "rownum",
            #             "domain": {"type": "rownum", "min": 0, "max": 4, "interval": 1}
            #         }
            #     ],
            #     "data": {
            #         "a": [
            #             {"b": "x", "v": 5},
            #             {"b": "x", "v": 7},
            #             [
            #                 {"b": "x", "v": 2},
            #                 {"b": "y", "v": 3}
            #             ],
            #             None
            #         ],
            #         "c": [None, None, None, "x"],
            #         "o": [1, 2, 3, 4]
            #     }
            # }
        }

        self._execute_es_tests(test)



    def test_select_whole_document(self):
        test = {
            "data": [
                {"o": 3, "_a": [
                    {"b": "x", "v": 2},
                    {"b": "y", "v": 3}
                ]},
                {"o": 1, "_a": {"b": "x", "v": 5}},
                {"o": 2, "_a": [
                    {"b": "x", "v": 7},
                ]},
                {"o": 4, "c": "x"}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": "*",
                "sort": ["o"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"o": 1, "a": {"b": "x", "v": 5}},
                    {"o": 2, "a": {"b": "x", "v": 7}},
                    {"o": 3, "a": [
                        {"b": "x", "v": 2},
                        {"b": "y", "v": 3}
                    ]},
                    {"o": 4, "c": "x"}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["o", "a", "c"],
                "data": [
                    [1, {"b": "x", "v": 5}, None],
                    [2, {"b": "x", "v": 7}, None],
                    [3,
                        [
                            {"b": "x", "v": 2},
                            {"b": "y", "v": 3}
                        ],
                        None
                    ],
                    [4, None, "x"]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 4, "interval": 1}
                    }
                ],
                "data": {
                    "a": [
                        {"b": "x", "v": 5},
                        {"b": "x", "v": 7},
                        [
                            {"b": "x", "v": 2},
                            {"b": "y", "v": 3}
                        ],
                        None
                    ],
                    "c": [None, None, None, "x"],
                    "o": [1, 2, 3, 4]
                }
            }
        }

        self._execute_es_tests(test)
