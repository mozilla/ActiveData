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
from pyLibrary.maths import Math
from pyLibrary.queries import query
from tests.base_test_class import ActiveDataBaseTest


lots_of_data = wrap([{"a": i} for i in range(30)])


class TestSetOps(ActiveDataBaseTest):

    def test_length(self):
        test = {
            "data": [
                {"v": "1"},
                {"v": "22"},
                {"v": "333"},
                {"v": "4444"},
                {"v": "55555"}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"name": "l", "value": {"length": "v"}},
                "sort": "v"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [1, 2, 3, 4, 5]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["l"],
                "data": [
                    [1],
                    [2],
                    [3],
                    [4],
                    [5]
                ]
           },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 5, "interval": 1}
                    }
                ],
                "data": {
                    "l": [1, 2, 3, 4, 5]
                }
            }
        }
        self._execute_es_tests(test)

    def test_length_w_inequality(self):
        test = {
            "data": [
                {"v": "1"},
                {"v": "22"},
                {"v": "333"},
                {"v": "4444"},
                {"v": "55555"}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": "v",
                "where": {
                    "gt": [
                        {
                            "length": "v"
                        },
                        2
                    ]
                },
                "sort": "v"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": ["333", "4444", "55555"]
            }
        }
        self._execute_es_tests(test)

    def test_left(self):
        test = {
            "data": [
                {},
                {"v": "1"},
                {"v": "22"},
                {"v": "333"},
                {"v": "4444"}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"name": "v", "value": {"left": {"v": 2}}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [None, "1", "22", "33", "44"]
            }
        }
        self._execute_es_tests(test)


    def test_ne(self):
        test = {
            "data": [
                {"a": 0, "b": 0},
                {"a": 0, "b": 1},
                {"a": 0},
                {"a": 1, "b": 0},
                {"a": 1, "b": 1},
                {"a": 1},
                {"b": 0},
                {"b": 1},
                {}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": ".",
                "where": {"ne": ["a", "b"]}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": 0, "b": 1},
                    {"a": 1, "b": 0}
                ]
            }
        }
        self._execute_es_tests(test)

    def test_select_when(self):
        test = {
            "data": [
                {"a": 0, "b": 0},
                {"a": 0, "b": 1},
                {"a": 0},
                {"a": 1, "b": 0},
                {"a": 1, "b": 1},
                {"a": 1},
                {"b": 0},
                {"b": 1},
                {}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": ["a", "b", {"name": "i/o", "value": {"when": {"eq": ["a", "b"]}, "then": 1, "else": 2}}],
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": 0, "b": 0, "i/o": 1},
                    {"a": 0, "b": 1, "i/o": 2},
                    {"a": 0, "i/o": 2},
                    {"a": 1, "b": 0, "i/o": 2},
                    {"a": 1, "b": 1, "i/o": 1},
                    {"a": 1, "i/o": 2},
                    {"b": 0, "i/o": 2},
                    {"b": 1, "i/o": 2},
                    {"i/o": 1}
                ]
            }
        }
        self._execute_es_tests(test)

    def test_select_add(self):
        test = {
            "data": [
                {"a": 0, "b": 0},
                {"a": 0, "b": 1},
                {"a": 0},
                {"a": 1, "b": 0},
                {"a": 1, "b": 1},
                {"a": 1},
                {"b": 0},
                {"b": 1},
                {}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": ["a", "b", {"name": "t", "value": {"add": ["a", "b"]}}]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": 0, "b": 0, "t": 0},
                    {"a": 0, "b": 1, "t": 1},
                    {"a": 0, "t": 0},
                    {"a": 1, "b": 0, "t": 1},
                    {"a": 1, "b": 1, "t": 2},
                    {"a": 1, "t": 1},
                    {"b": 0, "t": 0},
                    {"b": 1, "t": 1},
                    {}
                ]
            }
        }
        self._execute_es_tests(test)

    def test_select_add_w_default(self):
        test = {
            "data": [
                {"a": 1, "b": -1},  # DUMMY VALUE TO CREATE COLUMNS
                {}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": ["a", "b", {"name": "t", "value": {"add": ["a", "b"], "default": 0}}]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": 1, "b": -1, "t": 0},
                    {"t": 0}
                ]
            }
        }
        self._execute_es_tests(test)


    def test_select_average(self):
        test = {
            "data": [{"a": {"_b": [
                {"a": 0, "b": 0},
                {"a": 0, "b": 1},
                {"a": 0},
                {"a": 1, "b": 0},
                {"a": 1, "b": 1},
                {"a": 1},
                {"b": 0},
                {"b": 1},
                {}
            ]}}],
            "query": {
                "from": base_test_class.settings.backend_es.index+".a._b",
                "select": [
                    {"aggregate": "count"},
                    {"name": "t", "value": {"add": ["a", "b"]}, "aggregate": "average"}
                ],
                "edges": ["a", "b"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": 0, "b": 0, "count": 1, "t": 0},
                    {"a": 0, "b": 1, "count": 1, "t": 1},
                    {"a": 0, "count": 1, "t": 0},
                    {"a": 1, "b": 0, "count": 1, "t": 1},
                    {"a": 1, "b": 1, "count": 1, "t": 2},
                    {"a": 1, "count": 1, "t": 1},
                    {"b": 0, "count": 1, "t": 0},
                    {"b": 1, "count": 1, "t": 1},
                    {"count": 1}
                ]
            }
        }
        self._execute_es_tests(test)


# SELECT CLAUSE WITH SOMETHING A BIT MORE COMPLICATED
# q = {
#     "select": [
#         {
#             "value": {"when": {"eq": {"harness.step": "run-tests"}, "then": "harness.duration"}},
#             "aggregate": "average"
#         },
#         {
#             "value": {"when": {"neq": {"harness.step": "run-tests"}, "then": {"add": ["builder.duration", "harness.duration"]}}},
#             "aggregate": "average"
#         },
#         {"name": "b_duration", "value": "builder.duration", "aggregate": "average"},
#         {"name": "h_duration", "value": "harness.duration", "aggregate": "average"},
#         {"aggregate":"count"}
#     ],
#     "edges": [
#         {"name":"date", "value":"action.start_time", "domain":{
#             "min": GUI.state.sampleMin,
#             "max": GUI.state.sampleMax,
#             "interval": GUI.state.sampleInterval
#         }}
#     ],
#     "from": "jobs.action.timings",
#     "where": {"and": [
#         focusFilter,
#         {"gte":{"action.start_time": GUI.state.sampleMin}},
#         {"lt": {"action.start_time": GUI.state.sampleMax}},
#         {"or": steps.select("step").map(function(v){
#             if (v[1]){
#                 return {"eq": {"builder.step": v[0], "harness.step": v[1]}};
#             }else{
#                 return {"and":[
#                     {"eq": {"builder.step": v[0]}},
#                     {"missing":"harness.step"}
#                 ]};
#             }//endif
#         })}
#     ]},
#     "limit": 1000,
#     "format": "list"
# }
