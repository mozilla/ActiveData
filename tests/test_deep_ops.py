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
from pyLibrary.dot import wrap, nvl
from pyLibrary.queries import query
from tests.base_test_class import ActiveDataBaseTest


lots_of_data = wrap([{"a": i} for i in range(30)])


class TestSetOps(ActiveDataBaseTest):

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
                "from": "testdata",
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
                    ["y", 3],
                    [None, None]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "a.b",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [{"value": "x"}, {"value": "y"}]
                        }
                    }
                ],
                "data": {
                    "a.v": [14, 3, None]
                }
            }
        }
        self._execute_es_tests(test)


