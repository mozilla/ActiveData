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


class TestFilters(ActiveDataBaseTest):
    def test_where_expression(self):
        test = {
            "data": [  # PROPERTIES STARTING WITH _ ARE NOT NESTED AUTOMATICALLY
                       {"_a": {"_b": 0, "_c": 0}},
                       {"_a": {"_b": 0, "_c": 1}},
                       {"_a": {"_b": 1, "_c": 0}},
                       {"_a": {"_b": 1, "_c": 1}},
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": "*",
                "where": {"eq": ["_a._b", "_a._c"]},
                "sort": "_a._b"
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {"_a": {"_b": 0, "_c": 0}},
                {"_a": {"_b": 1, "_c": 1}},
            ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["_a._b", "_a._c"],
                "data": [[0, 0], [1, 1]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 2, "interval": 1}
                    }
                ],
                "data": {
                    "_a._b": [0, 1],
                    "_a._c": [0, 1]
                }
            }
        }
        self._execute_es_tests(test)


    def test_add_expression(self):
        test = {
            "data": [  # PROPERTIES STARTING WITH _ ARE NOT NESTED AUTOMATICALLY
                       {"_a": {"_b": 0, "_c": 0}},
                       {"_a": {"_b": 0, "_c": 1}},
                       {"_a": {"_b": 1, "_c": 0}},
                       {"_a": {"_b": 1, "_c": 1}},
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": "*",
                "where": {"eq": [{"add": ["_a._b", 1]}, "_a._c"]},
                "sort": "_a._b"
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {"_a": {"_b": 0, "_c": 1}}
            ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["_a._b", "_a._c"],
                "data": [[0, 1]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 1, "interval": 1}
                    }
                ],
                "data": {
                    "_a._b": [0],
                    "_a._c": [1]
                }
            }
        }
        self._execute_es_tests(test)


