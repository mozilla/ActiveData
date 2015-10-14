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
            "data": [  # PROPERTIES STARTING WITH _ ARE NESTED AUTOMATICALLY
                       {"a": {"b": 0, "c": 0}},
                       {"a": {"b": 0, "c": 1}},
                       {"a": {"b": 1, "c": 0}},
                       {"a": {"b": 1, "c": 1}},
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": "*",
                "where": {"eq": ["a.b", "a.c"]},
                "sort": "a.b"
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {"a": {"b": 0, "c": 0}},
                {"a": {"b": 1, "c": 1}},
            ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a.b", "a.c"],
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
                    "a.b": [0, 1],
                    "a.c": [0, 1]
                }
            }
        }
        self._execute_es_tests(test)


    def test_add_expression(self):
        test = {
            "data": [  # PROPERTIES STARTING WITH _ ARE NESTED AUTOMATICALLY
                       {"a": {"b": 0, "c": 0}},
                       {"a": {"b": 0, "c": 1}},
                       {"a": {"b": 1, "c": 0}},
                       {"a": {"b": 1, "c": 1}},
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": "*",
                "where": {"eq": [{"add": ["a.b", 1]}, "a.c"]},
                "sort": "a.b"
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {"a": {"b": 0, "c": 1}}
            ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a.b", "a.c"],
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
                    "a.b": [0],
                    "a.c": [1]
                }
            }
        }
        self._execute_es_tests(test)

    def test_regexp_expression(self):
        test = {
            "data": [{"_a":[
                {"a": "abba"},
                {"a": "aaba"},
                {"a": "aaaa"},
                {"a": "aa"},
                {"a": "aba"},
                {"a": "aa"},
                {"a": "ab"},
                {"a": "ba"},
                {"a": "a"},
                {"a": "b"}
            ]}],
            "query": {
                "from": base_test_class.settings.backend_es.index+"._a",
                "select": "*",
                "where": {"regex": {"a": ".*b.*"}},
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {"a": "abba"},
                {"a": "aaba"},
                {"a": "aba"},
                {"a": "ab"},
                {"a": "ba"},
                {"a": "b"}
            ]}
        }
        self._execute_es_tests(test)
