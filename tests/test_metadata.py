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
from pyLibrary.dot import set_default, wrap

from tests.base_test_class import ActiveDataBaseTest


class TestMetadata(ActiveDataBaseTest):


    def test_meta(self):
        test = wrap({
            "query": {"from": "meta.columns"},
            "data": [
                {"a": "b"}
            ]
        })

        settings = self._fill_es(test, tjson=False)

        table_name = settings.index

        test = set_default(test, {
            "query": {
                "from": "meta.columns",
                "where": {"eq": {"table": table_name}}
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {"table": table_name, "name": "a", "type": "string", "depth":0}
            ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["table", "name", "type", "depth"],
                "data": [[table_name, "a", "string", 0]]
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
                    "table": [table_name],
                    "name": ["a"],
                    "type": ["string"],
                    "depth": 0
                }
            }
        })
        self._send_queries(settings, test)

    def test_get_nested_columns(self):
        settings = self._fill_es({
            "query": {"from": "meta.columns"},  # DUMMY QUERY
            "data": [
                {"_a": [
                    {"b": "x", "v": 2},
                    {"b": "y", "v": 3}
                ]},
                {"_a": {"b": "x", "v": 5}},
                {"_a": [
                    {"b": "x", "v": 7},
                ]},
                {"c": "x"}
            ]})

        table_name = settings.index

        test = {
            "query": {
                "from": "meta.columns",
                "where": {"term": {"table": table_name}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"table": table_name, "name": "_a", "type": "nested", "depth": 0},
                    {"table": table_name, "name": "_a.b", "type": "string", "depth": 1},
                    {"table": table_name, "name": "_a.v", "type": "long", "depth": 1},
                    {"table": table_name, "name": "c", "type": "string", "depth": 0},
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["table", "name", "depth", "type"],
                "data": [
                    [table_name, "_a", 0, "nested"],
                    [table_name, "_a.b", 1, "string"],
                    [table_name, "_a.v", 1, "long"],
                    [table_name, "c", 0, "string"]
                ]
            }
        }

        self._send_queries(settings, test)
