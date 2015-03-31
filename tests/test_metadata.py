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

from tests.base_test_class import ActiveDataBaseTest


class TestMetadata(ActiveDataBaseTest):

    def test_get_columns(self):
        settings = self._fill_es({
            "query": {"from": ""},  # DUMMY QUERY
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
            ]})

        ALIAS = settings.index

        test = {
            "query": {
                "from": "metadata.columns",
                "where": {"term": {"cube": ALIAS}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"cube": ALIAS, "property": "a", "type": "nested"},
                    {"cube": ALIAS, "property": "a.b", "type": "string"},
                    {"cube": ALIAS, "property": "a.v", "type": "long"},
                    {"cube": ALIAS, "property": "c", "type": "string"},
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["cube", "property", "type"],
                "data": [
                    [ALIAS, "a", "nested"],
                    [ALIAS, "a.b", "string"],
                    [ALIAS, "a.v", "long"],
                    [ALIAS, "c", "string"]
                ]
            }
        }

        self._send_queries(settings, test)
