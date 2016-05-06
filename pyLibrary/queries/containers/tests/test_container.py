# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import division
from __future__ import unicode_literals

from pyLibrary.dot import wrap
from pyLibrary.queries.containers.Table_usingSQLite import Table_usingSQLite
from pyLibrary.queries.expressions import NullOp
from pyLibrary.testing.fuzzytestcase import FuzzyTestCase


class TestContainer(FuzzyTestCase):

    def test_flatten_inner(self):
        table = Table_usingSQLite("test_table")

        collection = {}
        uid = table.next_uid()  # 1
        ok, required_changes = table.flatten({"a": 0}, uid, collection)
        self.assertEqual(ok, False)
        self.assertEqual(required_changes, [{"add": {"name": "a", "type": "number", "nested_path": NullOp()}}])
        self.assertEqual(collection, {
            ".": {
                "rows": [{"__id__": 1, "a.$number": 0}],
                "active_columns": [{"es_column": "a.$number"}]
            }
        })
        table.change_schema(required_changes)
        table._insert(collection)

        collection = {}
        uid = table.next_uid()  # 2
        ok, required_changes = table.flatten({"a": {"b": 0}}, uid, collection)
        self.assertEqual(ok, False)
        self.assertEqual(required_changes, [
            {"add": {"name": "a", "type": "object", "nested_path": NullOp()}},
            {"add": {"name": "a.b", "type": "number", "nested_path": NullOp()}}
        ])
        self.assertEqual(collection, {
            ".": {
                "rows": [{"__id__": 2, "a.$object": ".", "a.b.$number": 0}],
                "active_columns": {wrap({"es_column": "a.b.$number"}), wrap({"es_column": "a.$object"})}
            }
        })
        table.change_schema(required_changes)
        table._insert(collection)

        collection = {}
        uid = table.next_uid()  # 3
        ok, required_changes = table.flatten({"a": {"b": [0, 1]}}, uid, collection)
        self.assertEqual(ok, False)
        self.assertEqual(required_changes, [{
            "add": {"name": "a.b", "type": "nested", "nested_path": NullOp()}
        }])
        self.assertEqual(collection, {
            ".": {
                "rows": [
                    {"__id__": 3, "a.$object": "."}
                ],
                "active_columns": {wrap({"es_column": "a.$object"}), wrap({"es_column": "a.b.$object"})}
            },
            "a.b": {
                "rows":[
                    {"__id__": 4, "__parent__": 3, "__order__": 0, "a.b.$number": 0},
                    {"__id__": 5, "__parent__": 3, "__order__": 1, "a.b.$number": 1}
                ],
                "active_columns": {wrap({"es_column": "a.b.$number"})}
            }
        })
        table.change_schema(required_changes)
        table._insert(collection)

        collection = {}
        uid = table.next_uid()  # 6
        ok, required_changes = table.flatten({"a": {"b": "value"}}, uid, collection)
        self.assertEqual(ok, False)
        self.assertEqual(required_changes, [{
            "add": {"name": "a.b", "type": "string", "nested_path": "a.b"}
        }])
        self.assertEqual(collection, {
            ".": {
                "rows": [
                    {"__id__": 6, "a.b.$object": ".", "a.$object": "."}
                ],
                "active_columns": {wrap({"es_column": "a.b.$object"}), wrap({"es_column": "a.$object"})}
            },
            "a.b": {
                "rows": [
                    {"__id__": 7, "__parent__": 6, "__order__": 0, "a.b.$string": "value"}
                ],
                "active_columns": {wrap({"es_column": "a.b.$string"})}
            }
        })
        table.change_schema(required_changes)
        table._insert(collection)

