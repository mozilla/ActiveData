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

from pyLibrary.debugs.logs import Log
from pyDots import wrap
from pyLibrary.queries.containers.list_usingSQLite import Table_usingSQLite
from pyLibrary.queries.expressions import NullOp
from pyLibrary.testing.fuzzytestcase import FuzzyTestCase


class TestContainer(FuzzyTestCase):

    def test_assumption(self):
        table = Table_usingSQLite("test_table")

        collection = {}
        uid = table.next_uid()
        ok, required_changes = table.flatten({"a": 1, "b": "v"}, uid, collection)
        table.change_schema(required_changes)

        uid = table.next_uid()
        ok, required_changes = table.flatten({"a": None, "b": "v"}, uid, collection)
        uid = table.next_uid()
        ok, required_changes = table.flatten({"a": 1, "b": None}, uid, collection)

        table._insert(collection)

        result = table.db.query('SELECT coalesce("a.$number", "b.$string"), length(coalesce("a.$number", "b.$string")) FROM '+table.name)
        self.assertEqual(result, {"data": [(1.0, 3), ('v', 1), (1.0, 3)]})

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

        # VERIFY CONTENT OF TABLE
        result = table.db.query('SELECT * FROM "test_table.a.b" ORDER BY __id__')
        self.assertEqual(result, {"data": [
            (2, 2, 0, 0, None),
            (4, 3, 0, 0, None),
            (5, 3, 1, 1, None),
            (7, 6, 0, None, 'value')
        ]})

        # VIEW METADATA
        command = 'PRAGMA table_info("test_table")'
        Log.note("Metadata\n{{meta|json|indent}}", meta=table.db.query(command))

        # VIEW METADATA
        command = 'PRAGMA table_info("test_table.a.b")'
        Log.note("Metadata\n{{meta|json|indent}}", meta=table.db.query(command))

        # VERIFY PULLING DATA
        result = table.query({"from": table.name})
        self.assertEqual(result, {"data": [
            {"a": 0},
            {"a": {"b": 0}},
            {"a": {"b": [0, 1]}},
            {"a": {"b": "value"}}
        ]})
        Log.note("{{result}}", result=result)


