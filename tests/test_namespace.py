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
from pyLibrary.queries.namespace.rename import Rename
from pyLibrary.queries.namespace.typed import Typed
from pyLibrary.queries.qb_usingES import FromES
from tests.base_test_class import ActiveDataBaseTest


class Namespace(ActiveDataBaseTest):
    """
    TEST A VARIETY OF RE-NAMINGS
    """

    def test_rename_table(self):
        pass

    def test_rename_select(self):
        query = {
            "from": base_test_class.settings.backend_es.index,
            "select": ["o", "w"],
            "format": "table"
        }

        self._fill_es({"query":query, "data": deep_test_data})
        db = FromES(settings=base_test_class.settings.backend_es)
        db.namespaces += [Rename(dimensions={"w": "a.v"}), Typed()]
        result = db.query(query)
        self.compare_to_expected(query, result, {
            "header": ["o", "w"],
            "data": [
                [3, 2],
                [1, 5],
                [2, 7],
                [4, None]
            ]
        })

    def test_rename_select_to_struct(self):
        query = {
            "from": base_test_class.settings.backend_es.index,
            "select": ["o", "w"],
            "format": "table"
        }

        self._fill_es({"query":query, "data": deep_test_data})
        db = FromES(settings=base_test_class.settings.backend_es)
        db.namespaces += [Rename(dimensions={"w": {"a": "a.v", "b": "a.b"}}), Typed()]
        result = db.query(query)
        self.compare_to_expected(query, result, {
            "header": ["o", "w.a", "w.b"],
            "data": [
                [3, 2, "x"],
                [1, 5, "x"],
                [2, 7, "x"],
                [4, None, None]
            ]
        })

    def test_rename_select_to_list(self):
        query = {
            "from": base_test_class.settings.backend_es.index,
            "select": ["o", "w"],
            "format": "table"
        }

        self._fill_es({"query":query, "data": deep_test_data})
        db = FromES(settings=base_test_class.settings.backend_es)
        db.namespaces += [Rename(dimensions={"w": ["a.v", "a.b"]}), Typed()]
        result = db.query(query)
        self.compare_to_expected(query, result, {
            "header": ["o", "w"],
            "data": [
                [3, [2, "x"]],
                [1, [5, "x"]],
                [2, [7, "x"]],
                [4, [None, None]]
            ]
        })

    def test_rename_edge(self):
        query = {
            "from": base_test_class.settings.backend_es.index,
            "edges": ["w"],
            "format": "table"
        }

        self._fill_es({"query":query, "data": deep_test_data})
        db = FromES(settings=base_test_class.settings.backend_es)
        db.namespaces += [Rename(dimensions={"w": "a.b"}), Typed()]
        result = db.query(query)
        self.compare_to_expected(query, result, {
            "header": ["w", "count"],
            "data": [
                ["x", 3],
                [None, 1]
            ]
        })

    def test_rename_edge_to_struct(self):
        query = {
            "from": base_test_class.settings.backend_es.index,
            "edges": ["w"],
            "format": "table"
        }

        self._fill_es({"query":query, "data": deep_test_data})
        db = FromES(settings=base_test_class.settings.backend_es)
        db.namespaces += [Rename(dimensions={"name": "w", "fields": {"a": "a.v", "b": "a.b"}}), Typed()]
        result = db.query(query)
        self.compare_to_expected(query, result, {
            "header": ["w.a", "w.b", "count"],
            "data": [
                [2, "x", 1],
                [5, "x", 1],
                [7, "x", 1],
                [None, None, 1]
            ]
        })

    def test_rename_edge_to_list(self):
        """
        EXPAND DIMENSION
        """
        query = {
            "from": base_test_class.settings.backend_es.index,
            "edges": ["w"],
            "format": "cube"
        }

        self._fill_es({"query": query, "data": deep_test_data})
        db = FromES(settings=base_test_class.settings.backend_es)
        db.namespaces += [Rename(dimensions={"w": ["a.v", "a.b"]}), Typed()]
        result = db.query(query)
        self.compare_to_expected(query, result, {
            "edges": [{
                "name": "w",
                "domain": {"type": "set", "partitions": [
                    {"value": [2, "x"]},
                    {"value": [5, "x"]},
                    {"value": [7, "x"]},
                    {"value": None}
                ]}
            }],
            "data": {"count": [1, 1, 1, 1]}
        })



deep_test_data = [
    {"o": 3, "a": {"b": "x", "v": 2}},
    {"o": 1, "a": {"b": "x", "v": 5}},
    {"o": 2, "a": {"b": "x", "v": 7}},
    {"o": 4, "c": "x"}
]


