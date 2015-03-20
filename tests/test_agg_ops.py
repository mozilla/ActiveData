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



class TestAggOps(ActiveDataBaseTest):

    def test_simplest(self):
        test = {
            "data": [{"a": i} for i in range(30)],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"aggregate": "count"}
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {"count": 30}
            ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["count"],
                "data": [[30]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [],
                "data": {
                    "count": 30
                }
            }
        }
        self._execute_es_tests(test)
