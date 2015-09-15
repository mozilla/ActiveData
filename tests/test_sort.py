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
from unittest import skip
import base_test_class
from pyLibrary.dot import wrap
from tests.base_test_class import ActiveDataBaseTest

lots_of_data = wrap([{"a": i} for i in range(30)])


class TestSorting(ActiveDataBaseTest):
    @skip
    def test_name_and_direction_sort(self):
        test = {
            "data": [
                {"a": 1},
                {"a": 3},
                {"a": 4},
                {"a": 6},
                {"a": 2}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": "a",
                "sort": {"a": "desc"}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": ["b"]
            }
        }
        self._execute_es_tests(test)



