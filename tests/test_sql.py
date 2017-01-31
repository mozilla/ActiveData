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

from mo_dots import wrap
from tests import NULL
from tests.base_test_class import ActiveDataBaseTest, TEST_TABLE

simple_test_data = [
    {"a": "c", "v": 13},
    {"a": "b", "v": 2},
    {"v": 3},
    {"a": "b"},
    {"a": "c", "v": 7},
    {"a": "c", "v": 11}
]


class TestSQL(ActiveDataBaseTest):

    def test_count(self):
        test = {
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,  # NEEDED TO FILL THE TABLE
                "sql": 'select a as "a", count(1) as "count" from '+TEST_TABLE+' group by a'
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "count"],
                "data": [
                    ["b", 2],
                    ["c", 3],
                    [NULL, 1]
                ]
            }
        }
        self.execute(test)

    def test_filter(self):
        test = {
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,  # NEEDED TO FILL THE TABLE
                "sql": 'select * from '+TEST_TABLE+' where v>=3'
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "v"],
                "data": [
                    ["c", 13],
                    [NULL, 3],
                    ["c", 7],
                    ["c", 11]
                ]
            }
        }
        self.execute(test)

    def execute(self, test):
        test = wrap(test)
        self.utils.fill_container(test, tjson=False)
        test.query.sql = test.query.sql.replace(TEST_TABLE, test.query['from'])
        self.utils.send_queries(test)
