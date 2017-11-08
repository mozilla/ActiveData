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

from mo_dots import wrap, Data

from mo_json import json2value, utf82unicode

from mo_logs import Log
from mo_logs.url import URL
from tests import compare_to_expected
from tests.test_jx import BaseTestCase, TEST_TABLE, NULL

simple_test_data = [
    {"a": "c", "v": 13},
    {"a": "b", "v": 2},
    {"v": 3},
    {"a": "b"},
    {"a": "c", "v": 7},
    {"a": "c", "v": 11}
]


class TestSQL(BaseTestCase):

    def test_count(self):
        sql = 'select a as "a", count(1) as "count" from '+TEST_TABLE+' group by a'
        expected = {
            "meta": {"format": "table"},
            "header": ["a", "count"],
            "data": [
                ["b", 2],
                ["c", 3],
                [NULL, 1]
            ]
        }
        result = self._run_sql_query(sql)
        compare_to_expected(result.meta.jx_query, result, expected, places=6)

    def test_filter(self):
        sql = 'select * from '+TEST_TABLE+' where v>=3'
        expected = {
            "meta": {"format": "table"},
            "header": ["a", "v"],
            "data": [
                ["c", 13],
                [NULL, 3],
                ["c", 7],
                ["c", 11]
            ]
        }
        result = self._run_sql_query(sql)
        compare_to_expected(result.meta.jx_query, result, expected, places=6)

    def test_select_from_dual(self):
        sql = "SELECT 1"
        expected = {
            "meta": {"format": "table"},
            "header": ["1"],
            "data": [
                [1]
            ]
        }
        result = self._run_sql_query(sql)
        compare_to_expected(result.meta.jx_query, result, expected, places=6)


    def execute(self, test):
        test = wrap(test)
        self.utils.fill_container(test, tjson=False)
        test.query.sql = test.query.sql.replace(TEST_TABLE, test.query['from'])
        self.utils.send_queries(test)

    def _run_sql_query(self, sql):
        if not self.utils.sql_url:
            Log.error("This test requires a `sql_url` parameter in the settings file")

        test = Data(data=simple_test_data)
        self.utils.fill_container(test)
        sql = sql.replace(TEST_TABLE, test.query['from'])

        url = URL(self.utils.sql_url)
        response = self.utils.post_till_response(str(url), json={"meta": {"testing": True}, "sql": sql})
        self.assertEqual(response.status_code, 200)
        return json2value(utf82unicode(response.all_content))

