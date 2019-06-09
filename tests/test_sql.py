# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

from active_data.actions.sql import parse_sql
from jx_base.expressions import NULL
from mo_dots import Data, wrap
from mo_files.url import URL
from mo_json import json2value, utf82unicode, value2json
from mo_logs import Log
from tests import compare_to_expected
from tests.test_jx import BaseTestCase, TEST_TABLE

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
            "header": ["."],
            "data": [
                [1]
            ]
        }
        result = self._run_sql_query(sql)
        compare_to_expected(result.meta.jx_query, result, expected, places=6)

    def test_groupby_w_aggregates(self):
        sql = """
        SELECT 
            floor(run.timestamp/86400) as date,
            count(result.value) AS "count",
            median(result.value) AS "median",
            percentile(result.value, 0.9) AS "90th"
        FROM 
            perf 
        WHERE 
            run.timestamp>=date('today-month') AND
            run.framework.name='vcs' AND
            run.suite='clone'
        GROUP BY
            floor(run.timestamp/86400)
        ORDER BY 
            floor(run.timestamp/86400)
        """

        expected = {
            "select": [
                {"name": "count", "value": "result.value", "aggregate": "count"},
                {"name": "median", "value": "result.value", "aggregate": "median"},
                {"name": "90th", "value": "result.value", "aggregate": "percentile", "percentile": 0.9}
            ],
            "from": "perf",
            "groupby": {
                "name": "date",
                "value": {"floor": {"div": ["run.timestamp", 86400]}}
            },
            "where": {"and": [
                {"gte": ["run.timestamp", {"date": {"literal": "today-month"}}]},
                {"eq": ["run.framework.name", {"literal": "vcs"}]},
                {"eq": ["run.suite", {"literal": "clone"}]}
            ]},
            "sort": {"value": {"floor": {"div": ["run.timestamp", 86400]}}}
        }

        result = parse_sql(sql)
        self.assertEqual(result, expected, "expecting to be parsed ")

    def test_in_clause(self):
        sql = "select * from " + TEST_TABLE + " where a IN ('b', 'c')"
        expected = {
            "meta": {"format": "table"},
            "header": ["a", "v"],
            "data": [
                ["c", 13],
                ["b", 2],
                ["b", None],
                ["c", 7],
                ["c", 11]
            ]
        }
        result = self._run_sql_query(sql)
        compare_to_expected(result.meta.jx_query, result, expected, places=6)

    def test_empty_count(self):
        sql = "select count() from " + TEST_TABLE
        expected = {
            "meta": {"format": "table"},
            "header": ["count"],
            "data": [[6]]
        }
        result = self._run_sql_query(sql)
        compare_to_expected(result.meta.jx_query, result, expected, places=6)

    def test_tuid_health(self):
        sql = """
            SELECT 
                count(1) AS error_count,
                floor(timestamp, 86400) AS "date"
            FROM "debug-etl"
            WHERE timestamp>date("today-month")
              AND template='TUID service has problems.'
            GROUP BY floor(timestamp, 86400) AS "date"
        """

        jx_query = parse_sql(sql)
        expected = {
            "format": "table",
            "from": "debug-etl",
            "groupby": {"name": "date", "value": {"floor": ["timestamp", 86400]}},
            "select": [
                {"aggregate": "count", "name": "error_count", "value": 1}
            ],
            "where": {"and": [
                {"gt": ["timestamp", {"date": "today-month"}]},
                {"eq": ["template", {"literal": "TUID service has problems."}]}
            ]}
        }
        self.assertAlmostEqual(jx_query, expected, places=6)

    def execute(self, test):
        test = wrap(test)
        self.utils.fill_container(test)
        test.query.sql = test.query.sql.replace(TEST_TABLE, test.query['from'])
        self.utils.send_queries(test)

    def _run_sql_query(self, sql):
        if not self.utils.testing.sql:
            Log.error("This test requires a `testing.sql` parameter in the config file")

        test = Data(data=simple_test_data)
        self.utils.fill_container(test)
        sql = sql.replace(TEST_TABLE, test.query['from'])

        url = URL(self.utils.testing.sql)
        response = self.utils.post_till_response(str(url), json={"meta": {"testing": True}, "sql": sql})
        self.assertEqual(response.status_code, 200)
        return json2value(utf82unicode(response.content))


_ = value2json
