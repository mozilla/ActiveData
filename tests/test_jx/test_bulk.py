# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#

from __future__ import absolute_import, division, unicode_literals

from unittest import skipIf

from jx_base.query import MAX_LIMIT
from jx_elasticsearch.es52 import agg_bulk
from jx_python import jx
from mo_dots import wrap
from mo_future import text
from mo_logs import Log
from mo_threads import Till
from mo_times import MINUTE
from mo_http import http
from tests.test_jx import BaseTestCase, TEST_TABLE


class TestBulk(BaseTestCase):
    def retry(self, url):
        def output(get_content):
            timeout = Till(seconds=MINUTE.seconds)
            while not timeout:
                try:
                    get_content()
                    break
                except Exception as e:
                    if "403" in e:
                        Log.note("waiting for {{url}}", url=url)
                        Till(seconds=2).wait()
                        continue
                    Log.error("failed", cause=e)

            self.assertFalse(timeout, "timeout")

        return output

    @skipIf(not agg_bulk.S3_CONFIG, "can not test S3")
    def test_bulk_aggs_list(self):
        data = wrap([{"a": "test" + text(i)} for i in range(10111)])
        expected = jx.sort([{"a": r.a, "count": 1} for r in data], "a")

        test = wrap(
            {
                "data": data,
                "query": {
                    "from": TEST_TABLE,
                    "groupby": "a",
                    "limit": len(data),
                    "chunk_size": 1000,
                    "sort": "a",
                },
                "expecting_list": {
                    "data": expected[:MAX_LIMIT]
                },  # DUMMY, TO ENSURE LOADED
            }
        )
        self.utils.execute_tests(test)

        test.query.format = "list"
        test.query.destination = "url"
        result = http.post_json(url=self.utils.testing.query, json=test.query,)
        self.assertEqual(result.meta.format, "list")

        @self.retry(result.url)
        def get_content():
            content = http.get_json(result.url)
            self.assertEqual(content.meta.format, "list")
            sorted_content = jx.sort(content.data, "a")
            sorted_expected = jx.sort(expected, "a")
            self.assertEqual(sorted_content, sorted_expected)

    @skipIf(not agg_bulk.S3_CONFIG, "can not test S3")
    def test_scroll_query_list(self):
        data = wrap([{"a": "test" + text(i)} for i in range(10111)])
        expected = jx.sort(data, "a")

        test = wrap(
            {
                "data": data,
                "query": {
                    "from": TEST_TABLE,
                    "limit": len(data),
                    "chunk_size": 10000,
                    "sort": "a",
                },
                "expecting_list": {
                    "data": expected[:MAX_LIMIT]
                },  # DUMMY, TO ENSURE LOADED
            }
        )
        self.utils.execute_tests(test)

        test.query.format = "list"
        test.query.destination = "url"
        result = http.post_json(url=self.utils.testing.query, json=test.query,)
        self.assertEqual(result.meta.format, "list")

        @self.retry(result.url)
        def get_content():
            content = http.get_json(result.url)
            self.assertEqual(content.meta.format, "list")
            sorted_content = jx.sort(content.data, "a")
            self.assertEqual(sorted_content, expected)

    @skipIf(not agg_bulk.S3_CONFIG, "can not test S3")
    def test_bulk_aggs_table(self):
        data = wrap([{"a": "test" + text(i)} for i in range(10111)])
        expected = jx.sort([{"a": r.a, "count": 1} for r in data], "a")

        test = wrap(
            {
                "data": data,
                "query": {
                    "from": TEST_TABLE,
                    "groupby": "a",
                    "limit": len(data),
                    "chunk_size": 10000,
                    "sort": "a",
                },
                "expecting_list": {
                    "data": expected[:MAX_LIMIT]
                },  # DUMMY, TO ENSURE LOADED
            }
        )
        self.utils.execute_tests(test)

        test.query.format = "table"
        test.query.destination = "url"
        result = http.post_json(url=self.utils.testing.query, json=test.query,)
        self.assertEqual(result.meta.format, "table")

        @self.retry(result.url)
        def get_content():
            content = http.get_json(result.url)
            self.assertEqual(content.header, ["a", "count"])
            self.assertEqual(content.meta.format, "table")
            sorted_content = jx.sort(content.data, 0)
            sorted_expected = [(row.a, row.c) for row in expected]
            self.assertEqual(sorted_content, sorted_expected)

    @skipIf(not agg_bulk.S3_CONFIG, "can not test S3")
    def test_scroll_query_table(self):
        data = wrap([{"a": "test" + text(i)} for i in range(10111)])
        expected = jx.sort(data, "a")

        test = wrap(
            {
                "data": data,
                "query": {
                    "from": TEST_TABLE,
                    "select": ["a"],
                    "limit": len(data),
                    "chunk_size": 10000,
                    "sort": "a",
                },
                "expecting_list": {
                    "data": expected[:MAX_LIMIT]
                },  # DUMMY, TO ENSURE LOADED
            }
        )
        self.utils.execute_tests(test)

        test.query.format = "table"
        test.query.sort = None
        test.query.destination = "url"
        result = http.post_json(url=self.utils.testing.query, json=test.query,)
        self.assertEqual(result.meta.format, "table")

        @self.retry(result.url)
        def get_content():
            content = http.get_json(result.url)
            self.assertEqual(content.header, ["a"])
            self.assertEqual(content.meta.format, "table")
            sorted_content = jx.sort(content.data, 0)
            sorted_expected = [(row.a,) for row in expected]
            self.assertEqual(sorted_content, sorted_expected)
