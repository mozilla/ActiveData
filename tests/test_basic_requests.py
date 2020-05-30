# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

from active_data import OVERVIEW
from mo_dots import dict_to_data
from mo_http import http
from mo_json import value2json
from mo_json_config import URL
from mo_logs import Log
from tests.test_jx import BaseTestCase, TEST_TABLE


class TestBasicRequests(BaseTestCase):

    def test_empty_request(self):
        response = self.utils.try_till_response(self.utils.testing.query, data=b"")
        self.assertEqual(response.status_code, 400)

    def test_root_request(self):
        if self.utils.not_real_service():
            return

        url = URL(self.utils.testing.query)
        url.path = ""
        url = str(url)
        response = self.utils.try_till_response(url, data=b"")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.all_content, OVERVIEW)

    def test_favicon(self):
        url = URL(self.utils.testing.query)
        url.path = "/favicon.ico"

        response = self.utils.try_till_response(str(url), data=b"")
        self.assertEqual(response.status_code, 200)

    def test_bad_file_request(self):
        url = URL(self.utils.testing.query)
        url.path = "/tools/../../README.md"

        response = self.utils.try_till_response(str(url), data=b"")

        if response.status_code == 200:
            Log.note("Response is:\n{{response|indent}}", response=response.content)

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.content, b"")

    def test_query_on_static_file(self):
        url = URL(self.utils.testing.query)
        url.path = "/tools/index.html?123"

        response = self.utils.try_till_response(str(url), data=b"")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.all_content, OVERVIEW)

    def test_rest_get(self):
        data = [
            {"a": 0, "b": 0},
            {"a": 0, "b": 1},
            {"a": 1, "b": 0},
            {"a": 1, "b": 1}
        ]

        test = dict_to_data({
            "data": data,
            "query": {"from": TEST_TABLE},
            "expecting_list": {"data": data}
        })

        self.utils.execute_tests(test)  # LOAD, AND SET meta.testing=True

        url = URL(self.utils.testing.query)
        url.path = "json/" + test.query['from']
        url.query = {"a": 1}

        response = self.utils.try_till_response(str(url), data=b"")
        self.assertEqual(response.status_code, 200)

        # ORDER DOES NOT MATTER, TEST EITHER
        expected1 = value2json([{"a": 1, "b": 0}, {"a": 1, "b": 1}], pretty=True).encode('utf8')
        expected2 = value2json([{"a": 1, "b": 1}, {"a": 1, "b": 0}], pretty=True).encode('utf8')

        try:
            self.assertEqual(response.all_content, expected1)
        except Exception:
            self.assertEqual(response.all_content, expected2)

    def test_index_wo_name(self):
        data = {
            "name": "The Parent Trap",
            "released": "29 July` 1998",
            "imdb": "http://www.imdb.com/title/tt0120783/",
            "rating": "PG",
            "director": {"name": "Nancy Meyers", "dob": "December 8, 1949"}
        }
        container = self.utils._es_cluster.get_or_create_index(index=TEST_TABLE, kwargs=self.utils._es_test_settings)
        try:
            self.utils._es_cluster.delete_index(container.settings.index)
        except Exception:
            pass
        container = self.utils._es_cluster.get_or_create_index(index=TEST_TABLE, kwargs=self.utils._es_test_settings)
        container.add({"value": data})
        container.refresh()

        result = http.post_json(
            url=self.utils.testing.query,
            json={
                "from": container.settings.index,
                "meta": {"testing": True},
                "format": "list"
            }
        )
        self.assertEqual(result.data, [data])
        try:
            self.utils._es_cluster.delete_index(container.settings.index)
        except Exception:
            pass

    def test_query_on_es_fields(self):
        schema = {
            "settings": {"analysis": {
                "analyzer": {"whiteboard_tokens": {
                    "type": "custom",
                    "tokenizer": "whiteboard_tokens_pattern",
                    "filter": ["stop"]
                }},
                "tokenizer": {"whiteboard_tokens_pattern": {
                    "type": "pattern",
                    "pattern": "\\s*([,;]*\\[|\\][\\s\\[]*|[;,])\\s*"
                }}
            }},
            "mappings": {"test_result": {
                "properties": {"status_whiteboard": {
                    "type": "keyword",
                    "store": True,
                    "fields": {"tokenized": {"type": "text", "analyzer": "whiteboard_tokens"}}
                }}
            }}
        }

        test = {
            "schema": schema,
            "data": [
                {
                    "bug_id": 123,
                    "status_whiteboard": "[test][fx21]"
                }
            ],
            "query": {
                "select": ["status_whiteboard"],
                "from": TEST_TABLE
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [{"status_whiteboard": "[test][fx21]"}]
            }
        }
        self.utils.execute_tests(test)
