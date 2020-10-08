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

import hashlib

from active_data.actions import save_query
from jx_elasticsearch import elasticsearch
from mo_dots import to_data, dict_to_data
from mo_future import text
from mo_json import value2json
from mo_json_config import URL
from mo_logs import Log
from mo_math import bytes2base64URL, bytes2base64
from mo_threads import Till
from mo_times import Timer
from tests.test_jx import BaseTestCase, TEST_TABLE


class TestLoadAndSaveQueries(BaseTestCase):

    def test_save_then_load(self):
        test = {
            "data": [
                {"a": "b"}
            ],
            "query": {
                "meta": {"save": True},
                "from": TEST_TABLE,
                "select": "a"
            },
            "expecting_list": {
                "meta": {
                    "format": "list"
                },
                "data": ["b"]
            }
        }

        settings = self.utils.fill_container(test)

        json = value2json({
            "from": settings.alias,
            "select": "a",
            "format": "list"
        })
        bytes = json.encode('utf8')
        expected_hash = bytes2base64URL(hashlib.sha1(bytes).digest()[0:6])
        Log.note("Flush saved query {{json}} with hash {{hash}}", json=json, hash=expected_hash)
        to_data(test).expecting_list.meta.saved_as = expected_hash

        self.utils.send_queries(test)

        # ENSURE THE QUERY HAS BEEN INDEXED
        container = elasticsearch.Index(index="saved_queries", type=save_query.DATA_TYPE, kwargs=settings)
        container.refresh()
        with Timer("wait for 5 seconds"):
            Till(seconds=5).wait()

        url = URL(self.utils.testing.query)
        response = self.utils.try_till_response(url.scheme + "://" + url.host + ":" + text(url.port) + "/find/" + expected_hash, data=b'')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.all_content, bytes)

    def test_recovery_of_empty_string(self):
        test = dict_to_data({
            "data": [
                {"a": "bee"}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "a",
                "where": {"prefix": {"a": ""}},
                "format": "list"
            },
            "expecting_list": {
                "meta": {
                    "format": "list"
                },
                "data": ["bee"]
            }
        })

        settings = self.utils.fill_container(test)

        bytes = value2json(test.query).encode('utf8')
        expected_hash = bytes2base64URL(hashlib.sha1(bytes).digest()[0:6])
        test.expecting_list.meta.saved_as = expected_hash

        test.query.meta = {"save": True}
        self.utils.send_queries(test)

        # ENSURE THE QUERY HAS BEEN INDEXED
        Log.note("Flush saved query")
        container = elasticsearch.Index(index="saved_queries", kwargs=settings)
        container.refresh()
        timeout = Till(seconds=5).wait()

        url = URL(self.utils.testing.query)
        url.path = ""
        while True:
            try:
                response = self.utils.try_till_response(url / "find" / expected_hash, data=b'')
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.all_content, bytes)
                break
            except Exception as cause:
                if timeout:
                    raise cause
