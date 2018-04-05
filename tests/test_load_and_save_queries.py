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

import hashlib

from future.utils import text_type

from mo_dots import wrap
from mo_json import value2json
from mo_json_config import URL
from mo_logs import Log
from mo_logs.strings import unicode2utf8
from mo_threads import Till
from mo_times import Timer
from pyLibrary import convert
from pyLibrary.env import elasticsearch
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

        bytes = unicode2utf8(value2json({
            "from": settings.index,
            "select": "a",
            "format": "list"
        }))
        expected_hash = convert.bytes2base64(hashlib.sha1(bytes).digest()[0:6]).replace("/", "_")
        wrap(test).expecting_list.meta.saved_as = expected_hash

        self.utils.send_queries(test)

        # ENSURE THE QUERY HAS BEEN INDEXED
        Log.note("Flush saved query (with hash {{hash}})", hash=expected_hash)
        container = elasticsearch.Index(index="saved_queries", kwargs=settings)
        container.flush(forced=True)
        with Timer("wait for 5 seconds"):
            Till(seconds=5).wait()

        url = URL(self.utils.service_url)
        response = self.utils.try_till_response(url.scheme + "://" + url.host + ":" + text_type(url.port) + "/find/" + expected_hash, data=b'')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.all_content, bytes)

    def test_recovery_of_empty_string(self):

        test = wrap({
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

        bytes = unicode2utf8(value2json(test.query))
        expected_hash = convert.bytes2base64(hashlib.sha1(bytes).digest()[0:6]).replace("/", "_")
        test.expecting_list.meta.saved_as = expected_hash

        test.query.meta = {"save": True}
        self.utils.send_queries(test)

        # ENSURE THE QUERY HAS BEEN INDEXED
        Log.note("Flush saved query")
        container = elasticsearch.Index(index="saved_queries", kwargs=settings)
        container.flush(forced=True)
        with Timer("wait for 5 seconds"):
            Till(seconds=5).wait()

        url = URL(self.utils.service_url)
        response = self.utils.try_till_response(url.scheme + "://" + url.host + ":" + text_type(url.port) + "/find/" + expected_hash, data=b'')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.all_content, bytes)


    # TODO: TEST RECOVERY OF QUERY USING {"prefix": {var: ""}} (EMPTY STRING IS NOT RECORDED RIGHT


