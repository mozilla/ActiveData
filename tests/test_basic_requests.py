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

from unittest import skipIf

from active_data.app import OVERVIEW
from mo_json_config import URL
from pyLibrary import convert
from tests.base_test_class import ActiveDataBaseTest, global_settings


class TestBasicRequests(ActiveDataBaseTest):

    @skipIf(global_settings.use=="sqlite", "not expected to pass yet")
    def test_empty_request(self):
        response = self.utils.try_till_response(self.utils.service_url, data=b"")
        self.assertEqual(response.status_code, 400)

    @skipIf(global_settings.use=="sqlite", "not expected to pass yet")
    def test_root_request(self):
        if self.utils.not_real_service():
            return

        url = URL(self.utils.service_url)
        url.path = ""
        url = str(url)
        response = self.utils.try_till_response(url, data=b"")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.all_content, OVERVIEW)

    @skipIf(global_settings.use=="sqlite", "not expected to pass yet")
    def test_bad_file_request(self):
        url = URL(self.utils.service_url)
        url.path = "/tools/../../README.md"

        response = self.utils.try_till_response(str(url), data=b"")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.all_content, "")

    @skipIf(global_settings.use=="sqlite", "not expected to pass yet")
    def test_query_on_static_file(self):
        url = URL(self.utils.service_url)
        url.path = "/tools/index.html?123"

        response = self.utils.try_till_response(str(url), data=b"")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.all_content, OVERVIEW)

    @skipIf(global_settings.use=="sqlite", "not expected to pass yet")
    def test_rest_get(self):
        settings = self.utils.fill_container({
            "data":[
                {"a": 0, "b": 0},
                {"a": 0, "b": 1},
                {"a": 1, "b": 0},
                {"a": 1, "b": 1}
            ],
            "query": {"from": ""}  # DUMMY LINE
        })

        url = URL(self.utils.service_url)
        url.path = "json/" + settings.index
        url.query = {"a": 1}

        response = self.utils.try_till_response(str(url), data=b"")
        self.assertEqual(response.status_code, 200)

        # ORDER DOES NOT MATTER, TEST EITHER
        expected1 = convert.unicode2utf8(convert.value2json([{"a": 1, "b": 0}, {"a": 1, "b": 1}], pretty=True))
        expected2 = convert.unicode2utf8(convert.value2json([{"a": 1, "b": 1}, {"a": 1, "b": 0}], pretty=True))

        try:
            self.assertEqual(response.all_content, expected1)
        except Exception:
            self.assertEqual(response.all_content, expected2)




