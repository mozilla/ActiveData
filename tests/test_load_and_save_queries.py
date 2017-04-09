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

from mo_dots import wrap
from mo_json import value2json
from mo_json_config import URL
from mo_threads import Till
from pyLibrary import convert
from pyLibrary.convert import unicode2utf8
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
        container = elasticsearch.Index(index="saved_queries", kwargs=settings)
        container.flush()
        Till(seconds=5).wait()

        url = URL(self.utils.service_url)

        response = self.utils.try_till_response(url.scheme+"://"+url.host+":"+unicode(url.port)+"/find/"+expected_hash, data=b'')

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.all_content, bytes)







