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
from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap
from pyLibrary.env import http
from pyLibrary.times.dates import Date, Duration
from pyLibrary.times.timer import Timer

from tests.base_test_class import ActiveDataBaseTest, error


class TestUnittests(ActiveDataBaseTest):
    def test_chunk_timing(self):
        test = wrap({"query": {
            "from": {
                "type": "elasticsearch",
                "settings": {
                    "host": "http://54.148.242.195",
                    "index": "unittest",
                    "type": "test_results"
                }
            },
            "select": {"value": "result.duration", "aggregate": "average"},
            "edges": [
                {"name":"chunk", "value": ["run.suite", "run.chunk"]},
                "result.ok"
            ],
            "where": {
                "gte": {"timestamp": (Date.now()-(Duration.DAY*7)).milli}
            },
            "format": "cube",
            "samples": {
                "limit": 30
            }
        }})

        query = convert.unicode2utf8(convert.value2json(test.query))
        # EXECUTE QUERY
        with Timer("query"):
            response = http.get(self.service_url, data=query)
            if response.status_code != 200:
                error(response)
        result = convert.json2value(convert.utf82unicode(response.all_content))

        Log.note("result\n{{result|indent}}", {"result": result})

