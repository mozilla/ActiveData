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


ES_CLUSTER_LOCATION = "http://52.10.189.133"

class TestUnittests(ActiveDataBaseTest):

    def test_chunk_timing(self):
        if self.not_real_service():
            return

        test = wrap({"query": {
            "from": {
                "type": "elasticsearch",
                "settings": {
                    "host": ES_CLUSTER_LOCATION,
                    "index": "unittest",
                    "type": "test_result"
                }
            },
            "select": {"value": "run.duration", "aggregate": "average"},
            "edges": [
                {"name": "chunk", "value": ["run.suite", "run.chunk"]}
            ],
            "where": {"and": [
                {"term": {"etl.id": 0}},
                {"gte": {"timestamp": Date.floor(Date.now() - (Duration.DAY * 7), Duration.DAY).milli / 1000}}
            ]},
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


#TODO: MAKE TEST: LIMIT NOT WORKING ON EDGES
#{"from":"unittest","edges":["result.test"],"limit":10000}

#TODO: RETURN RESULT FROM SINGLE-VALUE SELECT
# {
# 	"from":"unittest",
# 	"select":"run.stats.bytes",
# 	"where":{"and":[
# 		{"eq":{"machine.platform":"linux64"}},
# 		{"gt":{"run.stats.bytes":600000000}}
# 	]}
# }

#TODO: ES WILL NOT ACCEPT THESE TWO (NAIVE) AGGREGATES ON SAME FIELD, COMBINE THEM
# {
#     "from": "unittest",
#     "select": [
#         {
#             "value": "run.stats.bytes",
#             "aggregate": "max"
#         },
#         {
#             "value": "run.stats.bytes",
#             "aggregate": "count"
#         }
#     ],
#     "groupby": [
#         "machine.platform"
#     ],
#     "where": {
#         "and": [
#             {
#                 "eq": {
#                     "etl.id": 0
#                 }
#             },
#             {
#                 "gt": {
#                     "run.stats.bytes": 600000000
#                 }
#             }
#         ]
#     }
# }

#TODO: IT SEEMS TOO MANY COLUMNS RETURNED
#  {"from":"unittest"}


    def test_timing(self):
        if self.not_real_service():
            return

        test = wrap({"query": {
            "from": {
                "type": "elasticsearch",
                "settings": {
                    "host": ES_CLUSTER_LOCATION,
                    "index": "unittest",
                    "type": "test_result"
                }
            },
            "select": [
                {"name": "count", "value": "run.duration", "aggregate": "count"},
                {"name": "total", "value": "run.duration", "aggregate": "sum"}
            ],
            "edges": [
                {"name": "chunk", "value": ["run.suite", "run.chunk"]},
                "result.ok"
            ],
            "where": {"and": [
                {"lt": {"timestamp": Date.floor(Date.now()).milli / 1000}},
                {"gte": {"timestamp": Date.floor(Date.now() - (Duration.DAY * 7), Duration.DAY).milli / 1000}}
            ]},
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


    def test_branch_count(self):
        if self.not_real_service():
            return

        test = wrap({"query": {
            "from": {
                "type": "elasticsearch",
                "settings": {
                    "host": ES_CLUSTER_LOCATION,
                    "index": "unittest",
                    "type": "test_result"
                }
            },
            "select": [
                {"aggregate": "count"},
            ],
            "edges": [
                "build.branch"
            ],
            "where": {"or": [
                {"missing": "build.id"}
                # {"gte": {"timestamp": Date.floor(Date.now() - (Duration.DAY * 7), Duration.DAY).milli / 1000}}
            ]},
            "format": "table"
        }})

        query = convert.unicode2utf8(convert.value2json(test.query))
        # EXECUTE QUERY
        with Timer("query"):
            response = http.get(self.service_url, data=query)
            if response.status_code != 200:
                error(response)
        result = convert.json2value(convert.utf82unicode(response.all_content))

        Log.note("result\n{{result|indent}}", {"result": result})

