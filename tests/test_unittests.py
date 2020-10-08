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

import os
from unittest import skip

import mo_json_config
from mo_dots import dict_to_data
from mo_future import text
from mo_http import http
from mo_json import json2value, value2json
from mo_logs import Except, Log
from mo_times.dates import Date, Duration
from mo_times.durations import DAY
from mo_times.timer import Timer
from tests import error
from tests.test_jx import BaseTestCase, global_settings

APP_CONFIG_FILE = os.environ.get("TEST_CONFIG") or "tests/config/app_config.json"
ES_CLUSTER_LOCATION = None


@skip("not usually run")
class TestUnittests(BaseTestCase):
    process = None

    @classmethod
    def setUpClass(cls):
        BaseTestCase.setUpClass(assume_server_started=False)

        # START DIRECT-TO-ACTIVEDATA-ES SERVICE
        global ES_CLUSTER_LOCATION

        app_config = mo_json_config.get("file://" + APP_CONFIG_FILE)
        global_settings.testing.query = "http://localhost:"+text(app_config.flask.port)+"/query"
        ES_CLUSTER_LOCATION = app_config.elasticsearch.host

    @classmethod
    def tearDownClass(cls):
        # TestUnittests.process.stop()
        # TestUnittests.process.join()
        pass

    def test_simple_query(self):
        if self.not_real_service():
            return

        query = value2json({"from": "unittest"}).encode('utf8')
        # EXECUTE QUERY
        with Timer("query"):
            response = http.get(self.testing.query, data=query)
            if response.status_code != 200:
                error(response)
        result = json2value(response.all_content.decode('utf8'))

        Log.note("result\n{{result|indent}}", {"result": result})

    def test_chunk_timing(self):
        if self.not_real_service():
            return

        test = dict_to_data({"query": {
            "from": {
                "type": "elasticsearch",
                "settings": {
                    "host": ES_CLUSTER_LOCATION,
                    "index": "unittest",
                    "type": "test_result"
                }
            },
            "select": {"value": "run.stats.duration", "aggregate": "average"},
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

        query = value2json(test.query).encode('utf8')
        # EXECUTE QUERY
        with Timer("query"):
            response = self.utils.try_till_response(self.testing.query, data=query)
            if response.status_code != 200:
                error(response)
        result = json2value(response.all_content.decode('utf8'))

        Log.note("result\n{{result|indent}}", {"result": result})

    def test_multiple_agg_on_same_field(self):
        if self.not_real_service():
            return

        test = dict_to_data({"query": {
            "from": {
                "type": "elasticsearch",
                "settings": {
                    "host": ES_CLUSTER_LOCATION,
                    "index": "unittest",
                    "type": "test_result"
                }
            },
            "select": [
                {
                    "name": "max_bytes",
                    "value": "run.stats.bytes",
                    "aggregate": "max"
                },
                {
                    "name": "count",
                    "value": "run.stats.bytes",
                    "aggregate": "count"
                }
            ]
        }})

        query = value2json(test.query).encode('utf8')
        # EXECUTE QUERY
        with Timer("query"):
            response = http.get(self.testing.query, data=query)
            if response.status_code != 200:
                error(response)
        result = json2value(response.all_content.decode('utf8'))

        Log.note("result\n{{result|indent}}", {"result": result})

    def test_timing(self):
        if self.not_real_service():
            return

        test = dict_to_data({"query": {
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

        query = value2json(test.query).encode('utf8')
        # EXECUTE QUERY
        with Timer("query"):
            response = http.get(self.testing.query, data=query)
            if response.status_code != 200:
                error(response)
        result = json2value(response.all_content.decode('utf8'))

        Log.note("result\n{{result|indent}}", {"result": result})

    def test_big_result_works(self):
        result = http.post_json(global_settings.testing.query, data={
            "from": "unittest",
            "where": {"and": [
                {"gte": {"run.timestamp": Date.today() - DAY}},
                {"lt": {"run.timestamp": Date.today()}},
                {"eq": {"result.ok": False}}
            ]},
            "format": "list",
            "limit": 10000
        })
        if result.template:
            result = Except.new_instance(result)
            Log.error("problem with call", cause=result)
        Log.note("Got {{num}} test failures", num=len(result.data))

    def test_branch_count(self):
        if self.not_real_service():
            return

        test = dict_to_data({"query": {
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

        query = value2json(test.query).encode('utf8')
        # EXECUTE QUERY
        with Timer("query"):
            response = http.get(self.testing.query, data=query)
            if response.status_code != 200:
                error(response)
        result = json2value(response.all_content.decode('utf8'))

        Log.note("result\n{{result|indent}}", {"result": result})


    def test_failures_by_directory(self):
        if self.not_real_service():
            return

        test = dict_to_data({"query": {
            "from": {
                "type": "elasticsearch",
                "settings": {
                    "host": ES_CLUSTER_LOCATION,
                    "index": "unittest",
                    "type": "test_result"
                }
            },
            "select": [
                {
                    "aggregate": "count"
                }
            ],
            "edges": [
                "result.test",
                "result.ok"
            ],
            "where": {
                "prefix": {
                    "result.test": "/"
                }
            },
            "format": "table"
        }})

        query = value2json(test.query).encode('utf8')
        # EXECUTE QUERY
        with Timer("query"):
            response = http.get(self.testing.query, data=query)
            if response.status_code != 200:
                error(response)
        result = json2value(response.all_content.decode('utf8'))

        Log.note("result\n{{result|indent}}", {"result": result})



    def test_longest_running_tests(self):
        test = dict_to_data({"query": {
            "sort": {"sort": -1, "field": "avg"},
            "from": {
                "from": "unittest",
                "where": {"and": [{"gt": {"build.date": "1439337600"}}]},
                "groupby": ["build.platform", "build.type", "run.suite", "result.test"],
                "select": [{"aggregate": "avg", "name": "avg", "value": "result.duration"}],
                "format": "table",
                "limit": 100
            },
            "limit": 100,
            "format": "list"
        }})
        query = value2json(test.query).encode('utf8')
        # EXECUTE QUERY
        with Timer("query"):
            response = http.get(self.testing.query, data=query)
            if response.status_code != 200:
                error(response)
        result = json2value(response.all_content.decode('utf8'))

        Log.note("result\n{{result|indent}}", {"result": result})


