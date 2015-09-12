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

import os

from pyLibrary import convert, jsons
from pyLibrary.debugs.logs import Log, Except
from pyLibrary.dot import wrap
from pyLibrary.env import http
from pyLibrary.thread.multiprocess import Process
from pyLibrary.times.dates import Date, Duration
from pyLibrary.times.durations import DAY
from pyLibrary.times.timer import Timer
from tests.base_test_class import ActiveDataBaseTest, error, settings


APP_CONFIG_FILE = "tests/config/app_staging_settings.json"
ES_CLUSTER_LOCATION = None


class TestUnittests(ActiveDataBaseTest):
    process = None

    @classmethod
    def setUpClass(cls):
        ActiveDataBaseTest.setUpClass(assume_server_started=False)

        # START DIRECT-TO-ACTIVEDATA-ES SERVICE
        global ES_CLUSTER_LOCATION

        app_config = jsons.ref.get("file://"+APP_CONFIG_FILE)
        settings.service_url = "http://localhost:"+unicode(app_config.flask.port)+"/query"
        ES_CLUSTER_LOCATION = app_config.elasticsearch.host

        # TestUnittests.process = Process(
        #     "RUN APP",
        #     [
        #         "python",
        #         "active_data/app.py",
        #         "--settings=" + APP_CONFIG_FILE
        #     ],
        #    cwd=os.getcwd()
        # )
        # while True:
        #     line = TestUnittests.process.recieve.pop()
        #     if line:
        #         break
        ActiveDataBaseTest.server_is_ready.go()


    @classmethod
    def tearDownClass(cls):
        # TestUnittests.process.stop()
        # TestUnittests.process.join()
        pass


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
            response = self._try_till_response(self.service_url, data=query)
            if response.status_code != 200:
                error(response)
        result = convert.json2value(convert.utf82unicode(response.all_content))

        Log.note("result\n{{result|indent}}", {"result": result})

    def test_multiple_agg_on_same_field(self):
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

        query = convert.unicode2utf8(convert.value2json(test.query))
        # EXECUTE QUERY
        with Timer("query"):
            response = http.get(self.service_url, data=query)
            if response.status_code != 200:
                error(response)
        result = convert.json2value(convert.utf82unicode(response.all_content))

        Log.note("result\n{{result|indent}}", {"result": result})

    # TODO: ES WILL NOT ACCEPT THESE TWO (NAIVE) AGGREGATES ON SAME FIELD, COMBINE THEM


    #TODO: IT SEEMS TOO MANY COLUMNS RETURNED, ONLY RETURN SHALLOW COLUMNS?
    #  {"from":"unittest"}

    #TODO: SELECTING OBJECT SHOULD GIVE GOOD ERROR, OR ACTUALLY RETURN OBJECT
    # {
    # 	"from":"unittest",
    # 	"select":"run.files",
    # 	"where":{"gt":{"run.stats.bytes":2000000000}}
    # }

    #TODO: ENSURE NON-EXISTENT SELECT COLUMN IS POINTED OUT IN INEVITABLE ERROR
    # {
    #     "from": "unittest",
    #     "select": "run.result.test"
    # }

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

    def test_big_result_works(self):
        result = http.post_json(settings.service_url, data={
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


    def test_failures_by_directory(self):
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

        query = convert.unicode2utf8(convert.value2json(test.query))
        # EXECUTE QUERY
        with Timer("query"):
            response = http.get(self.service_url, data=query)
            if response.status_code != 200:
                error(response)
        result = convert.json2value(convert.utf82unicode(response.all_content))

        Log.note("result\n{{result|indent}}", {"result": result})



    def test_longest_running_tests(self):
        test = wrap({"query": {
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
        query = convert.unicode2utf8(convert.value2json(test.query))
        # EXECUTE QUERY
        with Timer("query"):
            response = http.get(self.service_url, data=query)
            if response.status_code != 200:
                error(response)
        result = convert.json2value(convert.utf82unicode(response.all_content))

        Log.note("result\n{{result|indent}}", {"result": result})


