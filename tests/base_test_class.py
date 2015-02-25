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

from _subprocess import CREATE_NEW_PROCESS_GROUP
import subprocess
import signal

from active_data.app import replace_vars
from pyLibrary import convert, jsons, queries
from pyLibrary.debugs.logs import Log, Except, constants
from pyLibrary.dot import wrap, listwrap, nvl
from pyLibrary.env import http
from pyLibrary.maths.randoms import Random
from pyLibrary.queries import qb
from pyLibrary.queries.query import _normalize_edges, _normalize_selects
from pyLibrary.strings import expand_template
from pyLibrary.testing import elasticsearch
from pyLibrary.testing.fuzzytestcase import FuzzyTestCase
from pyLibrary.thread.threads import Signal, Thread


settings = jsons.ref.get("file://tests/config/test_settings.json")
constants.set(settings.constants)


class ActiveDataBaseTest(FuzzyTestCase):
    """
    RESPONSIBLE FOR SETTING UP THE RAW CONTAINER, STARTING THE SERVICE,
    AND EXECUTING QUERIES, AND CONFIRMING EXPECTED RESULTS

    BASIC TEST FORMAT:
    {
        "name": "EXAMPLE TEMPLATE",
        "metadata": {},             # OPTIONAL DATA SHAPE REQUIRED FOR NESTED DOCUMENT QUERIES
        "data": [],                  # THE DOCUMENTS NEEDED FOR THIS TEST
        "query": {                   # THE Qb QUERY
            "from": "testdata",      # "testdata" WILL BE REPLACED WITH DATASTORE FILLED WITH data
            "edges": []              # THIS FILE IS EXPECTING EDGES (OR GROUP BY)
        },
        "expecting_list": []         # THE EXPECTATION WHEN "format":"list"
        "expecting_table": {}        # THE EXPECTATION WHEN "format":"table"
        "expecting_cube": {}         # THE EXPECTATION WHEN "format":"cube" (INCLUDING METADATA)
    }

    """

    server_is_ready = None
    please_stop = None
    thread = None
    server = None


    @classmethod
    def setUpClass(cls):
        ActiveDataBaseTest.server_is_ready = Signal()
        ActiveDataBaseTest.please_stop = Signal()
        if settings.startServer:
            ActiveDataBaseTest.thread = Thread("watch server", run_app, please_stop=ActiveDataBaseTest.please_stop, server_is_ready=ActiveDataBaseTest.server_is_ready).start()
            ActiveDataBaseTest.server = http
        else:
            # WE WILL USE THE ActiveServer CODE, AND CONNECT TO ES DIRECTLY.
            # THIS MAKES FOR SLIGHTLY FASTER TEST TIMES BECAUSE THE PROXY IS
            # MISSING
            ActiveDataBaseTest.server = FakeHttp()
            queries.config.default = {
                "type": "elasticsearch",
                "settings": settings.backend_es.copy()
            }
            ActiveDataBaseTest.server_is_ready.go()

        cluster = elasticsearch.Cluster(settings.backend_es)
        aliases = cluster.get_aliases()
        for a in aliases:
            if a.index.startswith("testing_"):
                cluster.delete_index(a.index)

    @classmethod
    def tearDownClass(cls):
        ActiveDataBaseTest.please_stop.go()
        Log.stop()
        if ActiveDataBaseTest.thread:
            ActiveDataBaseTest.thread.stopped.wait_for_go()

    def __init__(self, *args, **kwargs):
        """
        :param service_url:  location opf the ActiveData server we are testing
        :param backend_es:   the ElasticSearch settings for filling the backend
        """
        FuzzyTestCase.__init__(self, *args, **kwargs)
        self.service_url = settings.service_url
        self.backend_es = settings.backend_es.copy()
        self.es = None
        self.index = None

    def setUp(self):
        # ADD TEST RECORDS
        self.backend_es.index = "testing_" + Random.hex(10).lower()
        # self.backend_es.type = "test_results"
        self.es = elasticsearch.Cluster(self.backend_es)
        self.index = self.es.get_or_create_index(self.backend_es)
        self.server_is_ready.wait_for_go()

    def tearDown(self):
        self.es.delete_index(self.backend_es.index)

    def not_real_service(self):
        return not settings.startServer

    def _fill_es(self, subtest):
        settings = self.backend_es.copy()
        settings.index = "testing_" + Random.hex(10).lower()
        # settings.type = "test_results"

        try:
            url = "file://resources/schema/basic_schema.json.template?{{.|url}}"
            url = expand_template(url, {
                "type": settings.type,
                "metadata": subtest.metadata
            })
            settings.schema = jsons.ref.get(url)

            # MAKE CONTAINER
            container = self.es.get_or_create_index(settings)

            # INSERT DATA
            container.extend([
                {"value": v} for v in subtest.data
            ])
            container.flush()
            # ENSURE query POINTS TO CONTAINER
            frum = subtest.query["from"]
            if isinstance(frum, basestring):
                subtest.query["from"] = frum.replace("testdata", settings.index)
            else:
                Log.error("Do not know how to handle")
        except Exception, e:
            Log.error("can not load {{data}} into container", {"data":subtest.data}, e)

        return settings



    def _execute_es_tests(self, subtest):
        subtest = wrap(subtest)

        if subtest.disable:
            return

        if "elasticsearch" in subtest["not"]:
            return

        settings = self._fill_es(subtest)

        try:
            # EXECUTE QUERY
            num_expectations = 0
            for k, v in subtest.items():
                if not k.startswith("expecting_"):
                    continue
                num_expectations += 1
                format = k[len("expecting_"):]
                expected = v

                subtest.query.format = format
                query = convert.unicode2utf8(convert.value2json(subtest.query))
                # EXECUTE QUERY
                response = self._try_till_response(self.service_url, data=query)

                if response.status_code != 200:
                    error(response)
                result = convert.json2value(convert.utf82unicode(response.all_content))

                # HOW TO COMPARE THE OUT-OF-ORDER DATA?
                if format == "table":
                    expected.data = qb.sort(expected.data, range(len(expected.header)))
                    result.data = qb.sort(result.data, range(len(result.header)))
                elif format == "list":
                    sort_order=wrap(_normalize_edges(nvl(subtest.query.edges, subtest.query.groupby)) + _normalize_selects(listwrap(subtest.query.select))).name
                    expected.data = qb.sort(expected.data, sort_order)
                    result.data = qb.sort(result.data, sort_order)

                # CONFIRM MATCH
                self.assertAlmostEqual(result, expected)

            if num_expectations == 0:
                Log.error("Expecting test {{name|quote}} to have property named 'expecting_*' for testing the various format clauses", {
                    "name": subtest.name
                })
        except Exception, e:
            Log.error("Failed test {{name|quote}}", {"name": subtest.name}, e)
        finally:
            # REMOVE CONTAINER
            self.es.delete_index(settings.index)


    def _execute_query(self, query):
        query = wrap(query)

        try:
            query = convert.unicode2utf8(convert.value2json(query))
            # EXECUTE QUERY
            response = self._try_till_response(self.service_url, data=query)

            if response.status_code != 200:
                error(response)
            result = convert.json2value(convert.utf82unicode(response.all_content))

            return result
        except Exception, e:
            Log.error("Failed query", e)


    def _try_till_response(self, *args, **kwargs):
        while True:
            try:
                response = ActiveDataBaseTest.server.get(*args, **kwargs)
                return response
            except Exception, e:
                if "No connection could be made because the target machine actively refused it" in e:
                    Log.alert("Problem connecting")
                else:
                    Log.error("Server raised exception", e)


def error(response):
    response = convert.utf82unicode(response.content)

    try:
        e = Except.new_instance(convert.json2value(response))
    except Exception:
        e = None

    if e:
        Log.error("Failed request", e)
    else:
        Log.error("Failed request\n {{response}}", {"response": response})


def run_app(please_stop, server_is_ready):
    proc = subprocess.Popen(
        ["python", "active_data\\app.py", "--settings", "resources/config/development_settings.json"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=-1,
        creationflags=CREATE_NEW_PROCESS_GROUP
    )

    while not please_stop:
        line = proc.stdout.readline()
        if not line:
            continue
        if line.find(" * Running on") >= 0:
            server_is_ready.go()
        Log.note("SERVER: {{line}}", {"line": line.strip()})

    proc.send_signal(signal.CTRL_C_EVENT)



class FakeHttp(object):

    def get(*args, **kwargs):
        body = kwargs.get("data")

        if not body:
            return wrap({
                "status_code": 400
            })

        text = convert.utf82unicode(body)
        text = replace_vars(text)
        data = convert.json2value(text)
        result = qb.run(data)
        output_bytes = convert.unicode2utf8(convert.value2json(result))
        return wrap({
            "status_code":200,
            "all_content": output_bytes,
            "content": output_bytes
        })
