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

import itertools
import os
import signal
import subprocess
from string import ascii_lowercase

import mo_json_config
from active_data.actions.query import replace_vars
from mo_dots import wrap, coalesce, unwrap, listwrap, Data
from mo_kwargs import override
from mo_logs import Log, Except, constants
from mo_logs.exceptions import extract_stack
from mo_logs.strings import expand_template
from mo_testing.fuzzytestcase import assertAlmostEqual
from mo_times.dates import Date
from mo_times.durations import MINUTE
from pyLibrary import convert
from pyLibrary.env import http
from pyLibrary.queries import jx, containers
from pyLibrary.queries.query import QueryOp
from pyLibrary.testing import elasticsearch
from test_jx import TEST_TABLE


class ESUtils(object):
    """
    RESPONSIBLE FOR SETTING UP THE RAW CONTAINER,
    EXECUTING QUERIES, AND CONFIRMING EXPECTED RESULTS

    BASIC TEST FORMAT:
    {
        "name": "EXAMPLE TEMPLATE",
        "metadata": {},             # OPTIONAL DATA SHAPE REQUIRED FOR NESTED DOCUMENT QUERIES
        "data": [],                  # THE DOCUMENTS NEEDED FOR THIS TEST
        "query": {                   # THE JSON QUERY EXPRESSION
            "from": TEST_TABLE,      # TEST_TABLE WILL BE REPLACED WITH DATASTORE FILLED WITH data
            "edges": []              # THIS FILE IS EXPECTING EDGES (OR GROUP BY)
        },
        "expecting_list": []         # THE EXPECTATION WHEN "format":"list"
        "expecting_table": {}        # THE EXPECTATION WHEN "format":"table"
        "expecting_cube": {}         # THE EXPECTATION WHEN "format":"cube" (INCLUDING METADATA)
    }
    """
    indexes = []

    @override
    def __init__(
        self,
        service_url,  # location of the ActiveData server we are testing
        backend_es,  # the ElasticSearch settings for filling the backend
        fastTesting=False,
        kwargs=None
    ):
        if backend_es.schema==None:
            Log.error("Expecting backed_es to have a schema defined")

        letters = unicode(ascii_lowercase)
        self.random_letter = letters[int(Date.now().unix / 30) % 26]
        self.service_url = service_url
        self.backend_es = backend_es
        self.settings = kwargs
        self._es_test_settings = None
        self._es_cluster = None
        self._index = None

        if not containers.config.default:
            containers.config.default = {
                "type": "elasticsearch",
                "settings": backend_es
            }

        if not fastTesting:
            self.server = http
        else:
            Log.alert("TESTS WILL RUN FAST, BUT NOT ALL TESTS ARE RUN!\nEnsure the `file://tests/config/elasticsearch.json#fastTesting=true` to turn on the network response tests.")
            # WE WILL USE THE ActiveServer CODE, AND CONNECT TO ES DIRECTLY.
            # THIS MAKES FOR SLIGHTLY FASTER TEST TIMES BECAUSE THE PROXY IS
            # MISSING
            self.server = FakeHttp()
            containers.config.default = {
                "type": "elasticsearch",
                "settings": kwargs.backend_es.copy()
            }

    def setUp(self):
        global NEXT

        index_name = "testing_" + ("000"+unicode(NEXT))[-3:] + "_" + self.random_letter
        NEXT += 1

        self._es_test_settings = self.backend_es.copy()
        self._es_test_settings.index = index_name
        self._es_test_settings.alias = None
        self._es_cluster = elasticsearch.Cluster(self._es_test_settings)
        self._index = self._es_cluster.get_or_create_index(self._es_test_settings)

        ESUtils.indexes.append(self._index)

    def tearDown(self):
        if self._index in ESUtils.indexes:
            self._es_cluster.delete_index(self._index.settings.index)
            ESUtils.indexes.remove(self._index)

    def setUpClass(self):
        # REMOVE OLD INDEXES
        cluster = elasticsearch.Cluster(test_jx.global_settings.backend_es)
        aliases = cluster.get_aliases()
        for a in aliases:
            try:
                if a.index.startswith("testing_"):
                    create_time = Date(a.index[-15:], "%Y%m%d_%H%M%S")  # EXAMPLE testing_0ef53e45b320160118_180420
                    if create_time < Date.now() - 10 * MINUTE:
                        cluster.delete_index(a.index)
            except Exception, e:
                Log.warning("Problem removing {{index|quote}}", index=a.index, cause=e)

    def tearDownClass(self):
        cluster = elasticsearch.Cluster(test_jx.global_settings.backend_es)
        for i in ESUtils.indexes:
            try:
                cluster.delete_index(i.settings.index)
                Log.note("remove index {{index}}", index=i)
            except Exception, e:
                pass
        Log.stop()

    def not_real_service(self):
        return self.settings.fastTesting

    def execute_es_tests(self, subtest, tjson=False):
        subtest = wrap(subtest)
        subtest.name = extract_stack()[1]['method']

        self.fill_container(subtest, tjson=tjson)
        self.send_queries(subtest)

    def fill_container(self, subtest, tjson=False):
        """
        RETURN SETTINGS THAT CAN BE USED TO POINT TO THE INDEX THAT'S FILLED
        """
        subtest = wrap(subtest)
        _settings = self._es_test_settings  # ALREADY COPIED AT setUp()
        # _settings.index = "testing_" + Random.hex(10).lower()
        # settings.type = "test_result"

        try:
            url = "file://resources/schema/basic_schema.json.template?{{.|url}}"
            url = expand_template(url, {
                "type": _settings.type,
                "metadata": subtest.metadata
            })
            _settings.schema = mo_json_config.get(url)

            # MAKE CONTAINER
            container = self._es_cluster.get_or_create_index(tjson=tjson, kwargs=_settings)
            container.add_alias(_settings.index)

            # INSERT DATA
            container.extend([
                {"value": v} for v in subtest.data
            ])
            container.flush()
            # ENSURE query POINTS TO CONTAINER
            frum = subtest.query["from"]
            if isinstance(frum, basestring):
                subtest.query["from"] = frum.replace(TEST_TABLE, _settings.index)
            else:
                Log.error("Do not know how to handle")
        except Exception, e:
            Log.error("can not load {{data}} into container", {"data":subtest.data}, e)

        return _settings

    def send_queries(self, subtest):
        subtest = wrap(subtest)

        try:
            # EXECUTE QUERY
            num_expectations = 0
            for k, v in subtest.items():
                if k.startswith("expecting_"):  # WHAT FORMAT ARE WE REQUESTING
                    format = k[len("expecting_"):]
                elif k == "expecting":  # NO FORMAT REQUESTED (TO TEST DEFAULT FORMATS)
                    format = None
                else:
                    continue

                num_expectations += 1
                expected = v

                subtest.query.format = format
                subtest.query.meta.testing = True  # MARK ALL QUERIES FOR TESTING SO FULL METADATA IS AVAILABLE BEFORE QUERY EXECUTION
                query = convert.unicode2utf8(convert.value2json(subtest.query))
                # EXECUTE QUERY
                response = self.try_till_response(self.service_url, data=query)

                if response.status_code != 200:
                    error(response)
                result = convert.json2value(convert.utf82unicode(response.all_content))

                # HOW TO COMPARE THE OUT-OF-ORDER DATA?
                compare_to_expected(subtest.query, result, expected)
                Log.note("Test result compares well")
            if num_expectations == 0:
                Log.error("Expecting test {{name|quote}} to have property named 'expecting_*' for testing the various format clauses", {
                    "name": subtest.name
                })
        except Exception, e:
            Log.error("Failed test {{name|quote}}", {"name": subtest.name}, e)

    def execute_query(self, query):
        query = wrap(query)

        try:
            query = convert.unicode2utf8(convert.value2json(query))
            # EXECUTE QUERY
            response = self.try_till_response(self.service_url, data=query)

            if response.status_code != 200:
                error(response)
            result = convert.json2value(convert.utf82unicode(response.all_content))

            return result
        except Exception, e:
            Log.error("Failed query", e)

    def try_till_response(self, *args, **kwargs):
        while True:
            try:
                response = self.server.get(*args, **kwargs)
                return response
            except Exception, e:
                e = Except.wrap(e)
                if "No connection could be made because the target machine actively refused it" in e:
                    Log.alert("Problem connecting")
                else:
                    Log.error("Server raised exception", e)


def compare_to_expected(query, result, expect):
    query = wrap(query)
    expect = wrap(expect)

    if result.meta.format == "table":
        assertAlmostEqual(set(result.header), set(expect.header))

        # MAP FROM expected COLUMN TO result COLUMN
        mapping = zip(*zip(*filter(
            lambda v: v[0][1] == v[1][1],
            itertools.product(enumerate(expect.header), enumerate(result.header))
        ))[1])[0]
        result.header = [result.header[m] for m in mapping]

        if result.data:
            columns = zip(*unwrap(result.data))
            result.data = zip(*[columns[m] for m in mapping])

        if not query.sort:
            sort_table(result)
            sort_table(expect)
    elif result.meta.format == "list":
        if query["from"].startswith("meta."):
            pass
        else:
            query = QueryOp.wrap(query)

        if not query.sort:
            try:
                #result.data MAY BE A LIST OF VALUES, NOT OBJECTS
                data_columns = jx.sort(set(jx.get_columns(result.data, leaves=True)) | set(jx.get_columns(expect.data, leaves=True)), "name")
            except Exception:
                data_columns = [{"name":"."}]

            sort_order = listwrap(coalesce(query.edges, query.groupby)) + data_columns

            if isinstance(expect.data, list):
                try:
                    expect.data = jx.sort(expect.data, sort_order.name)
                except Exception, _:
                    pass

            if isinstance(result.data, list):
                try:
                    result.data = jx.sort(result.data, sort_order.name)
                except Exception, _:
                    pass

    elif result.meta.format == "cube" and len(result.edges) == 1 and result.edges[0].name == "rownum" and not query.sort:
        result_data, result_header = cube2list(result.data)
        result_data = unwrap(jx.sort(result_data, result_header))
        result.data = list2cube(result_data, result_header)

        expect_data, expect_header = cube2list(expect.data)
        expect_data = jx.sort(expect_data, expect_header)
        expect.data = list2cube(expect_data, expect_header)

    # CONFIRM MATCH
    assertAlmostEqual(result, expect, places=6)


def cube2list(cube):
    """
    RETURNS header SO THAT THE ORIGINAL CUBE CAN BE RECREATED
    :param cube: A dict WITH VALUES BEING A MULTIDIMENSIONAL ARRAY OF UNIFORM VALUES
    :return: (rows, header) TUPLE
    """
    header = list(unwrap(cube).keys())
    rows = []
    for r in zip(*[[(k, v) for v in a] for k, a in cube.items()]):
        row = Data()
        for k, v in r:
           row[k]=v
        rows.append(unwrap(row))
    return rows, header


def list2cube(rows, header):
    output = {h: [] for h in header}
    for r in rows:
        for h in header:
            if h==".":
                output[h].append(r)
            else:
                r = wrap(r)
                output[h].append(r[h])
    return output


def sort_table(result):
    """
    SORT ROWS IN TABLE, EVEN IF ELEMENTS ARE JSON
    """
    data = wrap([{unicode(i): v for i, v in enumerate(row)} for row in result.data])
    sort_columns = jx.sort(set(jx.get_columns(data, leaves=True).name))
    data = jx.sort(data, sort_columns)
    result.data = [tuple(row[unicode(i)] for i in range(len(result.header))) for row in data]


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
        ["python", "active_data\\app.py", "--settings", "tests/config/elasticsearch.json"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=-1
        #creationflags=CREATE_NEW_PROCESS_GROUP
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
        result = jx.run(data)
        output_bytes = convert.unicode2utf8(convert.value2json(result))
        return wrap({
            "status_code": 200,
            "all_content": output_bytes,
            "content": output_bytes
        })


test_jx.global_settings = mo_json_config.get("file://tests/config/elasticsearch.json")
constants.set(test_jx.global_settings.constants)
Log.alert("Resetting test count")
NEXT = 0

container_types = Data(
    elasticsearch=ESUtils,
)


try:
    # read_alternate_settings
    filename = os.environ.get("TEST_CONFIG")
    if filename:
        test_jx.global_settings = mo_json_config.get("file://"+filename)
    else:
        Log.alert("No TEST_CONFIG environment variable to point to config file.  Using /tests/config/elasticsearch.json")

    Log.start(test_jx.global_settings.debug)

    if not test_jx.global_settings.use:
        Log.error('Must have a {"use": type} set in the config file')
    test_jx.utils = container_types[test_jx.global_settings.use](test_jx.global_settings)
except Exception, e:
    Log.warning("problem", e)

