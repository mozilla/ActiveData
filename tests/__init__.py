# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

import itertools
import os
import signal
from string import ascii_lowercase
import subprocess

from jx_base import container as jx_containers
from jx_base.query import QueryOp
import jx_elasticsearch
from jx_python import jx
from mo_dots import Data, coalesce, is_list, listwrap, literal_field, unwrap, wrap
from mo_files.url import URL
from mo_future import is_text, text_type
from mo_json import json2value, value2json
import mo_json_config
from mo_kwargs import override
from mo_logs import Except, Log, constants
from mo_logs.exceptions import extract_stack
from mo_logs.strings import expand_template, unicode2utf8, utf82unicode
from mo_testing.fuzzytestcase import assertAlmostEqual
from mo_times import Date, MINUTE
from pyLibrary.env import http
from pyLibrary.env.elasticsearch import Cluster
from pyLibrary.testing import elasticsearch
from tests import test_jx

_ = test_jx  # REQUIRED TO SET test_jx.utils

DEFAULT_TEST_CONFIG = "tests/config/test_config.json"


class ESUtils(object):
    """
    RESPONSIBLE FOR SETTING UP THE RAW CONTAINER,
    EXECUTING QUERIES, AND CONFIRMING EXPECTED RESULTS

    BASIC TEST FORMAT:
    {
        "name": "EXAMPLE TEMPLATE",
        "schema": {},                # OPTIONAL ES SCHEMA FOR SPECIAL TESTS
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
        testing,  # location of the ActiveData server endpoints we are testing
        backend_es,  # the ElasticSearch settings for filling the backend
        fast_testing=False,
        kwargs=None
    ):
        if backend_es.schema == None:
            Log.error("Expecting backed_es to have a schema defined")

        letters = text_type(ascii_lowercase)
        self.random_letter = letters[int(Date.now().unix / 30) % 26]
        self.testing = testing
        self.backend_es = backend_es
        self.settings = kwargs
        self._es_test_settings = None
        self._es_cluster = None
        self._index = None

        if not jx_containers.config.default:
            jx_containers.config.default = {
                "type": "elasticsearch",
                "settings": backend_es
            }

        if not fast_testing:
            self.server = http
        else:
            Log.alert("TESTS WILL RUN FAST, BUT NOT ALL TESTS ARE RUN!\nEnsure the `file://tests/config/elasticsearch.json#fastTesting=true` to turn on the network response tests.")
            # WE WILL USE THE ActiveServer CODE, AND CONNECT TO ES DIRECTLY.
            # THIS MAKES FOR SLIGHTLY FASTER TEST TIMES BECAUSE THE PROXY IS
            # MISSING
            self.server = FakeHttp()
            jx_containers.config.default = {
                "type": "elasticsearch",
                "settings": kwargs.backend_es.copy()
            }

    def setUp(self):
        global NEXT

        index_name = "testing_" + ("000" + text_type(NEXT))[-3:] + "_" + self.random_letter
        NEXT += 1

        self._es_test_settings = self.backend_es.copy()
        self._es_test_settings.index = index_name
        self._es_cluster = elasticsearch.Cluster(self._es_test_settings)


    def tearDown(self):
        if self._es_test_settings.index in ESUtils.indexes:
            self._es_cluster.delete_index(self._es_test_settings.index)
            ESUtils.indexes.remove(self._es_test_settings.index)

    def setUpClass(self):
        while True:
            try:
                es = test_jx.global_settings.backend_es
                http.get_json(URL(es.host, port=es.port))
                break
            except Exception as e:
                e = Except.wrap(e)
                if "No connection could be made because the target machine actively refused it" in e or "Connection refused" in e:
                    Log.alert("Problem connecting")
                else:
                    Log.error("Server raised exception", e)

        # REMOVE OLD INDEXES
        cluster = elasticsearch.Cluster(test_jx.global_settings.backend_es)
        aliases = cluster.get_aliases()
        for a in aliases:
            try:
                if a.index.startswith("testing_"):
                    create_time = Date(a.index[-15:], "%Y%m%d_%H%M%S")  # EXAMPLE testing_0ef53e45b320160118_180420
                    if create_time < Date.now() - 10 * MINUTE:
                        cluster.delete_index(a.index)
            except Exception as e:
                Log.warning("Problem removing {{index|quote}}", index=a.index, cause=e)

    def tearDownClass(self):
        cluster = elasticsearch.Cluster(test_jx.global_settings.backend_es)
        for i in ESUtils.indexes:
            try:
                cluster.delete_index(i)
                Log.note("remove index {{index}}", index=i)
            except Exception as e:
                pass
        Log.stop()

    def not_real_service(self):
        return self.settings.fastTesting

    def execute_tests(self, subtest, typed=True, places=6):
        subtest = wrap(subtest)
        subtest.name = text_type(extract_stack()[1]['method'])

        self.fill_container(subtest, typed=typed)
        self.send_queries(subtest, places=places)

    def fill_container(self, subtest, typed=True):
        """
        RETURN SETTINGS THAT CAN BE USED TO POINT TO THE INDEX THAT'S FILLED
        """
        subtest = wrap(subtest)
        _settings = self._es_test_settings  # ALREADY COPIED AT setUp()

        try:
            url = "file://resources/schema/basic_schema.json.template?{{.|url}}"
            url = expand_template(url, {
                "type": _settings.type,
                "metadata": subtest.metadata
            })
            _settings.schema = mo_json_config.get(url)

            # MAKE CONTAINER
            container = self._es_cluster.get_or_create_index(
                typed=typed,
                schema=subtest.schema,
                kwargs=_settings
            )
            container.add_alias(_settings.index)

            _settings.alias = container.settings.alias
            _settings.index = container.settings.index
            ESUtils.indexes.append(_settings.index)

            # INSERT DATA
            container.extend({"value": d} for d in subtest.data)
            container.flush()
            self._es_cluster.get_metadata(force=True)

            # ENSURE query POINTS TO CONTAINER
            frum = subtest.query["from"]
            if frum == None:
                subtest.query["from"] = _settings.index
            elif is_text(frum):
                subtest.query["from"] = frum.replace(test_jx.TEST_TABLE, _settings.index)
            else:
                Log.error("Do not know how to handle")
        except Exception as e:
            Log.error("can not load {{data}} into container", data=subtest.data, cause=e)

        return _settings

    def send_queries(self, subtest, places=6):
        subtest = wrap(subtest)

        try:
            # EXECUTE QUERY
            num_expectations = 0
            for i, (k, v) in enumerate(subtest.items()):
                if k.startswith("expecting_"):  # WHAT FORMAT ARE WE REQUESTING
                    format = k[len("expecting_"):]
                elif k == "expecting":  # NO FORMAT REQUESTED (TO TEST DEFAULT FORMATS)
                    format = None
                else:
                    continue

                num_expectations += 1
                expected = v

                subtest.query.format = format
                subtest.query.meta.testing = (num_expectations == 1)  # MARK FIRST QUERY FOR TESTING SO FULL METADATA IS AVAILABLE BEFORE QUERY EXECUTION
                query = unicode2utf8(value2json(subtest.query))
                # EXECUTE QUERY
                response = self.try_till_response(self.testing.query, data=query)

                if response.status_code != 200:
                    error(response)
                result = json2value(utf82unicode(response.all_content))

                container = jx_elasticsearch.new_instance(self._es_test_settings)
                query = QueryOp.wrap(subtest.query, container, container.namespace)
                compare_to_expected(query, result, expected, places)
                Log.note("PASS {{name|quote}} (format={{format}})", name=subtest.name, format=format)
            if num_expectations == 0:
                Log.error(
                    "Expecting test {{name|quote}} to have property named 'expecting_*' for testing the various format clauses",
                    name=subtest.name
                )
        except Exception as e:
            Log.error("Failed test {{name|quote}}", {"name": subtest.name}, e)

    def execute_query(self, query):
        query = wrap(query)

        try:
            query = unicode2utf8(value2json(query))
            # EXECUTE QUERY
            response = self.try_till_response(self.testing.query, data=query)

            if response.status_code != 200:
                error(response)
            result = json2value(utf82unicode(response.all_content))

            return result
        except Exception as e:
            Log.error("Failed query", e)

    def try_till_response(self, *args, **kwargs):
        while True:
            try:
                response = self.server.get(*args, **kwargs)
                return response
            except Exception as e:
                e = Except.wrap(e)
                if "No connection could be made because the target machine actively refused it" in e or "Connection refused" in e:
                    Log.alert("Problem connecting")
                else:
                    Log.error("Server raised exception", e)

    def post_till_response(self, *args, **kwargs):
        while True:
            try:
                response = self.server.post(*args, **kwargs)
                return response
            except Exception as e:
                e = Except.wrap(e)
                if "No connection could be made because the target machine actively refused it" in e:
                    Log.alert("Problem connecting, retrying")
                else:
                    Log.error("Server raised exception", e)


def compare_to_expected(query, result, expect, places):
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
        if not query.sort:
            try:
                # result.data MAY BE A LIST OF VALUES, NOT OBJECTS
                data_columns = jx.sort(set(jx.get_columns(result.data, leaves=True)) | set(jx.get_columns(expect.data, leaves=True)), "name")
            except Exception:
                data_columns = [{"name": "."}]

            sort_order = listwrap(coalesce(query.edges, query.groupby)) + data_columns

            if is_list(expect.data):
                try:
                    expect.data = jx.sort(expect.data, sort_order.name)
                except Exception as _:
                    pass

            if is_list(result.data):
                try:
                    result.data = jx.sort(result.data, sort_order.name)
                except Exception as _:
                    pass

    elif result.meta.format == "cube" and len(result.edges) == 1 and result.edges[0].name == "rownum" and not query.sort:
        result_data, result_header = cube2list(result.data)
        result_header = map(literal_field, result_header)
        result_data = unwrap(jx.sort(result_data, result_header))
        result.data = list2cube(result_data, result_header)

        expect_data, expect_header = cube2list(expect.data)
        expect_header = map(literal_field, expect_header)
        expect_data = jx.sort(expect_data, expect_header)
        expect.data = list2cube(expect_data, expect_header)

    # CONFIRM MATCH
    assertAlmostEqual(result, expect, places=places)


def cube2list(cube):
    """
    RETURNS header SO THAT THE ORIGINAL CUBE CAN BE RECREATED
    :param cube: A dict WITH VALUES BEING A MULTIDIMENSIONAL ARRAY OF UNIFORM VALUES
    :return: (rows, header) TUPLE
    """
    header = list(unwrap(cube).keys())
    rows = []
    for r in zip(*([(k, unwrap(v)) for v in a] for k, a in cube.items())):
        row = dict(r)
        rows.append(row)
    return rows, header


def list2cube(rows, header):
    output = {h: [] for h in header}
    for r in rows:
        for h in header:
            if h == ".":
                output[h].append(r)
            else:
                output[h].append(r.get(h))
    return output


def sort_table(result):
    """
    SORT ROWS IN TABLE, EVEN IF ELEMENTS ARE JSON
    """
    data = wrap([{text_type(i): v for i, v in enumerate(row) if v != None} for row in result.data])
    sort_columns = jx.sort(set(jx.get_columns(data, leaves=True).name))
    data = jx.sort(data, sort_columns)
    result.data = [tuple(row[text_type(i)] for i in range(len(result.header))) for row in data]


def error(response):
    response = utf82unicode(response.content)

    try:
        e = Except.new_instance(json2value(response))
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
        # creationflags=CREATE_NEW_PROCESS_GROUP
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

        text = utf82unicode(body)
        data = json2value(text)
        result = jx.run(data)
        output_bytes = unicode2utf8(value2json(result))
        return wrap({
            "status_code": 200,
            "all_content": output_bytes,
            "content": output_bytes
        })


container_types = Data(
    elasticsearch=ESUtils,
)


try:
    # read_alternate_settings
    filename = os.environ.get("TEST_CONFIG")
    if filename:
        test_jx.global_settings = mo_json_config.get("file://" + filename)
    else:
        Log.alert("No TEST_CONFIG environment variable to point to config file.  Using " + DEFAULT_TEST_CONFIG)
        test_jx.global_settings = mo_json_config.get("file://" + DEFAULT_TEST_CONFIG)
    constants.set(test_jx.global_settings.constants)
    Log.start(test_jx.global_settings.debug)

    if not test_jx.global_settings.use:
        Log.error('Must have a {"use": type} set in the config file')

    test_jx.global_settings.elasticsearch.version = Cluster(test_jx.global_settings.elasticsearch).version
    test_jx.utils = container_types[test_jx.global_settings.use](test_jx.global_settings)
except Exception as e:
    Log.warning("problem", cause=e)

Log.alert("Resetting test count")
NEXT = 0
