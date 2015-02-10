from _subprocess import CREATE_NEW_PROCESS_GROUP
import subprocess
import signal

from pyLibrary import convert
from pyLibrary import jsons
from pyLibrary.debugs.logs import Log, Except, constants
from pyLibrary.env import http
from pyLibrary.maths.randoms import Random
from pyLibrary.testing import elasticsearch
from pyLibrary.testing.fuzzytestcase import FuzzyTestCase
from pyLibrary.thread.threads import Signal, Thread
from tests import testdata_set_ops, testdata_2_edge, testdata_1_edge, parametrize


all_subtests = []
all_subtests.extend(testdata_set_ops.tests)
all_subtests.extend(testdata_1_edge.tests)
all_subtests.extend(testdata_2_edge.tests)

settings = jsons.ref.get("file://tests/config/test_settings.json")
constants.set(settings.constants)


class TestSimpleRequests(FuzzyTestCase):
    server_is_ready = None
    please_stop = None
    thread = None


    @classmethod
    def setUpClass(cls):
        cls.server_is_ready = Signal()
        cls.please_stop = Signal()
        if settings.startServer:
            cls.thread = Thread("watch server", run_app, please_stop=cls.please_stop, server_is_ready=cls.server_is_ready).start()
        else:
            cls.server_is_ready.go()

        cluster = elasticsearch.Cluster(settings.backend_es)
        aliases = cluster.get_aliases()
        for a in aliases:
            if a.index.startswith("testing_"):
                cluster.delete_index(a.index)


    @classmethod
    def tearDownClass(cls):
        cls.please_stop.go()
        if cls.thread:
            cls.thread.stopped.wait_for_go()


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
        self.backend_es.type = "testdata"
        self.es = elasticsearch.Cluster(self.backend_es)
        self.index = self.es.get_or_create_index(self.backend_es)
        self.server_is_ready.wait_for_go()

    def tearDown(self):
        self.es.delete_index(self.backend_es.index)


    @parametrize("subtest", all_subtests)
    def test_queries(self, subtest):
        if subtest.name == "EXAMPLE TEMPLATE":
            return

        settings = self.backend_es.copy()
        settings.index = "testing_" + Random.hex(10).lower()
        settings.type = "testdata"
        settings.schema = subtest.metadata

        try:
            # MAKE CONTAINER
            container = self.es.get_or_create_index(settings)
            # INSERT DATA
            container.extend([
                {"value": v} for v in subtest.data
            ])
            container.flush()
            # ENSURE query POINTS TO CONTAINER
            subtest.query["from"] = settings.index
        except Exception, e:
            Log.warning("can not load {{data}} into container", {"data":subtest.data})
            return

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
                response = http.get(self.service_url, data=query)
                if response.status_code != 200:
                    error(response)
                result = convert.json2value(convert.utf82unicode(response.content))

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


def error(response):
    response = convert.utf82unicode(response.content)
    e = None
    try:
        e = Except.new_instance(convert.json2value(response))
    except Exception:
        pass
    if e:
        Log.error("Failed request", e)
    else:
        Log.error("Failed request\n {{response}}", {"response": response})


def run_app(please_stop, server_is_ready):
    proc = subprocess.Popen(
        ["python", "active_data\\app.py", "--settings", "resources/config/settings.json"],
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



