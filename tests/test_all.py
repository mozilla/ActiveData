from _subprocess import CREATE_NEW_PROCESS_GROUP
import subprocess
import signal

import pytest

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.env import http
from pyLibrary.maths.randoms import Random
from pyLibrary.testing import elasticsearch
from pyLibrary.testing.fuzzytestcase import FuzzyTestCase
from pyLibrary.thread.threads import Signal, Thread
from tests import testdata_set_ops, testdata_2_edge, testdata_1_edge


all_subtests = []
all_subtests.extend(testdata_set_ops.tests)
all_subtests.extend(testdata_1_edge.tests)
all_subtests.extend(testdata_2_edge.tests)


class TestSimpleRequests(FuzzyTestCase):

    server_is_ready = None
    please_stop = None
    thread = None


    @classmethod
    def setUpClass(cls):
        cls.server_is_ready = Signal()
        cls.please_stop = Signal()
        cls.thread = Thread("watch server", run_app, please_stop=cls.please_stop, server_is_ready=cls.server_is_ready).start()

    @classmethod
    def tearDownClass(cls):
        cls.please_stop.go()
        cls.thread.stopped.wait_for_go()


    def __init__(self, service_url, backend_es):
        """
        :param service_url:  location opf the ActiveData server we are testing
        :param backend_es:   the ElasticSearch settings for filling the backend
        """
        FuzzyTestCase.__init__(self)
        self.service_url = service_url
        self.backend_es = backend_es.copy()
        self.es = None
        self.index = None

    @pytest.mark.parametrize("subtest", all_subtests)
    def startup(self):
        # ADD TEST RECORDS
        self.backend_es.index = Random.hex(10)
        self.es = elasticsearch.Cluster(self.backend_es)
        self.index = self.es.get_or_create_index(self.backend_es)
        self.server_is_ready.wait_for_go()

    def tearDown(self):
        self.es.delete_index(self.backend_es.index)


    def test_queries(self, subtest):
        settings = self.backend_es.copy()
        settings.index = "testing" + Random.hex(10)
        settings.schema = subtest.metadata
        try:
            # MAKE CONTAINER
            with self.es.get_or_create_index(settings) as container:
                # INSERT DATA
                container.extend([
                    {"value": v} for v in subtest.data
                ])

                # EXECUTE QUERY
                query = convert.unicode2utf8(convert.value2json(subtest.query))
                response = http.get(self.service_url, body=query)
                result = convert.json2value(convert.utf82unicode(response.content))

                # CONFIRM MATCH
                self.assertAlmostEqual(result, subtest.expected)
        except Exception, e:
            Log.error("Failed test", e)
        finally:
            # REMOVE CONTAINER
            self.es.delete_index(settings.index)


def run_app(self, please_stop, server_is_ready):
    proc = subprocess.Popen(
        ["python", "active_data\\app.py", "--settings", "resources/config/test_settings.json"],
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
