from pyLibrary import strings, convert
from pyLibrary.debugs import constants
from pyLibrary.debugs.logs import Log, Except
from pyLibrary.dot import wrap, Dict, coalesce, unwraplist
from pyLibrary.env import http
from pyLibrary.queries import qb
from pyLibrary.queries.index import Index
from pyLibrary.queries.qb_usingES import FromES
from pyLibrary.queries.unique_index import UniqueIndex
from pyLibrary.strings import edit_distance
from pyLibrary.times.dates import Date

# DATE RANGE
from pyLibrary.times.durations import HOUR
from pyLibrary.times.timer import Timer

FROM_DATE = Date("today-2day")
TO_DATE = Date("tomorrow")

config = wrap({
    "ActiveData":{
        # "url": "http://activedata.allizom.org/query",
        "url": "http://localhost:5000/query"
    },
    "Bugzilla":{
        "url": "http://elasticsearch-zlb.bugs.scl3.mozilla.com:9200/public_bugs",
        "host": "http://elasticsearch-zlb.bugs.scl3.mozilla.com",
        "port": 9200,
        "index": "public_bugs",
        "type": "bug_version",
        "debug": False
    }
})

_set = Dict()
_set.pyLibrary.env.http.default_headers = {"From": "https://wiki.mozilla.org/Auto-tools/Projects/ActiveData"}
_set.pyLibrary.env.big_data.MAX_STRING_SIZE = 100 * 1000 * 1000
constants.set(_set)


#WAIT FOR SERVICE TO BE ACTIVE
with Timer("wait for service to respond"):
    while True:
        try:
            result = http.post_json(config.ActiveData.url, data={
                "from": "unittest",
                "limit": 1
            })
            break
        except Exception, e:
            if "No connection could be made because the target machine actively refused it" in e:
                Log.note("wait for AD to respond")
            else:
                Log.warning("problem", e)


# SIMPLE LIST OF ALL TEST FAILURES
with Timer("get failures"):
    result = http.post_json(config.ActiveData.url, data={
        "from": "unittest.result.subtests",
        "select": [
            "_id",
            {"name": "subtest_name", "value": "name"},
            "message",
            {"name": "suite", "value": "run.suite"},
            {"name": "chunk", "value": "run.chunk"},
            {"name": "test", "value": "result.test"},
            {"name": "build_date", "value": "build_date"},
            {"name": "branch", "value": "build.branch"},
            {"name": "revision", "value": "build.revision12"}
        ],
        "where": {"and": [
            {"gte": {"run.timestamp": FROM_DATE}},
            {"lt": {"run.timestamp": TO_DATE}},
            {"eq": {"result.ok": False}},
            {"eq": {"ok": False}},
            {"eq": {"build.branch": "mozilla-inbound"}}
        ]},
        "limit": 100,
        "format": "list"
    })

if result.type == "ERROR":
    Log.error("problem", cause=Except.wrap(result))

raw_failures = result.data
Log.note("got {{num}} errors", num=len(raw_failures))

if not raw_failures:
    exit()

#GROUP TESTS, AND COUNT
failures = Index(keys=["suite", "test"], data=raw_failures)


#FIND ALL THE SUCCESSES OF THE SAME
with Timer("get success"):
    success = http.post_json(config.ActiveData.url, zip=False, data={
        "from": "unittest",
        "select": [
            {"aggregate": "count"}
        ],
        "edges": [
            {"name": "test", "value": ["run.suite", "result.test"]},
            "result.ok",
            {
                "name": "build.date",
                "value": "build.date",
                "domain": {"type": "time", "min": FROM_DATE, "max": TO_DATE, "interval": HOUR}
            }
        ],
        "where": {"and": [
            {"gte": {"build.date": FROM_DATE}},
            {"lt": {"build.date": TO_DATE}},
            {"eq": {"build.branch": "mozilla-inbound"}},
            {"or": [
                {"eq": {"run.suite": g.suite, "result.test": g.test}}
                for g, d in qb.groupby(raw_failures, ("suite", "test"))
            ]}
        ]},
        "limit": 100000,
        "format": "cube"
    })

if success.type == "ERROR":
    Log.error("problem", cause=Except.wrap(success))

