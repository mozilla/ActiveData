# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from copy import copy

from pyLibrary import convert
from pyLibrary.env import elasticsearch
from pyLibrary.env.elasticsearch import ES_NUMERIC_TYPES
from pyLibrary.meta import use_settings
from pyLibrary.queries import qb
from pyLibrary.queries.containers import Container
from pyLibrary.queries.query import Query
from pyLibrary.debugs.logs import Log
from pyLibrary.dot.dicts import Dict
from pyLibrary.dot import coalesce, set_default, Null, literal_field, listwrap, split_field, join_field
from pyLibrary.dot import wrap
from pyLibrary.strings import expand_template
from pyLibrary.thread.threads import Queue, Thread, Lock
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import HOUR, MINUTE


DEBUG = True
TOO_OLD = 2*HOUR
singlton = None


class FromESMetadata(object):
    """
    QUERY THE METADATA
    """

    def __new__(cls, *args, **kwargs):
        global singlton
        if singlton:
            return singlton
        else:
            singlton = object.__new__(cls)
            return singlton

    @use_settings
    def __init__(self, host, index, alias=None, name=None, port=9200, settings=None):
        if hasattr(self, "settings"):
            return

        from pyLibrary.queries.containers.lists import ListContainer

        Container.__init__(self, None, schema=self)
        self.settings = settings
        self.default_name = coalesce(name, alias, index)
        self.default_es = elasticsearch.Cluster(settings=settings)
        self.locker = Lock("")
        self.todo = Queue("refresh metadata")

        table_columns = metadata_tables()
        column_columns = metadata_columns()
        self.tables = ListContainer([], wrap({c.name: c for c in table_columns}))
        self.columns = ListContainer([], wrap({c.name: c for c in column_columns}))
        self.columns.insert(column_columns)
        self.columns.insert(table_columns)
        self.worker = Thread.run("refresh metadata", self.monitor)
        return

    @property
    def query_path(self):
        return None

    @property
    def url(self):
        return self.default_es.path + "/" + self.default_name.replace(".", "/")

    def get_table(self, table_name):
        with self.locker:
            return self.tables.query({"where": {"eq": {"name": table_name}}})

    def upsert_column(self, c):
        existing_columns = filter(lambda r: r.table == c.table and r.abs_name == c.abs_name, self.columns.data)
        if not existing_columns:
            self.columns.add(c)
            cols = filter(lambda r: r.table == "meta.columns", self.columns.data)
            for c in cols:
                c.partitions = c.cardinality = c.last_updated = None
            self.todo.add(c)
            self.todo.extend(cols)
        else:
            set_default(existing_columns[0], c)
            self.todo.add(existing_columns[0])

    def _get_columns(self, table=None):
        # TODO: HANDLE MORE THEN ONE ES, MAP TABLE SHORT_NAME TO ES INSTANCE
        alias_done = set()
        index = split_field(table)[0]
        query_path = split_field(table)[1:]
        metadata = self.default_es.get_metadata(index=index)
        for index, meta in qb.sort(metadata.indices.items(), {"value": 0, "sort": -1}):
            for _, properties in meta.mappings.items():
                columns = elasticsearch.parse_properties(index, None, properties.properties)
                with self.locker:
                    for c in columns:
                        # ABSOLUTE
                        c.table = join_field([index]+query_path)
                        self.upsert_column(c)

                        for alias in meta.aliases:
                            # ONLY THE LATEST ALIAS IS CHOSEN TO GET COLUMNS
                            if alias in alias_done:
                                continue
                            alias_done.add(alias)

                            c = copy(c)
                            c.table = join_field([alias]+query_path)
                            self.upsert_column(c)

    def query(self, _query):
        return self.columns.query(Query(set_default(
            {
                "from": self.columns,
                "sort": ["table", "name"]
            },
            _query.as_dict()
        )))

    def get_columns(self, table):
        """
        RETURN METADATA COLUMNS
        """
        with self.locker:
            columns = qb.sort(filter(lambda r: r.table == table, self.columns.data), "name")
            if columns:
                return columns

        self._get_columns(table=table)
        with self.locker:
            columns = qb.sort(filter(lambda r: r.table == table, self.columns.data), "name")
            if columns:
                return columns

        # self._get_columns(table=table)
        Log.error("no columns for {{table}}", table=table)

    def _update_cardinality(self, c):
        """
        QUERY ES TO FIND CARDINALITY AND PARTITIONS FOR A SIMPLE COLUMN
        """
        if c.type in ["object", "nested"]:
            Log.error("not supported")
        try:
            if c.table == "meta.columns":
                with self.locker:
                    partitions = qb.sort([g[c.abs_name] for g, _ in qb.groupby(self.columns, c.abs_name) if g[c.abs_name] != None])
                    self.columns.update({
                        "set": {
                            "partitions": partitions,
                            "cardinality": len(partitions),
                            "last_updated": Date.now()
                        },
                        "where": {"eq": {"table": c.table, "abs_name": c.abs_name}}
                    })
                return
            if c.table == "meta.tables":
                with self.locker:
                    partitions = qb.sort([g[c.abs_name] for g, _ in qb.groupby(self.tables, c.abs_name) if g[c.abs_name] != None])
                    self.columns.update({
                        "set": {
                            "partitions": partitions,
                            "cardinality": len(partitions),
                            "last_updated": Date.now()
                        },
                        "where": {"eq": {"table": c.table, "name": c.name}}
                    })
                return

            result = self.default_es.post("/"+c.table+"/_search", data={
                "aggs": {c.name: _counting_query(c)},
                "size": 0
            })
            r = result.aggregations.values()[0]
            cardinaility = coalesce(r.value, r._nested.value)

            query = Dict(size=0)
            if c.type in ["object", "nested"]:
                Log.note("{{field}} has {{num}} parts", field=c.name, num=c.cardinality)
                with self.locker:
                    self.columns.update({
                        "set": {
                            "cardinality": cardinaility,
                            "last_updated": Date.now()
                        },
                        "clear": ["partitions"],
                        "where": {"eq": {"table": c.table, "name": c.name}}
                    })
                return
            elif c.cardinality > 1000:
                Log.note("{{field}} has {{num}} parts", field=c.name, num=c.cardinality)
                with self.locker:
                    self.columns.update({
                        "set": {
                            "cardinality": cardinaility,
                            "last_updated": Date.now()
                        },
                        "clear": ["partitions"],
                        "where": {"eq": {"table": c.table, "name": c.name}}
                    })
                return
            elif c.type in ES_NUMERIC_TYPES and c.cardinality > 30:
                Log.note("{{field}} has {{num}} parts", field=c.name, num=c.cardinality)
                with self.locker:
                    self.columns.update({
                        "set": {
                            "cardinality": cardinaility,
                            "last_updated": Date.now()
                        },
                        "clear": ["partitions"],
                        "where": {"eq": {"table": c.table, "name": c.name}}
                    })
                return
            elif c.nested_path:
                query.aggs[literal_field(c.name)] = {
                    "nested": {"path": listwrap(c.nested_path)[0]},
                    "aggs": {"_nested": {"terms": {"field": c.name, "size": 0}}}
                }
            else:
                query.aggs[literal_field(c.name)] = {"terms": {"field": c.name, "size": 0}}

            result = self.default_es.post("/"+c.table+"/_search", data=query)

            aggs = result.aggregations.values()[0]
            if aggs._nested:
                parts = qb.sort(aggs._nested.buckets.key)
            else:
                parts = qb.sort(aggs.buckets.key)

            Log.note("{{field}} has {{parts}}", field=c.name, parts=parts)
            with self.locker:
                self.columns.update({
                    "set": {
                        "cardinality": cardinaility,
                        "partitions": parts,
                        "last_updated": Date.now()
                    },
                    "where": {"eq": {"table": c.table, "abs_name": c.abs_name}}
                })
        except Exception, e:
            self.columns.update({
                "set": {
                    "last_updated": Date.now()
                },
                "clear":[
                    "cardinality",
                    "partitions",
                ],
                "where": {"eq": {"table": c.table, "abs_name": c.abs_name}}
            })
            Log.warning("Could not get {{col.table}}.{{col.abs_name}} info", col=c, cause=e)

    def monitor(self, please_stop):
        while not please_stop:
            if not self.todo:
                with self.locker:
                    old_columns = filter(lambda c: (c.last_updated == None or c.last_updated < Date.now()-TOO_OLD) and c.type not in ["object", "nested"], self.columns)
                    if old_columns:
                        self.todo.extend(old_columns)
                    else:
                        Log.note("no more metatdata to update")

            column = self.todo.pop(timeout=10*MINUTE)
            if column:
                if column.type in ["object", "nested"]:
                    continue
                if column.last_updated >= Date.now()-TOO_OLD:
                    continue
                self._update_cardinality(column)
                Log.note("updated {{column.name}}", column=column)


def _counting_query(c):
    if c.nested_path:
        return {
            "nested": {
                "path": listwrap(c.nested_path)[0]  # FIRST ONE IS LONGEST
            },
            "aggs": {
                "_nested": {"cardinality": {
                    "field": c.name,
                    "precision_threshold": 10 if c.type in ES_NUMERIC_TYPES else 100
                }}
            }
        }
    else:
        return {"cardinality": {
            "field": c.name
        }}


def metadata_columns():
    return wrap(
        [
            Column(
                table="meta.columns",
                name=c,
                abs_name=c,
                type="string",
                nested_path=Null,
            )
            for c in [
                "name",
                "type",
                "nested_path",
                "relative",
                "abs_name",
                "table"
            ]
        ] + [
            Column(
                table="meta.columns",
                name=c,
                abs_name=c,
                type="object",
                nested_path=Null,
            )
            for c in [
                "domain",
                "partitions"
            ]
        ] + [
            Column(
                table="meta.columns",
                name=c,
                abs_name=c,
                type="long",
                nested_path=Null,
            )
            for c in [
                "count",
                "cardinality"
            ]
        ] + [
            Column(
                table="meta.columns",
                name="last_updated",
                abs_name="last_updated",
                type="time",
                nested_path=Null,
            )
        ]
    )

def metadata_tables():
    return wrap(
        [
            Column(
                table="meta.tables",
                name=c,
                abs_name=c,
                type="string",
                nested_path=Null
            )
            for c in [
                "name",
                "url",
                "query_path"
            ]
        ]
    )





def DataClass(name, columns):
    """
    Each column has {"name", "required", "nulls", "default"} properties
    """

    columns = wrap([{"name": c, "required": True, "nulls": False} if isinstance(c, basestring) else c for c in columns])
    slots = columns.name
    required = wrap(filter(lambda c: c.required and not c.nulls and not c.default, columns)).name
    nulls = wrap(filter(lambda c: c.nulls, columns)).name

    code = expand_template("""
from __future__ import unicode_literals
from collections import Mapping

class {{name}}(Mapping):
    __slots__ = {{slots}}

    def __init__(self, **kwargs):
        if not kwargs:
            return

        for s in {{slots}}:
            setattr(self, s, kwargs.get(s, kwargs.get('default', Null)))

        missed = {{required}}-set(kwargs.keys())
        if missed:
            Log.error("Expecting properties {"+"{missed}}", missed=missed)

        illegal = set(kwargs.keys())-set({{slots}})
        if illegal:
            Log.error("{"+"{names}} are not a valid properties", names=illegal)

    def __getitem__(self, item):
        return getattr(self, item)

    def __setitem__(self, item, value):
        setattr(self, item, value)
        return self

    def __setattr__(self, item, value):
        if item not in {{slots}}:
            Log.error("{"+"{item|quote}} not valid attribute", item=item)
        object.__setattr__(self, item, value)

    def __getattr__(self, item):
        Log.error("{"+"{item|quote}} not valid attribute", item=item)

    def items(self):
        return ((k, getattr(self, k)) for k in {{slots}})

    def __copy__(self):
        _set = object.__setattr__
        output = object.__new__(Column)
        {{assign}}
        return output

    def __iter__(self):
        return {{slots}}.__iter__()

    def __len__(self):
        return {{len_slots}}

    def __str__(self):
        return str({{dict}})

temp = {{name}}
""",
        {
            "name": name,
            "slots": "(" + (", ".join(convert.value2quote(s) for s in slots)) + ")",
            "required": "{" + (", ".join(convert.value2quote(s) for s in required)) + "}",
            "nulls": "{" + (", ".join(convert.value2quote(s) for s in nulls)) + "}",
            "len_slots": len(slots),
            "dict": "{" + (", ".join(convert.value2quote(s) + ": self." + s for s in slots)) + "}",
            "assign": "; ".join("_set(output, "+convert.value2quote(s)+", self."+s+")" for s in slots)
        }
    )

    return _exec(code)


def _exec(code):
    temp = None
    exec(code)
    return temp


class Table(DataClass("Table", [
    "name",
    "url",
    "query_path"
])):
    @property
    def columns(self):
        return FromESMetadata.singlton.get_columns(table=self.name)


Column = DataClass(
    "Column",
    [
        "name",
        "abs_name",
        "table",
        "type",
        {"name": "useSource", "default": False},
        {"name": "nested_path", "nulls": True},  # AN ARRAY OF PATHS (FROM DEEPEST TO SHALLOWEST) INDICATING THE JSON SUB-ARRAYS
        {"name": "relative", "nulls": True},
        {"name": "count", "nulls": True},
        {"name": "cardinality", "nulls": True},
        {"name": "partitions", "nulls": True},
        {"name": "last_updated", "nulls": True}
    ]
)





a = {
    "_id": "182553:18220451.14.179",
    "result": {
        "status": "OK",
        "stats": {"action": {"test_status": 432}, "fail": 6, "pass": 426},
        "ok": false,
        "start_time": 1441284018.565,
        "test": "toolkit/components/prompts/test/test_modal_prompts.html",
        "result": "OK",
        "expected": "OK",
        "subtests": [
            {
                "status": "fail",
                "ok": false,
                "name": "checking button0 default",
                "ordering": 0,
                "timestamp": 1441284033.645,
                "subtest": "checking button0 default",
                "expected": "pass",
                "message": "got false, expected true"
            },
            {
                "status": "fail",
                "ok": false,
                "name": "checking button0 default",
                "ordering": 1,
                "timestamp": 1441284034.381,
                "subtest": "checking button0 default",
                "expected": "pass",
                "message": "got false, expected true"
            }
        ],
        "last_log_time": 1441284036.021,
        "duration": 17.473999977111816,
        "end_time": 1441284036.039
    },
    "run": {
        "files": {
            "url": "http://mozilla-releng-blobs.s3.amazonaws.com/blobs/mozilla-inbound/sha512/e6c834818fbbad050543c7dc5ec7ddc00b2433ea9dcfa0217bd99a3ae40daacfd74c9225a7a8945d5a4e02449af5ffc6452bc7c6a97a49592227cfca11dc1f93",
            "name": "plain-chunked_raw.log"
        },
        "status": 1,
        "stats": {
            "status": {"skip": 18, "ok": 636},
            "ok": 653,
            "lines": 3705,
            "start_time": 1441283166.06,
            "bytes": 869394,
            "action": {
                "test_status": 617,
                "log": 1718,
                "process_exit": 30,
                "process_start": 30
            },
            "end_time": 1441284124.884,
            "duration": 958.824000120163,
            "total": 654
        },
        "buildbot_status": "warnings",
        "timestamp": 1441284143,
        "chunk": 5,
        "talos": false,
        "machine": {"os": "ubuntu64_vm", "name": "tst-linux64-spot-1771"},
        "key": "build.mozilla-inbound_ubuntu64_vm_test_pgo-mochitest-5.53.log_uploaded",
        "suite": "mochitest",
        "insertion_time": 1441284143,
        "job_number": 53,
        "logurl": "http://ftp.mozilla.org/pub/mozilla.org/firefox/tinderbox-builds/mozilla-inbound-linux64-pgo/1441279802/mozilla-inbound_ubuntu64_vm_test_pgo-mochitest-5-bm122-tests1-linux64-build53.txt.gz"
    },
    "build": {
        "product": "firefox",
        "revision12": "550e06c1615f",
        "name": "Ubuntu VM 12.04 x64 mozilla-inbound pgo test mochitest-5",
        "url": "https://queue.taskcluster.net/v1/task/T6IwwMnmQLWMHAZ3Y2Z9xA/artifacts/public/build/firefox-43.0a1.en-US.linux-x86_64.tar.bz2",
        "platform": "linux64",
        "branch": "mozilla-inbound",
        "date": 1441254602,
        "release": "production",
        "locale": "en-US",
        "type": "pgo",
        "id": "20150903043002",
        "revision": "550e06c1615f82cc82278873f8137edbdd0c3c3a"
    },
    "repo": {
        "changeset": {
            "files": [
                "js/src/vm/HelperThreads.cpp",
                "js/src/vm/HelperThreads.h",
                "js/src/vm/Runtime.cpp"
            ],
            "description": "Bug 1201051 - Make helper thread initialization report errors rather than crashing r=terrence",
            "author": "Jon <replaced@mozilla.com>",
            "id12": "550e06c1615f",
            "date": 1441278480,
            "id": "550e06c1615f82cc82278873f8137edbdd0c3c3a"
        },
        "index": 260643,
        "parents": "21e229c5ff708285d9e2c837c9679aab55ee4c48",
        "branch": {
            "url": "https://hg.mozilla.org/integration/mozilla-inbound",
            "last_used": 1440115924,
            "locale": "en-US",
            "name": "mozilla-inbound",
            "parent_name": "Source code integration work"
        },
        "push": {
            "date": 1441278531,
            "user": "replaced@mozilla.com",
            "id": 64555
        },
        "children": "e737274a67063f673d47d1895e3008dc6d73cf9c",
        "etl": {"timestamp": 1441286446.851573}
    },
    "etl": {
        "name": "unittest",
        "timestamp": 1441286777.499345,
        "source": {
            "os": "Linux3.13.0-48-generic",
            "python": "CPython",
            "timestamp": 1441286769.820252,
            "source": {
                "name": "Pulse block",
                "timestamp": 1441284205.602198,
                "bucket": "ekyle-pulse-logger",
                "source": {
                    "count": 18220475,
                    "name": "pulse.mozilla.org",
                    "sent": 1441255343,
                    "id": 18220475
                },
                "type": "aggregation",
                "id": 182553
            },
            "type": "join",
            "id": 14,
            "name": "plain-chunked_raw.log"
        },
        "duration": 7.638665000000002,
        "type": "join",
        "id": 179,
        "revision": "bed2888f2ba55f32ac8792944c1ca822797cf7e3"
    }
}
