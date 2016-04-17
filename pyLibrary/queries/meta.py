# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import itertools
from copy import copy
from itertools import product

from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce, set_default, Null, literal_field, listwrap, split_field, join_field
from pyLibrary.dot import wrap
from pyLibrary.dot.dicts import Dict
from pyLibrary.meta import use_settings, DataClass
from pyLibrary.queries import jx, Schema
from pyLibrary.queries.query import QueryOp
from pyLibrary.thread.threads import Queue, Thread
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import HOUR, MINUTE
from pyLibrary.times.timer import Timer

_elasticsearch = None

ENABLE_META_SCAN = False
DEBUG = True
TOO_OLD = 2*HOUR
singlton = None
TEST_TABLE_PREFIX = "testing"  # USED TO TURN OFF COMPLAINING ABOUT TEST INDEXES



class FromESMetadata(Schema):
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
        global _elasticsearch
        if hasattr(self, "settings"):
            return

        from pyLibrary.queries.containers.lists import ListContainer
        from pyLibrary.env import elasticsearch as _elasticsearch

        self.settings = settings
        self.default_name = coalesce(name, alias, index)
        self.default_es = _elasticsearch.Cluster(settings=settings)
        self.todo = Queue("refresh metadata", max=100000, unique=True)

        self.meta=Dict()
        table_columns = metadata_tables()
        column_columns = metadata_columns()
        self.meta.tables = ListContainer("meta.tables", [], wrap({c.name: c for c in table_columns}))
        self.meta.columns = ListContainer("meta.columns", [], wrap({c.name: c for c in column_columns}))
        self.meta.columns.insert(column_columns)
        self.meta.columns.insert(table_columns)
        # TODO: fix monitor so it does not bring down ES
        if ENABLE_META_SCAN:
            self.worker = Thread.run("refresh metadata", self.monitor)
        else:
            self.worker = Thread.run("refresh metadata", self.not_monitor)
        return

    @property
    def query_path(self):
        return None

    @property
    def url(self):
        return self.default_es.path + "/" + self.default_name.replace(".", "/")

    def get_table(self, table_name):
        with self.meta.tables.locker:
            return self.meta.tables.query({"where": {"eq": {"name": table_name}}})

    def _upsert_column(self, c):
        # ASSUMING THE  self.meta.columns.locker IS HAD
        existing_columns = [r for r in self.meta.columns.data if r.table == c.table and r.name == c.name]
        if not existing_columns:
            self.meta.columns.add(c)
            Log.note("todo: {{table}}.{{column}}", table=c.table, column=c.es_column)
            self.todo.add(c)

            # MARK meta.columns AS DIRTY TOO
            cols = [r for r in self.meta.columns.data if r.table == "meta.columns"]
            for cc in cols:
                cc.partitions = cc.cardinality = None
                cc.last_updated = Date.now()
            self.todo.extend(cols)
        else:
            canonical = existing_columns[0]
            if canonical.relative and not c.relative:
                return  # RELATIVE COLUMNS WILL SHADOW ABSOLUTE COLUMNS

            for key in Column.__slots__:
                canonical[key] = c[key]
            Log.note("todo: {{table}}.{{column}}", table=canonical.table, column=canonical.es_column)
            self.todo.add(canonical)

    def _get_columns(self, table=None, metadata=None):
        # TODO: HANDLE MORE THEN ONE ES, MAP TABLE SHORT_NAME TO ES INSTANCE
        if not metadata:
            metadata = self.default_es.get_metadata(force=True)

        def parse_all(please_stop):
            for abs_index, meta in jx.sort(metadata.indices.items(), {"value": 0, "sort": -1}):
                if meta.index != abs_index:
                    continue

                for _, properties in meta.mappings.items():
                    if please_stop:
                        return
                    self._parse_properties(abs_index, properties, meta)

        if table:
            for abs_index, meta in jx.sort(metadata.indices.items(), {"value": 0, "sort": -1}):
                if table == meta.index:
                    for _, properties in meta.mappings.items():
                        self._parse_properties(abs_index, properties, meta)
                    return
                if table == abs_index:
                    self._get_columns(table=meta.index, metadata=metadata)
                    return
        else:
            self.parser = Thread.run("parse properties", parse_all)




    def _parse_properties(self, abs_index, properties, meta):
        abs_columns = _elasticsearch.parse_properties(abs_index, None, properties.properties)
        abs_columns = abs_columns.filter(  # TODO: REMOVE WHEN jobs PROPERTY EXPLOSION IS CONTAINED
            lambda r: not r.es_column.startswith("other.") and
                      not r.es_column.startswith("previous_values.cf_") and
                      not r.es_index.startswith("debug")
        )
        with Timer("upserting {{num}} columns", {"num": len(abs_columns)}, debug=DEBUG):
            def add_column(c, query_path):
                c.last_updated = Date.now()
                if query_path:
                    c.table = c.es_index + "." + query_path.last()
                else:
                    c.table = c.es_index

                with self.meta.columns.locker:
                    self._upsert_column(c)
                    for alias in meta.aliases:
                        c = copy(c)
                        if query_path:
                            c.table = alias + "." + query_path.last()
                        else:
                            c.table = alias
                        self._upsert_column(c)

            # EACH query_path IS A LIST OF EVER-INCREASING PATHS THROUGH EACH NESTED LEVEL
            query_paths = wrap([[c.es_column] for c in abs_columns if c.type == "nested"])
            for a, b in itertools.product(query_paths, query_paths):
                aa = a.last()
                bb = b.last()
                if aa and bb.startswith(aa):
                    for i, b_prefix in enumerate(b):
                        if len(b_prefix) < len(aa):
                            continue
                        if aa == b_prefix:
                            break  # SPLIT ALREADY FOUND
                        b.insert(0, aa)
                        break
            query_paths.append([])

            for c in abs_columns:
                # ADD RELATIVE COLUMNS
                full_path = listwrap(c.nested_path)
                abs_depth = len(full_path)
                abs_parent = coalesce(full_path.last(), "")
                for query_path in query_paths:
                    rel_depth = len(query_path)

                    # ABSOLUTE
                    add_column(copy(c), query_path)
                    cc = copy(c)
                    cc.relative = True

                    if not query_path:
                        add_column(cc, query_path)
                        continue

                    rel_parent = query_path.last()

                    if c.es_column.startswith(rel_parent+"."):
                        cc.name = c.es_column[len(rel_parent)+1:]
                        add_column(cc, query_path)
                    elif c.es_column == rel_parent:
                        cc.name = "."
                        add_column(cc, query_path)
                    elif not abs_parent:
                        # THIS RELATIVE NAME (..o) ALSO NEEDS A RELATIVE NAME (o)
                        # AND THEN REMOVE THE SHADOWED
                        cc.name = "." + ("." * (rel_depth - abs_depth)) + c.es_column
                        add_column(cc, query_path)
                    elif rel_parent.startswith(abs_parent+"."):
                        cc.name = "." + ("." * (rel_depth - abs_depth)) + c.es_column
                        add_column(cc, query_path)
                    elif rel_parent != abs_parent:
                        # SIBLING NESTED PATHS ARE INVISIBLE
                        pass
                    else:
                        Log.error("logic error")


    def query(self, _query):
        return self.meta.columns.query(QueryOp(set_default(
            {
                "from": self.meta.columns,
                "sort": ["table", "name"]
            },
            _query.as_dict()
        )))

    def get_columns(self, table_name, column_name=None, fail_when_not_found=False):
        """
        RETURN METADATA COLUMNS
        """
        try:
            with self.meta.columns.locker:
                columns = [c for c in self.meta.columns.data if c.table == table_name and (column_name is None or c.name==column_name)]
            if columns:
                columns = jx.sort(columns, "name")
                if fail_when_not_found:
                    # AT LEAST WAIT FOR THE COLUMNS TO UPDATE
                    while len(self.todo) and not all(columns.get("last_updated")):
                        Log.note("waiting for columns to update {{columns|json}}", columns=[c.table+"."+c.es_column for c in columns if not c.last_updated])
                        Thread.sleep(seconds=1)
                    return columns
                elif all(columns.get("last_updated")):
                    return columns
        except Exception, e:
            Log.error("Not expected", cause=e)

        if fail_when_not_found:
            if column_name:
                Log.error("no columns matching {{table}}.{{column}}", table=table_name, column=column_name)
            else:
                self._get_columns(table=table_name)
                Log.error("no columns for {{table}}", table=table_name)

        self._get_columns(table=join_field(split_field(table_name)[0:1]))
        return self.get_columns(table_name=table_name, column_name=column_name, fail_when_not_found=True)

    def _update_cardinality(self, c):
        """
        QUERY ES TO FIND CARDINALITY AND PARTITIONS FOR A SIMPLE COLUMN
        """
        if c.type in ["object", "nested"]:
            Log.error("not supported")
        try:
            if c.table == "meta.columns":
                with self.meta.columns.locker:
                    partitions = jx.sort([g[c.es_column] for g, _ in jx.groupby(self.meta.columns, c.es_column) if g[c.es_column] != None])
                    self.meta.columns.update({
                        "set": {
                            "partitions": partitions,
                            "count": len(self.meta.columns),
                            "cardinality": len(partitions),
                            "last_updated": Date.now()
                        },
                        "where": {"eq": {"table": c.table, "es_column": c.es_column}}
                    })
                return
            if c.table == "meta.tables":
                with self.meta.columns.locker:
                    partitions = jx.sort([g[c.es_column] for g, _ in jx.groupby(self.meta.tables, c.es_column) if g[c.es_column] != None])
                    self.meta.columns.update({
                        "set": {
                            "partitions": partitions,
                            "count": len(self.meta.tables),
                            "cardinality": len(partitions),
                            "last_updated": Date.now()
                        },
                        "where": {"eq": {"table": c.table, "name": c.name}}
                    })
                return

            es_index = c.table.split(".")[0]
            result = self.default_es.post("/"+es_index+"/_search", data={
                "aggs": {c.name: _counting_query(c)},
                "size": 0
            })
            r = result.aggregations.values()[0]
            count = result.hits.total
            cardinality = coalesce(r.value, r._nested.value, 0 if r.doc_count==0 else None)
            if cardinality == None:
                Log.error("logic error")

            query = Dict(size=0)
            if cardinality > 1000 or (count >= 30 and cardinality == count) or (count >= 1000 and cardinality / count > 0.99):
                Log.note("{{table}}.{{field}} has {{num}} parts", table=c.table, field=c.es_column, num=cardinality)
                with self.meta.columns.locker:
                    self.meta.columns.update({
                        "set": {
                            "count": count,
                            "cardinality": cardinality,
                            "last_updated": Date.now()
                        },
                        "clear": ["partitions"],
                        "where": {"eq": {"es_index": c.es_index, "es_column": c.es_column}}
                    })
                return
            elif c.type in _elasticsearch.ES_NUMERIC_TYPES and cardinality > 30:
                Log.note("{{field}} has {{num}} parts", field=c.name, num=cardinality)
                with self.meta.columns.locker:
                    self.meta.columns.update({
                        "set": {
                            "count": count,
                            "cardinality": cardinality,
                            "last_updated": Date.now()
                        },
                        "clear": ["partitions"],
                        "where": {"eq": {"es_index": c.es_index, "es_column": c.es_column}}
                    })
                return
            elif c.nested_path:
                query.aggs[literal_field(c.name)] = {
                    "nested": {"path": listwrap(c.nested_path)[0]},
                    "aggs": {"_nested": {"terms": {"field": c.es_column, "size": 0}}}
                }
            else:
                query.aggs[literal_field(c.name)] = {"terms": {"field": c.es_column, "size": 0}}

            result = self.default_es.post("/"+es_index+"/_search", data=query)

            aggs = result.aggregations.values()[0]
            if aggs._nested:
                parts = jx.sort(aggs._nested.buckets.key)
            else:
                parts = jx.sort(aggs.buckets.key)

            Log.note("{{field}} has {{parts}}", field=c.name, parts=parts)
            with self.meta.columns.locker:
                self.meta.columns.update({
                    "set": {
                        "count": count,
                        "cardinality": cardinality,
                        "partitions": parts,
                        "last_updated": Date.now()
                    },
                    "where": {"eq": {"es_index": c.es_index, "es_column": c.es_column}}
                })
        except Exception, e:
            if "IndexMissingException" in e and c.table.startswith(TEST_TABLE_PREFIX):
                with self.meta.columns.locker:
                    self.meta.columns.update({
                        "set": {
                            "count": 0,
                            "cardinality": 0,
                            "last_updated": Date.now()
                        },
                        "clear":[
                            "partitions"
                        ],
                        "where": {"eq": {"es_index": c.es_index, "es_column": c.es_column}}
                    })
            else:
                self.meta.columns.update({
                    "set": {
                        "last_updated": Date.now()
                    },
                    "clear": [
                        "count",
                        "cardinality",
                        "partitions",
                    ],
                    "where": {"eq": {"table": c.table, "es_column": c.es_column}}
                })
                Log.warning("Could not get {{col.table}}.{{col.es_column}} info", col=c, cause=e)

    def monitor(self, please_stop):
        please_stop.on_go(lambda: self.todo.add(Thread.STOP))
        while not please_stop:
            try:
                if not self.todo:
                    with self.meta.columns.locker:
                        old_columns = filter(
                            lambda c: (c.last_updated == None or c.last_updated < Date.now()-TOO_OLD) and c.type not in ["object", "nested"],
                            self.meta.columns
                        )
                        if old_columns:
                            Log.note("Old columns wth dates {{dates|json}}", dates=wrap(old_columns).last_updated)
                            self.todo.extend(old_columns)
                            # TEST CONSISTENCY
                            for c, d in product(list(self.todo.queue), list(self.todo.queue)):
                                if c.es_column == d.es_column and c.table == d.table and c != d:
                                    Log.error("")
                        else:
                            Log.note("no more metatdata to update")

                column = self.todo.pop(timeout=10*MINUTE)
                if column:
                    Log.note("update {{table}}.{{column}}", table=column.table, column=column.es_column)
                    if column.type in ["object", "nested"]:
                        with self.meta.columns.locker:
                            column.last_updated = Date.now()
                        continue
                    elif column.last_updated >= Date.now()-TOO_OLD:
                        continue
                    try:
                        self._update_cardinality(column)
                        if DEBUG and not column.table.startswith(TEST_TABLE_PREFIX):
                            Log.note("updated {{column.name}}", column=column)
                    except Exception, e:
                        Log.warning("problem getting cardinality for {{column.name}}", column=column, cause=e)
            except Exception, e:
                Log.warning("problem in cardinality monitor", cause=e)

    def not_monitor(self, please_stop):
        Log.alert("metadata scan has been disabled")
        please_stop.on_go(lambda: self.todo.add(Thread.STOP))
        while not please_stop:
            c = self.todo.pop()
            if c == Thread.STOP:
                break

            if not c.last_updated or c.last_updated >= Date.now()-TOO_OLD:
                continue

            with self.meta.columns.locker:
                self.meta.columns.update({
                    "set": {
                        "last_updated": Date.now()
                    },
                    "clear":[
                        "count",
                        "cardinality",
                        "partitions",
                    ],
                    "where": {"eq": {"es_index": c.es_index, "es_column": c.es_column}}
                })
            Log.note("Could not get {{col.es_index}}.{{col.es_column}} info", col=c)


def _counting_query(c):
    if c.nested_path:
        return {
            "nested": {
                "path": listwrap(c.nested_path)[0]  # FIRST ONE IS LONGEST
            },
            "aggs": {
                "_nested": {"cardinality": {
                    "field": c.es_column,
                    "precision_threshold": 10 if c.type in _elasticsearch.ES_NUMERIC_TYPES else 100
                }}
            }
        }
    else:
        return {"cardinality": {
            "field": c.es_column
        }}


def metadata_columns():
    return wrap(
        [
            Column(
                table="meta.columns",
                es_index=None,
                name=c,
                es_column=c,
                type="string",
                nested_path=Null,
            )
            for c in [
                "name",
                "type",
                "nested_path",
                "relative",
                "es_column",
                "table"
            ]
        ] + [
            Column(
                table="meta.columns",
                es_index=None,
                name=c,
                es_column=c,
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
                es_index=None,
                name=c,
                es_column=c,
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
                es_index=None,
                name="last_updated",
                es_column="last_updated",
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
                es_index=None,
                es_column=c,
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





class Table(DataClass("Table", [
    "name",
    "url",
    "query_path"
])):
    @property
    def columns(self):
        return FromESMetadata.singlton.get_columns(table_name=self.name)


Column = DataClass(
    "Column",
    [
        "name",
        "table",
        "es_column",
        "es_index",
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



