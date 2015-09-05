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

from pyLibrary.env import elasticsearch
from pyLibrary.env.elasticsearch import ES_NUMERIC_TYPES
from pyLibrary.meta import use_settings
from pyLibrary.queries import qb
from pyLibrary.queries.containers import Container
from pyLibrary.queries.containers.lists import ListContainer
from pyLibrary.queries.query import Query
from pyLibrary.debugs.logs import Log
from pyLibrary.dot.dicts import Dict
from pyLibrary.dot import coalesce, set_default, Null, literal_field
from pyLibrary.dot import wrap
from pyLibrary.strings import Duration
from pyLibrary.thread.threads import Queue, Thread
from pyLibrary.times.dates import Date


class FromESMetadata(Container):
    """
    QUERY THE METADATA
    """
    singleton = None


    @use_settings
    def __init__(self, host, index, alias=None, name=None, port=9200, settings=None):
        if FromESMetadata.singlton:
            Log.error("only one metadata manager allowed")
        FromESMetadata.singlton = self

        Container.__init__(self, None, schema=self)
        self.settings = settings
        self.name = coalesce(name, alias, index)
        self.default_es = elasticsearch.Cluster(settings=settings)
        self.columns = ListContainer([], {c.name: c for c in self.get_columns(table="meta.columns")})
        self.todo = Queue("refresh metadata")
        self.worker = Thread.run("refresh metadata", self.monitor)
        self.locker = Lock("")
        return

    @property
    def query_path(self):
        return None

    @property
    def url(self):
        return self.default_es.path + "/" + self.name.replace(".", "/")

    def _get_columns(self, index_name):
        all_columns = []
        alias_done = set()
        metadata = self.default_es.get_metadata()
        for index, meta in qb.sort(metadata.indices.items(), {"value": 0, "sort": -1}):
            for _, properties in meta.mappings.items():
                columns = _parse_properties(index, properties.properties)
                for c in columns:
                    c.table = index

                with self.locker():
                    all_columns.update({
                        "clear":".",
                        "where":{"eq":{"table": index}}
                    })
                    all_columns.insert(columns)

                for a in meta.aliases:
                    # ONLY THE LATEST ALIAS IS CHOSEN TO GET COLUMNS
                    if a in alias_done:
                        continue
                    alias_done.add(a)

                    for c in columns:
                        cc = copy(c)
                        cc.table = a
                        all_columns.append(cc)
                        self.todo.extend(columns)

                    with self.locker():
                        all_columns.update({
                            "clear":".",
                            "where":{"eq":{"table": a}}
                        })
                        all_columns.insert(columns)
                        self.todo.extend(columns)


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
        if table == "meta.columns":
            return metadata_columns()
        else:

            Log.error("Unknonw metadata: {{name}}.  Only `meta.columns` exists for now.", name=self.settings.name)

    def _update_cardinality(self, c):
        """
        QUERY ES TO FIND CARDINALITY AND PARTITIONS FOR A SIMPLE COLUMN
        """
        if c.type in ["object", "nested"]:
            Log.error("not supported")

        result = self.default_es.search({
            "aggs": {c.name: _counting_query(c)},
            "size": 0
        })
        r = result.aggregations.values()[0]
        cardinaility = coalesce(r.value, r._nested.value)

        query = Dict(size=0)
        if c.type in ["object", "nested"] or c.cardinality > 1000:
            Log.note("{{field}} has {{num}} parts", field=c.name, num=c.cardinality)
            self.columns.update({
                "set": {
                    "cardinality": cardinaility,
                    "partitions": None,
                    "last_updated": Date.now()
                },
                "clear": ["partitions"],
                "where": {"eq": {"table": c.table, "name": c.name}}
            })
            return
        elif c.type in ES_NUMERIC_TYPES and c.cardinality > 30:
            Log.note("{{field}} has {{num}} parts", field=c.name, num=c.cardinality)
            self.columns.update({
                "set": {
                    "cardinality": cardinaility,
                    "partitions": None,
                    "last_updated": Date.now()
                },
                "clear": ["partitions"],
                "where": {"eq": {"table": c.table, "name": c.name}}
            })
            return
        elif c.nested_path:
            query.aggs[literal_field(c.name)] = {
                "nested": {"path": c.nested_path[0]},
                "aggs": {"_nested": {"terms": {"field": c.name, "size": 0}}}
            }
        else:
            query.aggs[literal_field(c.name)] = {"terms": {"field": c.name, "size": 0}}

        result = self.default_es.search(query)

        aggs = result.aggregations.values()[0]
        if aggs._nested:
            parts = qb.sort(aggs._nested.buckets.key)
        else:
            parts = qb.sort(aggs.buckets.key)

        Log.note("{{field}} has {{parts}}", field=c.name, parts=parts)
        self.columns.update({
            "set": {
                "cardinality": cardinaility,
                "partitions": parts,
                "last_updated": Date.now()
            },
            "where": {"eq": {"table": c.table, "name": c.name}}
        })

    def monitor(self, please_stop):
        while not please_stop:
            if not self.todo:
                old_columns = self.columns.query({
                    "select": ".",
                    "where": {"or":[
                        {"missing":"etl.timestamp"},
                        {"gt": {"etl.timestamp": Date.now()-Duration("2hour")}}
                    ]}
                })
                self.todo.extend(old_columns)

            column = self.todo.pop(Duration.MINUTE*10)
            if column:
                self._update_cardinality(column)
            else:
                Thread.sleep(Duration.MINUTE)

class Column(object):
    """
    REPRESENT A DATABASE COLUMN IN THE ELASTICSEARCH
    """
    __slots__ = (
        "name",
        "type",
        "nested_path",
        "useSource",
        "domain",
        "relative",
        "abs_name",
        "table",
        "count",
        "cardinality",
        "partitions",
        "last_updated"
    )

    def __init__(self, **kwargs):
        for s in Column.__slots__:
            setattr(self, s, kwargs.get(s, Null))

        for k in kwargs.keys():
            if k not in Column.__slots__:
                Log.error("{{name}} is not a valid property", name=k)

    def __getitem__(self, item):
        return getattr(self, item)

    def __getattr__(self, item):
        Log.error("{{item|quote}} not valid attribute", item=item)

    def __copy__(self):
        return Column(**{k: getattr(self, k) for k in Column.__slots__})

    def as_dict(self):
        return wrap({k: getattr(self, k) for k in Column.__slots__})

    def __dict__(self):
        Log.error("use as_dict()")


def _counting_query(c):
    if c.nested_path:
        return {
            "nested": {
                "path": c.nested_path[0] # FIRST ONE IS LONGEST
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
                name="etl.timestamp",
                type="long",
                nested_path=Null,
            )
        ]
    )

def parse_columns(parent_index_name, parent_query_path, esProperties):
    """
    RETURN THE COLUMN DEFINITIONS IN THE GIVEN esProperties OBJECT
    """
    columns = DictList()
    for name, property in esProperties.items():
        if parent_query_path:
            index_name, query_path = parent_index_name, join_field(split_field(parent_query_path) + [name])
        else:
            index_name, query_path = parent_index_name, name

        if property.type == "nested" and property.properties:
            # NESTED TYPE IS A NEW TYPE DEFINITION
            # MARKUP CHILD COLUMNS WITH THE EXTRA DEPTH
            self_columns = parse_columns(index_name, query_path, property.properties)
            for c in self_columns:
                if not c.nested_path:
                    c.nested_path = [query_path]
                else:
                    c.nested_path.insert(0, query_path)
            columns.extend(self_columns)
            columns.append(Column(
                table=index_name,
                name=query_path,
                type="nested",
                nested_path=[name],
                useSource=False,
                domain=Null
            ))

            continue

        if property.properties:
            child_columns = parse_columns(index_name, query_path, property.properties)
            columns.extend(child_columns)
            columns.append(Column(
                table=index_name,
                name=query_path,
                type="object",
                nested_path=Null,
                useSource=False,
                domain=Null
            ))

        if property.dynamic:
            continue
        if not property.type:
            continue
        if property.type == "multi_field":
            property.type = property.fields[name].type  # PULL DEFAULT TYPE
            for i, (n, p) in enumerate(property.fields.items()):
                if n == name:
                    # DEFAULT
                    columns.append(Column(
                        table=index_name,
                        name=query_path,
                        type=p.type,
                        useSource=p.index == "no"
                    ))
                else:
                    columns.append(Column(
                        table=index_name,
                        name=query_path + "." + n,
                        type=p.type,
                        useSource=p.index == "no"
                    ))
            continue

        if property.type in ["string", "boolean", "integer", "date", "long", "double"]:
            columns.append(Column(
                table=index_name,
                name=query_path,
                type=property.type,
                nested_path=Null,
                useSource=property.index == "no",
                domain=Null
            ))
            if property.index_name and name != property.index_name:
                columns.append(Column(
                    table=index_name,
                    name=property.index_name,
                    type=property.type,
                    nested_path=Null,
                    useSource=property.index == "no",
                    domain=Null
                ))
        elif property.enabled == None or property.enabled == False:
            columns.append(Column(
                table=index_name,
                name=query_path,
                type="object",
                nested_path=Null,
                useSource=True,
                domain=Null
            ))
        else:
            Log.warning("unknown type {{type}} for property {{path}}", type=property.type, path=query_path)

    return columns

