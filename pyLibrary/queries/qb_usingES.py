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

from collections import Mapping
import sys
from pyLibrary import convert
from pyLibrary.env import elasticsearch, http
from pyLibrary.meta import use_settings
from pyLibrary.queries import qb, expressions, containers
from pyLibrary.queries.containers import Container
from pyLibrary.queries.domains import is_keyword
from pyLibrary.queries.es09 import setop as es09_setop
from pyLibrary.queries.es09 import aggop as es09_aggop
from pyLibrary.queries.es14.aggs import es_aggsop, is_aggsop
from pyLibrary.queries.es14.deep import is_deepop, es_deepop
from pyLibrary.queries.es14.setop import is_setop, es_setop
from pyLibrary.queries.dimensions import Dimension
from pyLibrary.queries.es14.util import aggregates1_4
from pyLibrary.queries.expressions import qb_expression
from pyLibrary.queries.meta import FromESMetadata
from pyLibrary.queries.namespace.typed import Typed
from pyLibrary.queries.query import Query, _normalize_where
from pyLibrary.debugs.exceptions import Except
from pyLibrary.debugs.logs import Log
from pyLibrary.dot.dicts import Dict
from pyLibrary.dot import coalesce, split_field, literal_field, unwraplist, join_field, unwrap
from pyLibrary.dot.lists import DictList
from pyLibrary.dot import wrap, listwrap


class FromES(Container):
    """
    SEND GENERAL qb QUERIES TO ElasticSearch
    """

    def __new__(cls, *args, **kwargs):
        if (len(args) == 1 and args[0].get("index") == "meta") or kwargs.get("index") == "meta":
            output = FromESMetadata.__new__(FromESMetadata, *args, **kwargs)
            output.__init__(*args, **kwargs)
            return output
        else:
            return Container.__new__(cls)

    @use_settings
    def __init__(self, host, index, type=None, alias=None, name=None, port=9200, read_only=True, settings=None):
        Container.__init__(self, None, None)
        if not containers.config.default:
            containers.config.default.settings = settings
        self.settings = settings
        self.name = coalesce(name, alias, index)
        if read_only:
            self._es = elasticsearch.Alias(alias=coalesce(alias, index), settings=settings)
        else:
            self._es = elasticsearch.Cluster(settings=settings).get_index(read_only=read_only, settings=settings)

        self.meta = FromESMetadata(settings=settings)
        self.settings.type = self._es.settings.type
        self.edges = Dict()
        self.worker = None
        self._columns = self.get_columns()
        self.schema = {c.name: c for c in self._columns}
        # SWITCH ON TYPED MODE
        self.typed = any(c.name in ("$value", "$object") for c in self._columns)

    @staticmethod
    def wrap(es):
        output = FromES(es.settings)
        output._es = es
        return output

    def as_dict(self):
        settings = self.settings.copy()
        settings.settings = None
        return settings

    def __json__(self):
        return convert.value2json(self.as_dict())


    def __enter__(self):
        Log.error("No longer used")
        return self

    def __exit__(self, type, value, traceback):
        if not self.worker:
            return

        if isinstance(value, Exception):
            self.worker.stop()
            self.worker.join()
        else:
            self.worker.join()

    @property
    def query_path(self):
        return join_field(split_field(self.name)[1:])

    @property
    def url(self):
        return self._es.url

    def query(self, _query):
        try:
            query = Query(_query, schema=self)

            for n in self.namespaces:
                query = n.convert(query)
            if self.typed:
                query = Typed().convert(query)

            for s in listwrap(query.select):
                if not aggregates1_4.get(s.aggregate):
                    Log.error("ES can not aggregate " + s.name + " because '" + s.aggregate + "' is not a recognized aggregate")

            frum = query["from"]
            if isinstance(frum, Query):
                result = self.query(frum)
                q2 = query.copy()
                q2.frum = result
                return qb.run(q2)

            if is_deepop(self._es, query):
                return es_deepop(self._es, query)
            if is_aggsop(self._es, query):
                return es_aggsop(self._es, frum, query)
            if is_setop(self._es, query):
                return es_setop(self._es, query)
            if es09_setop.is_setop(query):
                return es09_setop.es_setop(self._es, None, query)
            if es09_aggop.is_aggop(query):
                return es09_aggop.es_aggop(self._es, None, query)
            Log.error("Can not handle")
        except Exception, e:
            e = Except.wrap(e)
            if "Data too large, data for" in e:
                http.post(self._es.cluster.path+"/_cache/clear")
                Log.error("Problem (Tried to clear Elasticsearch cache)", e)
            Log.error("problem", e)

    def get_columns(self, table=None):
        if table is None or table==self.settings.index or table==self.settings.alias:
            pass
        elif table.startswith(self.settings.index+".") or table.startswith(self.setings.alias):
            pass
        else:
            Log.error("expecting `table` to be same as, or deeper, than index name")
        query_path = self.query_path if self.query_path != "." else None
        abs_columns = [copy(c) for c in unwrap(self.meta.get_columns(table=coalesce(table, self.settings.index)))]

        columns = []
        shadowed_columns = set()

        def add_column(c):
            columns.append(c)
            if c.relative:
                for a in abs_columns:
                    if a.name.startswith(c.name + ".") or a.name == c.name:
                        shadowed_columns.add(a)

        if query_path:
            try:
                query_depth = (len(listwrap(c.nested_path)) for c in abs_columns if listwrap(c.nested_path)[0] == query_path).next()
            except Exception:
                Log.error("{{path}} does not exist", path=query_path)

            # ADD RELATIVE COLUMNS
            for c in abs_columns:
                full_path = listwrap(c.nested_path)
                nested_path = full_path[0]
                if nested_path == query_path:
                    add_column(c)
                    c = copy(c)
                    c.name = c.abs_name[len(query_path) + 1:] if c.type != "nested" else "."
                    c.relative = True
                    add_column(c)
                elif not full_path:
                    add_column(c)
                    c = copy(c)
                    c.name = "." + ("." * query_depth) + c.abs_name
                    c.relative = True
                    add_column(c)
                elif query_depth > len(full_path) and query_path.startswith(nested_path + "."):
                    diff = query_depth - len(full_path)
                    add_column(c)
                    c = copy(c)
                    c.name = "." + ("." * diff) + (c.abs_name[len(nested_path) + 1:] if c.type != "nested" else "")
                    c.relative = True
                    add_column(c)
                elif c.abs_name.startswith(query_path + "."):
                    add_column(c)
                    c = copy(c)
                    c.name = c.abs_name[len(query_path)+1:]
                    c.relative = True
                    add_column(c)
                elif c.abs_name == query_path:
                    add_column(c)
                    c = copy(c)
                    c.name = "."
                    c.relative = True
                    add_column(c)
                elif query_path and nested_path != query_path:
                    # SIBLING NESTED PATHS ARE INVISIBLE
                    pass
                else:
                    Log.error("logic error")
        else:
            for c in abs_columns:
                c = copy(c)
                c.relative = True
                add_column(c)

        columns = [c for c in columns if c not in shadowed_columns]
        return wrap(columns)

    def addDimension(self, dim):
        if isinstance(dim, list):
            Log.error("Expecting dimension to be a object, not a list:\n{{dim}}",  dim= dim)
        self._addDimension(dim, [])

    def _addDimension(self, dim, path):
        dim.full_name = dim.name
        for e in dim.edges:
            d = Dimension(e, dim, self)
            self.edges[d.full_name] = d

    def __getitem__(self, item):
        c = self.schema.get(item)
        if c:
             return c

        e = self.edges[item]
        if not c:
            Log.warning("Column with name {{column|quote}} can not be found in {{table}}", column=item, table=self.name)
        return e

    def __getattr__(self, item):
        return self.edges[item]

    def normalize_edges(self, edges):
        output = DictList()
        for e in listwrap(edges):
            output.extend(self._normalize_edge(e))
        return output

    def _normalize_edge(self, edge):
        """
        RETURN A EDGE DEFINITION INTO A SIMPLE ARRAY OF PATH-LEAF
        DEFINITIONS [ {"name":<pathA>, "value":<pathB>}, ... ]

        USEFUL FOR DECLARING HIGH-LEVEL DIMENSIONS, AND RELIEVING LOW LEVEL PATH PAIRS
        """
        if isinstance(edge, basestring):
            e = self[edge]
            if e:
                domain = e.getDomain()
                fields = domain.dimension.fields

                if isinstance(fields, list):
                    if len(fields) == 1:
                        return [{"value": fields[0]}]
                    else:
                        return [{"name": (edge + "[" + str(i) + "]"), "value": v} for i, v in enumerate(fields)]
                elif isinstance(fields, Mapping):
                    return [{"name": (edge + "." + k), "value": v} for k, v in fields.items()]
                else:
                    Log.error("do not know how to handle")

            return [{
                        "name": edge,
                        "value": edge
                    }]
        else:
            return [{
                        "name": coalesce(edge.name, edge.value),
                        "value": edge.value
                    }]


    def update(self, command):
        """
        EXPECTING command == {"set":term, "where":where}
        THE set CLAUSE IS A DICT MAPPING NAMES TO VALUES
        THE where CLAUSE IS AN ES FILTER
        """
        command = wrap(command)
        schema = self._es.get_schema()

        # GET IDS OF DOCUMENTS
        results = self._es.search({
            "fields": listwrap(schema._routing.path),
            "query": {"filtered": {
                "query": {"match_all": {}},
                "filter": qb_expression(command.where).to_esfilter()
            }},
            "size": 200000
        })

        # SCRIPT IS SAME FOR ALL (CAN ONLY HANDLE ASSIGNMENT TO CONSTANT)
        scripts = DictList()
        for k, v in command.set.items():
            if not is_keyword(k):
                Log.error("Only support simple paths for now")
            if isinstance(v, Mapping) and v.doc:
                scripts.append({"doc": v.doc})
            else:
                scripts.append({"script": "ctx._source." + k + " = " + qb_expression(v).to_ruby()})

        if results.hits.hits:
            updates = []
            for h in results.hits.hits:
                for s in scripts:
                    updates.append({"update": {"_id": h._id, "_routing": unwraplist(h.fields[literal_field(schema._routing.path)])}})
                    updates.append(s)
            content = ("\n".join(convert.value2json(c) for c in updates) + "\n").encode('utf-8')
            response = self._es.cluster.post(
                self._es.path + "/_bulk",
                data=content,
                headers={"Content-Type": "application/json"}
            )
            if response.errors:
                Log.error("could not update: {{error}}", error=[e.error for i in response["items"] for e in i.values() if e.status not in (200, 201)])

