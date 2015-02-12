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
from copy import copy

from pyLibrary.collections.matrix import Matrix
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import listwrap, Dict, wrap, literal_field, set_default, nvl
from pyLibrary.queries import es_query_util, Q
from pyLibrary.queries.Q import accumulate
from pyLibrary.queries.cube import Cube
from pyLibrary.queries.domains import PARTITION, SimpleSetDomain



# THE NEW AND FANTASTIC AGGS OPERATION IN ELASTICSEARCH!
# WE ALL WIN NOW!
from pyLibrary.queries.es_query_util import aggregates1_4
from pyLibrary.queries.filters import simplify_esfilter, where2esfilter


def is_aggsop(es, query):
    if es.cluster.version.startswith("1.4") and query.edges:
        return True
    return False


def es_aggsop(es, mvel, query):
    select = listwrap(query.select)

    esQuery = Dict()
    for s in select:
        if s.aggregate == "count" and s.value:
            esQuery.aggs[literal_field(s.name)].value_count.field = s.value
            # esQuery.aggs["missing_"+literal_field(s.name)].missing.field = s.value
        elif s.aggregate == "count":
            pass
        else:
            esQuery.aggs[literal_field(s.name)][aggregates1_4[s.aggregate]].field = s.value

    decoders = [AggsDecoder(e) for e in query.edges]
    start = 0
    for d in decoders:
        esQuery = d.append_query(esQuery, start)
        start += d.num_columns

    esQuery.size = nvl(query.limit, 0)
    esQuery.filter = simplify_esfilter(query.where)
    result = es_query_util.post(es, esQuery, query.limit)

    if query.format == "cube":
        if any(isinstance(d, DefaultDecoder) for d in decoders):
            # ENUMERATE THE DOMAINS, IF UNKNOWN AT QUERY TIME
            for row, agg in aggs_iterator(result.aggregations, start):
                if agg.doc_count:  # DO NOT COUNT STUFF THAT HAS NO PRESENCE
                    for d in decoders:
                        d.count(row)
            for d in decoders:
                d.done_count()

        new_edges = [d.edge for d in decoders]
        dims = tuple(len(e.domain.partitions) + (0 if e.allowNulls is False else 1) for e in new_edges)
        matricies = [(s, Matrix(dims=dims, zeros=(s.aggregate == "count"))) for s in select]

        for row, agg in aggs_iterator(result.aggregations, start):
            coord = tuple(d.get_part(row) for d in decoders)
            for s, m in matricies:
                # name = literal_field(s.name)
                if s.aggregate == "count" and s.value == None:
                    m[coord] = agg.doc_count
                else:
                    if m[coord] != None:
                        Log.error("Not expected")
                    m[coord] = agg[s.name].value

        cube = Cube(query.select, new_edges, {s.name: m for s, m in matricies})
        cube.frum = query
        return cube
    elif query.format == "table":
        new_edges = count_dims(result.aggregations, decoders)
        header = new_edges.name + select.name

        data = []
        for row, agg in aggs_iterator(result.aggregations, start):
            output = copy(row)
            for s in select:
                if s.aggregate == "count" and s.value == None:
                    output.append(agg.doc_count)
                else:
                    output.append(agg[literal_field(s.name)].value)
            data.append(output)
        return {'header': header, "data": data}

    elif query.format == "list":
        new_edges = count_dims(result.aggregations, decoders)
        data = []
        for row, agg in aggs_iterator(result.aggregations, start):
            output = {e.name: r for e, r in zip(new_edges, row)}

            for s in select:
                if s.aggregate == "count" and s.value == None:
                    output[literal_field(s.name)] = agg.doc_count
                else:
                    output[literal_field(s.name)] = agg[literal_field(s.name)].value
            data.append(output)
        return data
    else:
        Log.error("Format {{format|quote}} not supported yet", {"format": query.format})


def count_dims(aggs, decoders):
    new_edges = []
    for d in decoders:
        if d.edge.domain.type == "default":
            d.edge = copy(d.edge)
            d.edge.domain.partitions = set()
        new_edges.append(d.edge)

    _count_dims(aggs, decoders, len(decoders) - 1)

    # REWRITE THE DOMAINS TO REAL DOMAIN OBJECTS
    for e in new_edges:
        if e.domain.type == "default":
            e.domain = SimpleSetDomain(
                key="value",
                partitions=[{"value": v} for i, v in enumerate(Q.sort(e.domain.partitions))]
            )

    return wrap(new_edges)


def _count_dims(aggs, decoders, rem):
    d = decoders[rem]
    buckets = aggs[literal_field(str(d.start))].buckets

    if isinstance(d, DefaultDecoder):
        domain = d.edge.domain
        domain.partitions |= set(buckets.key)

    if rem > 0:
        for b in buckets:
            _count_dims(b, decoders, rem - 1)


class AggsDecoder(object):
    def __new__(cls, *args, **kwargs):
        e = args[0]
        if e.value and e.domain.type == "default":
            return object.__new__(DefaultDecoder, e.copy())
        if e.value and e.domain.type in PARTITION:
            return object.__new__(SimpleDecoder, e)
        elif not e.value and e.domain.dimension.fields:
            # THIS domain IS FROM A dimension THAT IS A SIMPLE LIST OF fields
            # JUST PULL THE FIELDS
            fields = e.domain.dimension.fields
            if isinstance(fields, dict):
                return object.__new__(DimFieldDictDecoder, e)
            else:
                return object.__new__(DimFieldListDecoder, e)
        else:
            Log.error("domain type of {{type}} is not supported yet", {"type": e.domain.type})


    def __init__(self, edge):
        self.start = None
        self.edge = edge
        self.name = literal_field(self.edge.name)

    def append_query(self, esQuery, start):
        Log.error("Not supported")

    def count(self, row):
        pass

    def done_count(self):
        pass

    def get_part(self, row):
        Log.error("Not supported")

    @property
    def num_columns(self):
        return 0


class SimpleDecoder(AggsDecoder):
    def append_query(self, esQuery, start):
        self.start = start
        esQuery.terms = {"field": self.edge.value}
        return wrap({"aggs": {str(start): esQuery}})

    def get_part(self, row):
        return self.edge.domain.getIndexByKey(row[self.start])

    @property
    def num_columns(self):
        return 1


class DefaultDecoder(AggsDecoder):
    # FOR DECODING THE default DOMAIN TYPE (UNKNOWN-AT-QUERY-TIME SET OF VALUES)

    def __init__(self, edge):
        AggsDecoder.__init__(self, edge)
        self.edge = self.edge.copy()
        self.edge.domain.partitions = set()

    def append_query(self, esQuery, start):
        self.start = start
        counter = esQuery.copy()
        missing = esQuery.copy()
        counter.terms = {"field": self.edge.value}
        missing.missing = {"field": self.edge.value}

        return wrap({"aggs": {
            str(start): counter,
            str(start) + "_missing": missing
        }})

    def get_part(self, row):
        return self.edge.domain.getIndexByKey(row[self.start])

    def count(self, row):
        v = row[self.start]
        if v != None:
            self.edge.domain.partitions.add(v)

    def done_count(self):
        self.edge.domain = SimpleSetDomain(
            key="value",
            partitions=[{"value": v} for i, v in enumerate(Q.sort(self.edge.domain.partitions))]
        )

    @property
    def num_columns(self):
        return 1


class DimFieldListDecoder(DefaultDecoder):
    def __init__(self, edge):
        DefaultDecoder.__init__(self, edge)
        self.fields = edge.domain.dimension.fields

    def append_query(self, esQuery, start):
        self.start = start
        for i, v in enumerate(self.fields):
            esQuery = wrap({"aggs": {
                str(start + i): set_default({"terms": {"field": v}}, esQuery),
                str(start + i) + "_missing": set_default({"missing": {"field": v}}, esQuery),
            }})

        filter = simplify_esfilter(self.edge.domain.where)
        if not isinstance(filter.match_all, dict):
            Log.error("Extra depth needed into the aggs")
        return esQuery

    def count(self, row):
        self.edge.domain.partitions.add(tuple(row[self.start:self.start + len(self.fields):]))

    def done_count(self):
        self.edge.domain = SimpleSetDomain(
            key="value",
            partitions=[{"value": v, "dataIndex": i} for i, v in enumerate(Q.sort(self.edge.domain.partitions, range(len(self.fields))))]
        )

    def get_part(self, row):
        parts = self.edge.domain.partitions
        find = tuple(row[self.start:self.start + self.num_columns:])
        for p in parts:
            if p.value == find:
                return p.dataIndex
        else:
            return len(parts)

    def _get_sub(self, aggs, coord):
        domain = self.edge.domain
        buckets = aggs[self.name].buckets
        for b in buckets:
            c = domain.getIndexByKey(b.key)
            yield (c, b)

    @property
    def num_columns(self):
        return len(self.fields)


class DimFieldDictDecoder(DefaultDecoder):
    def __init__(self, edge):
        DefaultDecoder.__init__(self, edge)
        self.fields = Q.sort(edge.domain.dimension.fields.items(), 0)

    def append_query(self, esQuery, start):
        self.start = start
        for i, (k, v) in enumerate(self.fields):
            esQuery.terms = {"field": v}
        esQuery.filter = simplify_esfilter(self.edge.domain.esfilter)
        return esQuery

    def get_part(self, row):
        # coord IS NOW SET, WHICH PART IS IT?
        part = Dict()
        for i, (k, v) in enumerate(self.fields):
            part[v] = row[self.start + i]

        c = self.edge.domain.getIndexByPart(part)
        return c

    @property
    def num_columns(self):
        return len(self.fields)


def aggs_iterator(aggs, depth):
    """
    DIG INTO ES'S RECURSIVE aggs DATA-STRUCTURE:
    RETURN AN ITERATOR OVER THE EFFECTIVE ROWS OF THE RESULTS
    """
    coord = [None] * depth

    def _aggs_iterator(aggs, d):
        if d > 0:
            for b in aggs[str(d)].buckets:
                coord[d] = b.key
                for a in _aggs_iterator(b, d - 1):
                    yield a
            coord[d] = None
            for a in _aggs_iterator(aggs[str(d) + "_missing"], d - 1):
                yield a
        else:
            for b in aggs[str(d)].buckets:
                coord[d] = b.key
                yield b
            coord[d] = None
            yield aggs[str(d) + "_missing"]

    for a in _aggs_iterator(aggs, depth - 1):
        yield coord, a


