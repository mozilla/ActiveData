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

from pyLibrary.collections import MAX
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import listwrap, Dict, wrap, literal_field, set_default, nvl, Null
from pyLibrary.queries import es_query_util, qb
from pyLibrary.queries.domains import PARTITION, SimpleSetDomain
from pyLibrary.queries.es_query_util import aggregates1_4
from pyLibrary.queries.filters import simplify_esfilter
from pyLibrary.times.timer import Timer


def is_aggsop(es, query):
    es.cluster.get_metadata()
    if es.cluster.version.startswith("1.4") and (query.edges or query.groupby):
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

    if query.where:
        filter = simplify_esfilter(query.where)
        esQuery = Dict(
            aggs={"_filter": set_default({"filter": filter}, esQuery)}
        )

    es_duration = Timer("ES query time")
    with es_duration:
        result = es_query_util.post(es, esQuery, query.limit)

    try:
        formatter, mime_type = format_dispatch[query.format]
        output = formatter(decoders, result.aggregations, start, query, select)
        output.meta.es_response_time = es_duration.duration.seconds
        output.meta.content_type = mime_type
        return output
    except Exception, e:
        if query.format not in format_dispatch:
            Log.error("Format {{format|quote}} not supported yet", {"format": query.format}, e)
        Log.error("Some problem", e)


class AggsDecoder(object):
    def __new__(cls, *args, **kwargs):
        e = args[0]
        if e.value and e.domain.type == "default":
            return object.__new__(DefaultDecoder, e.copy())
        if e.value and e.domain.type in PARTITION:
            return object.__new__(SetDecoder, e)
        if e.value and e.domain.type == "time":
            return object.__new__(TimeDecoder, e)
        elif not e.value and e.domain.dimension.fields:
            # THIS domain IS FROM A dimension THAT IS A SIMPLE LIST OF fields
            # JUST PULL THE FIELDS
            fields = e.domain.dimension.fields
            if isinstance(fields, dict):
                Log.error("Not supported yet")
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

    def get_value(self, index):
        Log.error("Not implemented")

    def get_index(self, row):
        Log.error("Not implemented")

    @property
    def num_columns(self):
        return 0


class SetDecoder(AggsDecoder):
    def append_query(self, esQuery, start):
        self.start = start
        return wrap({"aggs": {
            "_match": set_default({"terms": {"field": self.edge.value}}, esQuery),
            "_missing": set_default({"missing": {"field": self.edge.value}}, esQuery),
        }})

    def get_value(self, index):
        return self.edge.domain.getKeyByIndex(index)

    def get_index(self, row):
        try:
            part = row[self.start]
            if part == None:
                return len(self.edge.domain.partitions)
            return self.edge.domain.getIndexByKey(part.key)
        except Exception, e:
            Log.error("problem", e)

    @property
    def num_columns(self):
        return 1


class TimeDecoder(AggsDecoder):
    def append_query(self, esQuery, start):
        self.start = start
        domain = self.edge.domain

        # USE RANGES
        _min = nvl(domain.min, MAX(domain.partitions.min))
        _max = nvl(domain.max, MAX(domain.partitions.max))

        return wrap({"aggs": {
            "_match": set_default(
                {"range": {
                    "field": self.edge.value,
                    "ranges": [{"from": p.min.unix, "to": p.max.unix} for p in domain.partitions]
                }},
                esQuery
            ),
            "_missing": set_default(
                {"filter": {"or": [
                    {"range": {self.edge.value: {"lt": _min.unix}}},
                    {"range": {self.edge.value: {"gte": _max.unix}}},
                    {"missing": {"field": self.edge.value}}
                ]}},
                esQuery
            ),
        }})

        # histogram BREAKS WHEN USING extended_bounds (OOM), WE NEED BOUNDS TO CONTROL EDGES
        # return wrap({"aggs": {
        #     "_match": set_default(
        #         {"histogram": {
        #             "field": self.edge.value,
        #             "interval": domain.interval.unix,
        #             "min_doc_count": 0,
        #             "extended_bounds": {
        #                 "min": domain.min.unix,
        #                 "max": domain.max.unix,
        #             }
        #         }},
        #         esQuery
        #     ),
        #     "_other": set_default(
        #         {"range": {
        #             "field": self.edge.value,
        #             "ranges": [
        #                 {"to": domain.min.unix},
        #                 {"from": domain.max.unix}
        #             ]
        #         }},
        #         esQuery
        #     ),
        #     "_missing": set_default({"missing": {"field": self.edge.value}}, esQuery),
        # }})

    def get_value(self, index):
        return self.edge.domain.getKeyByIndex(index)

    def get_index(self, row):
        domain = self.edge.domain
        part = row[self.start]
        if part == None:
            return len(domain.partitions)

        f = nvl(part["from"], part.key)
        t = nvl(part.to, part.key)
        if f == None or t == None:
            return len(domain.partitions)
        else:
            for p in domain.partitions:
                if p.min.unix <= f <p.max.unix:
                    return p.dataIndex
        sample = part.copy
        sample.buckets = None
        Log.error("Expecting to find {{part}}", {"part":sample})

    @property
    def num_columns(self):
        return 1


class DefaultDecoder(SetDecoder):
    # FOR DECODING THE default DOMAIN TYPE (UNKNOWN-AT-QUERY-TIME SET OF VALUES)

    def __init__(self, edge):
        AggsDecoder.__init__(self, edge)
        self.edge = self.edge.copy()
        self.edge.allowNulls = False  # SINCE WE DO NOT KNOW THE DOMAIN, WE HAVE NO SENSE OF WHAT IS OUTSIDE THAT DOMAIN, allowNulls==True MAKES NO SENSE
        self.edge.domain.partitions = set()

    def append_query(self, esQuery, start):
        self.start = start
        return wrap({"aggs": {
            "_match": set_default({"terms": {"field": self.edge.value}}, esQuery),
            "_missing": set_default({"missing": {"field": self.edge.value}}, esQuery),
        }})

    def count(self, row):
        part = row[self.start]
        if part == None:
            self.edge.allowNulls = True  # OK! WE WILL ALLOW NULLS
        else:
            self.edge.domain.partitions.add(part.key)

    def done_count(self):
        self.edge.domain = SimpleSetDomain(
            partitions=qb.sort(self.edge.domain.partitions)
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
                "_match": set_default({"terms": {"field": v}}, esQuery),
                "_missing": set_default({"missing": {"field": v}}, esQuery),
            }})

        if self.edge.domain.where:
            filter = simplify_esfilter(self.edge.domain.where)
            esQuery = {"aggs": {"_filter": set_default({"filter": filter}, esQuery)}}

        return esQuery

    def count(self, row):
        part = row[self.start:self.start + len(self.fields):]
        value = tuple(p.key for p in part)
        self.edge.domain.partitions.add(value)

    def done_count(self):
        self.edge.domain = SimpleSetDomain(
            key="value",
            partitions=[{"value": v, "dataIndex": i} for i, v in enumerate(qb.sort(self.edge.domain.partitions, range(len(self.fields))))]
        )

    def get_index(self, row):
        parts = self.edge.domain.partitions
        find = tuple(p.key for p in row[self.start:self.start + self.num_columns:])
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



def aggs_iterator(aggs, decoders):
    """
    DIG INTO ES'S RECURSIVE aggs DATA-STRUCTURE:
    RETURN AN ITERATOR OVER THE EFFECTIVE ROWS OF THE RESULTS
    """
    depth = decoders[-1].start + decoders[-1].num_columns
    parts = [None] * depth

    def _aggs_iterator(aggs, d):
        if aggs._filter:
            aggs = aggs._filter

        if d > 0:
            for b in aggs._match.buckets:
                parts[d] = b
                for a in _aggs_iterator(b, d - 1):
                    yield a
            parts[d] = Null
            for b in aggs._other.buckets:
                for a in _aggs_iterator(b, d - 1):
                    yield a
            b = aggs._missing
            if b.doc_count:
                for a in _aggs_iterator(b, d - 1):
                    yield a
        else:
            for b in aggs._match.buckets:
                parts[d] = b
                if b.doc_count:
                    yield b
            parts[d] = Null
            for b in aggs._other.buckets:
                if b.doc_count:
                    yield b
            b = aggs._missing
            if b.doc_count:
                yield b

    for a in _aggs_iterator(aggs, depth - 1):
        yield parts, a





def count_dim(aggs, decoders):
    if any(isinstance(d, DefaultDecoder) for d in decoders):
        # ENUMERATE THE DOMAINS, IF UNKNOWN AT QUERY TIME
        for row, agg in aggs_iterator(aggs, decoders):
            for d in decoders:
                d.count(row)
        for d in decoders:
            d.done_count()
    new_edges = wrap([d.edge for d in decoders])
    return new_edges


format_dispatch = {}
from pyLibrary.queries.es_query_aggs_format import format_cube

_ = format_cube

