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
from collections import Mapping

from pyLibrary.collections import MAX
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import listwrap, Dict, wrap, literal_field, set_default, coalesce, Null, split_field, join_field
from pyLibrary.queries import qb, es09
from pyLibrary.queries.dimensions import Dimension
from pyLibrary.queries.domains import PARTITION, SimpleSetDomain, is_keyword
from pyLibrary.queries.es14.util import aggregates1_4
from pyLibrary.queries.expressions import simplify_esfilter, qb_expression_to_ruby, get_all_vars
from pyLibrary.times.timer import Timer


def is_aggsop(es, query):
    es.cluster.get_metadata()
    if (es.cluster.version.startswith("1.4.") or es.cluster.version.startswith("1.5.")) and (query.edges or query.groupby or any(a != None and a != "none" for a in listwrap(query.select).aggregate)):
        return True
    return False


def es_aggsop(es, frum, query):
    select = listwrap(query.select)

    es_query = Dict()
    new_select = Dict()
    formula = []
    for s in select:
        if s.aggregate == "count" and (s.value == None or s.value == "."):
            s.pull = "doc_count"
        elif is_keyword(s.value):
            new_select[literal_field(s.value)] += [s]
        else:
            formula.append(s)

    for litral_field, many in new_select.items():
        if len(many)>1:
            canonical_name=literal_field(many[0].name)
            es_query.aggs[canonical_name].stats.field = many[0].value
            for s in many:
                if s.aggregate == "count":
                    s.pull = canonical_name + ".count"
                else:
                    s.pull = canonical_name + "." + aggregates1_4[s.aggregate]
        else:
            s = many[0]
            s.pull = literal_field(s.value) + ".value"
            es_query.aggs[literal_field(s.value)][aggregates1_4[s.aggregate]].field = s.value

    for i, s in enumerate(formula):
        new_select[unicode(i)] = s
        s.pull = literal_field(s.name) + ".value"
        es_query.aggs[literal_field(s.name)][aggregates1_4[s.aggregate]].script = qb_expression_to_ruby(s.value)

    decoders = [AggsDecoder(e, query) for e in coalesce(query.edges, query.groupby, [])]
    start = 0
    for d in decoders:
        es_query = d.append_query(es_query, start)
        start += d.num_columns

    if query.where:
        filter = simplify_esfilter(query.where)
        es_query = Dict(
            aggs={"_filter": set_default({"filter": filter}, es_query)}
        )

    if len(split_field(frum.name)) > 1:
        es_query = wrap({
            "size": 0,
            "aggs": {"_nested": set_default({
                "nested": {
                    "path": join_field(split_field(frum.name)[1::])
                }
            }, es_query)}
        })

    with Timer("ES query time") as es_duration:
        result = es09.util.post(es, es_query, query.limit)

    try:
        formatter, groupby_formatter, aggop_formatter, mime_type = format_dispatch[query.format]
        if query.edges:
            output = formatter(decoders, result.aggregations, start, query, select)
        elif query.groupby:
            output = groupby_formatter(decoders, result.aggregations, start, query, select)
        else:
            output = aggop_formatter(decoders, result.aggregations, start, query, select)

        output.meta.es_response_time = es_duration.seconds
        output.meta.content_type = mime_type
        output.meta.es_query = es_query
        return output
    except Exception, e:
        if query.format not in format_dispatch:
            Log.error("Format {{format|quote}} not supported yet",  format= query.format, cause=e)
        Log.error("Some problem", e)


class AggsDecoder(object):
    def __new__(cls, *args, **kwargs):
        e = args[0]
        if e.value and e.domain.type == "default":
            return object.__new__(DefaultDecoder, e.copy())
        if e.value and e.domain.type in PARTITION:
            return object.__new__(SetDecoder, e)
        if isinstance(e.domain.dimension, Dimension):
            e.domain = e.domain.dimension.getDomain()
            return object.__new__(SetDecoder, e)
        if e.value and e.domain.type == "time":
            return object.__new__(TimeDecoder, e)
        if e.value and e.domain.type == "duration":
            return object.__new__(DurationDecoder, e)
        elif e.value and e.domain.type == "range":
            return object.__new__(RangeDecoder, e)
        elif not e.value and e.domain.dimension.fields:
            # THIS domain IS FROM A dimension THAT IS A SIMPLE LIST OF fields
            # JUST PULL THE FIELDS
            fields = e.domain.dimension.fields
            if isinstance(fields, Mapping):
                return object.__new__(DimFieldDictDecoder, e)
            else:
                return object.__new__(DimFieldListDecoder, e)
        else:
            Log.error("domain type of {{type}} is not supported yet",  type= e.domain.type)


    def __init__(self, edge, query):
        self.start = None
        self.edge = edge
        self.name = literal_field(self.edge.name)

    def append_query(self, es_query, start):
        Log.error("Not supported")

    def count(self, row):
        pass

    def done_count(self):
        pass

    def get_value_from_row(self, row):
        Log.error("Not implemented")

    def get_value(self, index):
        Log.error("Not implemented")

    def get_index(self, row):
        Log.error("Not implemented")

    @property
    def num_columns(self):
        return 0


class SetDecoder(AggsDecoder):
    def append_query(self, es_query, start):
        self.start = start
        return wrap({"aggs": {
            "_match": set_default({"terms": {"field": self.edge.value}}, es_query),
            "_missing": set_default({"missing": {"field": self.edge.value}}, es_query),
        }})

    def get_value(self, index):
        return self.edge.domain.getKeyByIndex(index)

    def get_value_from_row(self, row):
        return row[self.start].key

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


def _range_composer(edge, domain, es_query, to_float):
    # USE RANGES
    _min = coalesce(domain.min, MAX(domain.partitions.min))
    _max = coalesce(domain.max, MAX(domain.partitions.max))

    if is_keyword(edge.value):
        calc = {"field": edge.value}
    else:
        calc = {"script": qb_expression_to_ruby(edge.value)}

    if is_keyword(edge.value):
        missing_range = {"or": [
            {"range": {edge.value: {"lt": to_float(_min)}}},
            {"range": {edge.value: {"gte": to_float(_max)}}}
        ]}
    else:
        missing_range = {"script": {"script": qb_expression_to_ruby({"or": [
            {"lt": [edge.value, to_float(_min)]},
            {"gt": [edge.value, to_float(_max)]},
        ]})}}

    return wrap({"aggs": {
        "_match": set_default(
            {"range": calc},
            {"range": {"ranges": [{"from": to_float(p.min), "to": to_float(p.max)} for p in domain.partitions]}},
            es_query
        ),
        "_missing": set_default(
            {"filter": {"or": [
                missing_range,
                {"missing": {"field": get_all_vars(edge.value)}}
            ]}},
            es_query
        ),
    }})


class TimeDecoder(AggsDecoder):
    def append_query(self, es_query, start):
        self.start = start
        return _range_composer(self.edge, self.edge.domain, es_query, lambda x: x.unix)

    def get_value(self, index):
        return self.edge.domain.getKeyByIndex(index)

    def get_index(self, row):
        domain = self.edge.domain
        part = row[self.start]
        if part == None:
            return len(domain.partitions)

        f = coalesce(part["from"], part.key)
        t = coalesce(part.to, part.key)
        if f == None or t == None:
            return len(domain.partitions)
        else:
            for p in domain.partitions:
                if p.min.unix <= f <p.max.unix:
                    return p.dataIndex
        sample = part.copy
        sample.buckets = None
        Log.error("Expecting to find {{part}}",  part=sample)

    @property
    def num_columns(self):
        return 1


class DurationDecoder(AggsDecoder):
    def append_query(self, es_query, start):
        self.start = start
        return _range_composer(self.edge, self.edge.domain, es_query, lambda x: x.seconds)

    def get_value(self, index):
        return self.edge.domain.getKeyByIndex(index)

    def get_index(self, row):
        domain = self.edge.domain
        part = row[self.start]
        if part == None:
            return len(domain.partitions)

        f = coalesce(part["from"], part.key)
        t = coalesce(part.to, part.key)
        if f == None or t == None:
            return len(domain.partitions)
        else:
            for p in domain.partitions:
                if p.min.seconds <= f < p.max.seconds:
                    return p.dataIndex
        sample = part.copy
        sample.buckets = None
        Log.error("Expecting to find {{part}}",  part=sample)

    @property
    def num_columns(self):
        return 1


class RangeDecoder(AggsDecoder):
    def append_query(self, es_query, start):
        self.start = start
        return _range_composer(self.edge, self.edge.domain, es_query, lambda x: x)

    def get_value(self, index):
        return self.edge.domain.getKeyByIndex(index)

    def get_index(self, row):
        domain = self.edge.domain
        part = row[self.start]
        if part == None:
            return len(domain.partitions)

        f = coalesce(part["from"], part.key)
        t = coalesce(part.to, part.key)
        if f == None or t == None:
            return len(domain.partitions)
        else:
            for p in domain.partitions:
                if p.min <= f <p.max:
                    return p.dataIndex
        sample = part.copy
        sample.buckets = None
        Log.error("Expecting to find {{part}}",  part=sample)

    @property
    def num_columns(self):
        return 1


class DefaultDecoder(SetDecoder):
    # FOR DECODING THE default DOMAIN TYPE (UNKNOWN-AT-QUERY-TIME SET OF VALUES)

    def __init__(self, edge, query):
        AggsDecoder.__init__(self, edge, query)
        self.edge = self.edge.copy()
        self.edge.allowNulls = False  # SINCE WE DO NOT KNOW THE DOMAIN, WE HAVE NO SENSE OF WHAT IS OUTSIDE THAT DOMAIN, allowNulls==True MAKES NO SENSE
        self.edge.domain.partitions = set()
        self.edge.domain.limit = coalesce(self.edge.domain.limit, query.limit, 10)

    def append_query(self, es_query, start):
        self.start = start
        return wrap({"aggs": {
            "_match": set_default(
                {"terms": {
                    "field": self.edge.value,
                    "size": self.edge.domain.limit
                }},
                es_query
            ),
            "_missing": set_default({"missing": {"field": self.edge.value}}, es_query),
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
    def __init__(self, edge, query):
        DefaultDecoder.__init__(self, edge, query)
        self.fields = edge.domain.dimension.fields

    def append_query(self, es_query, start):
        self.start = start
        for i, v in enumerate(self.fields):
            es_query = wrap({"aggs": {
                "_match": set_default({"terms": {"field": v}}, es_query),
                "_missing": set_default({"missing": {"field": v}}, es_query),
            }})

        if self.edge.domain.where:
            filter = simplify_esfilter(self.edge.domain.where)
            es_query = {"aggs": {"_filter": set_default({"filter": filter}, es_query)}}

        return es_query

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



class DimFieldDictDecoder(DefaultDecoder):
    def __init__(self, edge, query):
        DefaultDecoder.__init__(self, edge, query)
        self.fields = edge.domain.dimension.fields.items()

    def append_query(self, es_query, start):
        self.start = start
        for i, (k, v) in enumerate(self.fields):
            es_query = wrap({"aggs": {
                "_match": set_default({"terms": {"field": v}}, es_query),
                "_missing": set_default({"missing": {"field": v}}, es_query),
            }})

        if self.edge.domain.where:
            filter = simplify_esfilter(self.edge.domain.where)
            es_query = {"aggs": {"_filter": set_default({"filter": filter}, es_query)}}

        return es_query

    def count(self, row):
        part = row[self.start:self.start + len(self.fields):]
        value = {k: p.key for (k, v), p in zip(self.fields, part)}
        self.edge.domain.partitions.add(value)

    def done_count(self):
        self.edge.domain = SimpleSetDomain(
            key="value",
            partitions=[{"value": v, "dataIndex": i} for i, v in enumerate(qb.sort(self.edge.domain.partitions, [k for k, v in self.fields]))]
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
        return len(self.fields.values())



def aggs_iterator(aggs, decoders):
    """
    DIG INTO ES'S RECURSIVE aggs DATA-STRUCTURE:
    RETURN AN ITERATOR OVER THE EFFECTIVE ROWS OF THE RESULTS
    """
    depth = decoders[-1].start + decoders[-1].num_columns
    parts = [None] * depth

    def _aggs_iterator(agg, d):
        deeper = coalesce(agg._filter, agg._nested)
        while deeper:
            agg = deeper
            deeper = coalesce(agg._filter, agg._nested)

        if d > 0:
            for b in agg._match.buckets:
                parts[d] = b
                for a in _aggs_iterator(b, d - 1):
                    yield a
            parts[d] = Null
            for b in agg._other.buckets:
                for a in _aggs_iterator(b, d - 1):
                    yield a
            b = agg._missing
            if b.doc_count:
                for a in _aggs_iterator(b, d - 1):
                    yield a
        else:
            for b in agg._match.buckets:
                parts[d] = b
                if b.doc_count:
                    yield b
            parts[d] = Null
            for b in agg._other.buckets:
                if b.doc_count:
                    yield b
            b = agg._missing
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
from pyLibrary.queries.es14.format import format_cube

_ = format_cube

