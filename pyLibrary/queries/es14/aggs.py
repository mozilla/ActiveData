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
from pyLibrary.maths import Math
from pyLibrary.queries import qb, es09
from pyLibrary.queries.dimensions import Dimension
from pyLibrary.queries.domains import PARTITION, SimpleSetDomain, is_keyword, DefaultDomain
from pyLibrary.queries.es14.util import aggregates1_4, NON_STATISTICAL_AGGS
from pyLibrary.queries.expressions import simplify_esfilter, qb_expression_to_ruby, get_all_vars
from pyLibrary.queries.query import DEFAULT_LIMIT
from pyLibrary.times.timer import Timer


def is_aggsop(es, query):
    es.cluster.get_metadata()
    if any(map(es.cluster.version.startswith, ["1.4.", "1.5.", "1.6.", "1.7."])) and (query.edges or query.groupby or any(a != None and a != "none" for a in listwrap(query.select).aggregate)):
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
        elif s.value == ".":
            if frum.typed:
                # STATISITCAL AGGS IMPLY $value, WHILE OTHERS CAN BE ANYTHING
                if s.aggregate in NON_STATISTICAL_AGGS:
                    #TODO: HANDLE BOTH $value AND $objects TO COUNT
                    Log.error("do not know how to handle")
                else:
                    s.value = "$value"
                    new_select["$value"] += [s]
            else:
                if s.aggregate in NON_STATISTICAL_AGGS:
                    #TODO:  WE SHOULD BE ABLE TO COUNT, BUT WE MUST *OR* ALL LEAF VALUES TO DO IT
                    Log.error("do not know how to handle")
                else:
                    Log.error('Not expecting ES to have a value at "." which {{agg}} can be applied', agg=s.aggregate)
        elif is_keyword(s.value):
            new_select[literal_field(s.value)] += [s]
        else:
            formula.append(s)

    for canonical_name, many in new_select.items():
        representative = many[0]
        if representative.value == ".":
            Log.error("do not know how to handle")
        else:
            field_name = representative.value

        if len(many) > 1 or many[0].aggregate in ("median", "percentile"):
            # canonical_name=literal_field(many[0].name)
            for s in many:
                if s.aggregate == "count":
                    es_query.aggs[literal_field(canonical_name)].stats.field = field_name
                    s.pull = literal_field(canonical_name) + ".count"
                elif s.aggregate == "median":
                    #ES USES DIFFERENT METHOD FOR PERCENTILES THAN FOR STATS AND COUNT
                    key=literal_field(canonical_name + " percentile")

                    es_query.aggs[key].percentiles.field = field_name
                    es_query.aggs[key].percentiles.percents += [50]
                    s.pull = key + ".values.50\.0"
                elif s.aggregate == "percentile":
                    #ES USES DIFFERENT METHOD FOR PERCENTILES THAN FOR STATS AND COUNT
                    key=literal_field(canonical_name + " percentile")
                    percent = Math.round(s.percentile * 100, decimal=6)

                    es_query.aggs[key].percentiles.field = field_name
                    es_query.aggs[key].percentiles.percents += [percent]
                    s.pull = key + ".values." + literal_field(unicode(percent))
                else:
                    es_query.aggs[literal_field(canonical_name)].stats.field = field_name
                    s.pull = literal_field(canonical_name) + "." + aggregates1_4[s.aggregate]
        else:
            es_query.aggs[literal_field(canonical_name)][aggregates1_4[representative.aggregate]].field = field_name
            representative.pull = literal_field(canonical_name) + ".value"

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
        #TODO: INCLUDE FILTERS ON EDGES

        filter = simplify_esfilter(query.where)
        es_query = Dict(
            aggs={"_filter": set_default({"filter": filter}, es_query)}
        )

    if len(split_field(frum.name)) > 1:
        es_query = wrap({
            "size": 0,
            "aggs": {"_nested": set_default(
                {
                    "nested": {
                        "path": frum.query_path
                    }
                },
                es_query
            )}
        })

    es_query.size=0

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

        output.meta.es_response_time = es_duration.duration
        output.meta.content_type = mime_type
        output.meta.es_query = es_query
        return output
    except Exception, e:
        if query.format not in format_dispatch:
            Log.error("Format {{format|quote}} not supported yet",  format= query.format, cause=e)
        Log.error("Some problem", e)


class AggsDecoder(object):
    def __new__(cls, e=None, query=None, *args, **kwargs):
        if query.groupby:
            # GROUPBY ASSUMES WE IGNORE THE DOMAIN RANGE
            e.allowNulls = False
        else:
            e.allowNulls = coalesce(e.allowNulls, True)

        if e.value and e.domain.type == "default":
            if query.groupby:
                return object.__new__(DefaultDecoder, e.copy())

            if is_keyword(e.value):
                cols = query.frum.get_columns()
                col = cols.filter(lambda c: c.name == e.value)[0]
                if not col:
                    return object.__new__(DefaultDecoder, e.copy())
                limit = coalesce(e.domain.limit, query.limit, DEFAULT_LIMIT)

                if col.partitions != None:
                    e.domain = SimpleSetDomain(partitions=col.partitions[:limit:])
                else:
                    e.domain = set_default(DefaultDomain(limit=limit), e.domain.as_dict())
                    return object.__new__(DefaultDecoder, e.copy())

            elif isinstance(e.value, (list, Mapping)):
                Log.error("Not supported yet")
            else:
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
        domain = self.edge.domain

        include = [p[domain.key] for p in domain.partitions]
        if self.edge.allowNulls:

            return wrap({"aggs": {
                "_match": set_default({"terms": {
                    "field": self.edge.value,
                    "size": 0,
                    "include": include
                }}, es_query),
                "_missing": set_default(
                    {"filter": {"or": [
                        {"missing": {"field": self.edge.value}},
                        {"not": {"terms": {self.edge.value: include}}}
                    ]}},
                    es_query
                ),
            }})
        else:
            return wrap({"aggs": {
                "_match": set_default({"terms": {
                    "field": self.edge.value,
                    "size": 0,
                    "include": include
                }}, es_query)
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

    if edge.allowNulls:
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
        missing_filter = set_default(
            {"filter": {"or": [
                missing_range,
                {"or": [{"missing": {"field": v}} for v in get_all_vars(edge.value)]}
            ]}},
            es_query
        )
    else:
        missing_filter = None

    return wrap({"aggs": {
        "_match": set_default(
            {"range": calc},
            {"range": {"ranges": [{"from": to_float(p.min), "to": to_float(p.max)} for p in domain.partitions]}},
            es_query
        ),
        "_missing": missing_filter
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
        # self.edge.allowNulls = False  # SINCE WE DO NOT KNOW THE DOMAIN, WE HAVE NO SENSE OF WHAT IS OUTSIDE THAT DOMAIN, allowNulls==True MAKES NO SENSE
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
                "_match": set_default({"terms": {
                    "field": v,
                    "size": self.edge.domain.limit
                }}, es_query),
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
                "_match": set_default({"terms": {
                    "field": v,
                    "size": self.edge.domain.limit
                }}, es_query),
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

