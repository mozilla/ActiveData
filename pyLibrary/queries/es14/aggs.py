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

from pyLibrary import convert
from pyLibrary.collections import MAX
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import listwrap, Dict, wrap, literal_field, set_default, coalesce, Null, split_field, DictList, unwrap
from pyLibrary.maths import Math
from pyLibrary.queries import qb, es09
from pyLibrary.queries.dimensions import Dimension
from pyLibrary.queries.domains import PARTITION, SimpleSetDomain, is_keyword, DefaultDomain
from pyLibrary.queries.es14.util import aggregates1_4, NON_STATISTICAL_AGGS
from pyLibrary.queries.expressions import simplify_esfilter, split_expression_by_depth, qb_expression, AndOp, Variable, Literal, OrOp, BinaryOp
from pyLibrary.queries.query import DEFAULT_LIMIT, MAX_LIMIT
from pyLibrary.times.timer import Timer


def is_aggsop(es, query):
    es.cluster.get_metadata()
    if any(map(es.cluster.version.startswith, ["1.4.", "1.5.", "1.6.", "1.7."])) and (query.edges or query.groupby or any(a != None and a != "none" for a in listwrap(query.select).aggregate)):
        return True
    return False


def get_decoders_by_depth(query):
    """
    RETURN A LIST OF DECODER ARRAYS, ONE ARRAY FOR EACH NESTED DEPTH
    """
    schema = query.frum
    output = DictList()
    for e in coalesce(query.edges, query.groupby, []):
        if e.value:
            e = e.copy()
            e.value = qb_expression(e.value)
            vars_ = e.value.vars()

            for v in vars_:
                if not schema[v]:
                    Log.error("{{var}} does not exist in schema", var=v)

            e.value = e.value.map({schema[v].name: schema[v].abs_name for v in vars_})
        else:
            vars_ = e.domain.dimension.fields
            e.domain.dimension = e.domain.dimension.copy()
            e.domain.dimension.fields = [schema[v].abs_name for v in vars_]

        depths = set(len(listwrap(schema[v].nested_path)) for v in vars_)
        if len(depths) > 1:
            Log.error("expression {{expr}} spans tables, can not handle", expr=e.value)
        depth = list(depths)[0]
        while len(output) <= depth:
            output.append([])
        output[depth].append(AggsDecoder(e, query))
    return output


def es_aggsop(es, frum, query):
    select = wrap([s.copy() for s in listwrap(query.select)])

    es_query = Dict()
    new_select = Dict()  #MAP FROM canonical_name (USED FOR NAMES IN QUERY) TO SELECT MAPPING
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
        elif is_keyword(s.value) and s.aggregate=="count":
            s.value = coalesce(frum[s.value].abs_name, s.value)
            new_select["count_"+literal_field(s.value)] += [s]
        elif is_keyword(s.value):
            s.value = coalesce(frum[s.value].abs_name, s.value)
            new_select[literal_field(s.value)] += [s]
        else:
            formula.append(s)

    for canonical_name, many in new_select.items():
        representative = many[0]
        if representative.value == ".":
            Log.error("do not know how to handle")
        else:
            field_name = representative.value

        # canonical_name=literal_field(many[0].name)
        for s in many:
            if s.aggregate == "count":
                es_query.aggs[literal_field(canonical_name)].value_count.field = field_name
                s.pull = literal_field(canonical_name) + ".value"
            elif s.aggregate == "median":
                #ES USES DIFFERENT METHOD FOR PERCENTILES THAN FOR STATS AND COUNT
                key = literal_field(canonical_name + " percentile")

                es_query.aggs[key].percentiles.field = field_name
                es_query.aggs[key].percentiles.percents += [50]
                s.pull = key + ".values.50\.0"
            elif s.aggregate == "percentile":
                #ES USES DIFFERENT METHOD FOR PERCENTILES THAN FOR STATS AND COUNT
                key = literal_field(canonical_name + " percentile")
                percent = Math.round(s.percentile * 100, decimal=6)

                es_query.aggs[key].percentiles.field = field_name
                es_query.aggs[key].percentiles.percents += [percent]
                s.pull = key + ".values." + literal_field(unicode(percent))
            elif s.aggregate == "cardinality":
                #ES USES DIFFERENT METHOD FOR CARDINALITY
                key = literal_field(canonical_name + " cardinality")

                es_query.aggs[key].cardinality.field = field_name
                s.pull = key + ".value"
            else:
                # PULL VALUE OUT OF THE stats AGGREGATE
                es_query.aggs[literal_field(canonical_name)].stats.field = field_name
                s.pull = literal_field(canonical_name) + "." + aggregates1_4[s.aggregate]

    for i, s in enumerate(formula):
        canonical_name = literal_field(s.name)
        abs_value = qb_expression(s.value).map({c.name: c.abs_name for c in frum._columns})

        if s.aggregate == "count":
            es_query.aggs[literal_field(canonical_name)].value_count.script = abs_value.to_ruby()
            s.pull = literal_field(canonical_name) + ".value"
        elif s.aggregate == "median":
            #ES USES DIFFERENT METHOD FOR PERCENTILES THAN FOR STATS AND COUNT
            key = literal_field(canonical_name + " percentile")

            es_query.aggs[key].percentiles.script = abs_value.to_ruby()
            es_query.aggs[key].percentiles.percents += [50]
            s.pull = key + ".values.50\.0"
        elif s.aggregate == "percentile":
            #ES USES DIFFERENT METHOD FOR PERCENTILES THAN FOR STATS AND COUNT
            key = literal_field(canonical_name + " percentile")
            percent = Math.round(s.percentile * 100, decimal=6)

            es_query.aggs[key].percentiles.script = abs_value.to_ruby()
            es_query.aggs[key].percentiles.percents += [percent]
            s.pull = key + ".values." + literal_field(unicode(percent))
        elif s.aggregate == "cardinality":
            #ES USES DIFFERENT METHOD FOR CARDINALITY
            key = canonical_name + " cardinality"

            es_query.aggs[key].cardinality.script = abs_value.to_ruby()
            s.pull = key + ".value"
        else:
            # PULL VALUE OUT OF THE stats AGGREGATE
            s.pull = canonical_name + "." + aggregates1_4[s.aggregate]
            es_query.aggs[canonical_name].stats.script = abs_value.to_ruby()


    decoders = get_decoders_by_depth(query)
    start = 0

    vars_ = qb_expression(query.where).vars()
    map_ = {v: frum[v].abs_name for v in vars_}

    #<TERRIBLE SECTION> THIS IS WHERE WE WEAVE THE where CLAUSE WITH nested
    split_where = split_expression_by_depth(qb_expression(query.where), schema=frum, map_=map_)

    if len(split_field(frum.name)) > 1:
        if any(split_where[2:]):
            Log.error("Where clause is too deep")

        for d in decoders[1]:
            es_query = d.append_query(es_query, start)
            start += d.num_columns

        if split_where[1]:
            #TODO: INCLUDE FILTERS ON EDGES
            filter_ = simplify_esfilter(AndOp("and", split_where[1]).to_esfilter())
            es_query = Dict(
                aggs={"_filter": set_default({"filter": filter_}, es_query)}
            )

        es_query = wrap({
            "aggs": {"_nested": set_default(
                {
                    "nested": {
                        "path": frum.query_path
                    }
                },
                es_query
            )}
        })
    else:
        if any(split_where[1:]):
            Log.error("Where clause is too deep")

    for d in decoders[0]:
        es_query = d.append_query(es_query, start)
        start += d.num_columns

    if split_where[0]:
        #TODO: INCLUDE FILTERS ON EDGES
        filter = simplify_esfilter(AndOp("and", split_where[0]).to_esfilter())
        es_query = Dict(
            aggs={"_filter": set_default({"filter": filter}, es_query)}
        )
    # </TERRIBLE SECTION>

    if not es_query:
        es_query = wrap({"query": {"match_all": {}}})

    es_query.size = 0

    with Timer("ES query time") as es_duration:
        result = es09.util.post(es, es_query, query.limit)

    try:
        decoders = [d for ds in decoders for d in ds]
        result.aggregations.doc_count = coalesce(result.aggregations.doc_count, result.hits.total)  # IT APPEARS THE OLD doc_count IS GONE

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
            Log.error("Format {{format|quote}} not supported yet", format=query.format, cause=e)
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
                return object.__new__(DefaultDecoder, e)

            if isinstance(e.value, basestring):
                Log.error("Not expected anymore")

            if isinstance(e.value, Variable):
                cols = query.frum.get_columns()
                col = cols.filter(lambda c: c.name == e.value.var)[0]
                if not col:
                    return object.__new__(DefaultDecoder, e)
                limit = coalesce(e.domain.limit, query.limit, DEFAULT_LIMIT)

                if col.partitions != None:
                    e.domain = SimpleSetDomain(partitions=col.partitions[:limit:])
                else:
                    e.domain = set_default(DefaultDomain(limit=limit), e.domain.as_dict())
                    return object.__new__(DefaultDecoder, e)

            else:
                return object.__new__(DefaultDecoder, e)

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
                Log.error("No longer allowed: All objects are expressions")
            else:
                return object.__new__(DimFieldListDecoder, e)
        else:
            Log.error("domain type of {{type}} is not supported yet", type=e.domain.type)


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

    def __init__(self, edge, query):
        AggsDecoder.__init__(self, edge, query)
        self.domain = edge.domain


    def append_query(self, es_query, start):
        self.start = start
        domain = self.domain
        field_name = unicode(self.edge.value)

        include = [p[domain.key] for p in domain.partitions]
        if self.edge.allowNulls:

            return wrap({"aggs": {
                "_match": set_default({"terms": {
                    "field": field_name,
                    "size": 0,
                    "include": include
                }}, es_query),
                "_missing": set_default(
                    {"filter": {"or": [
                        {"missing": {"field": field_name}},
                        {"not": {"terms": {field_name: include}}}
                    ]}},
                    es_query
                ),
            }})
        else:
            return wrap({"aggs": {
                "_match": set_default({"terms": {
                    "field": field_name,
                    "size": 0,
                    "include": include
                }}, es_query)
            }})

    def get_value(self, index):
        return self.domain.getKeyByIndex(index)

    def get_value_from_row(self, row):
        return row[self.start].key

    def get_index(self, row):
        try:
            part = row[self.start]
            return self.domain.getIndexByKey(part.key)
        except Exception, e:
            Log.error("problem", cause=e)

    @property
    def num_columns(self):
        return 1


def _range_composer(edge, domain, es_query, to_float):
    # USE RANGES
    _min = coalesce(domain.min, MAX(domain.partitions.min))
    _max = coalesce(domain.max, MAX(domain.partitions.max))

    if isinstance(edge.value, Variable):
        calc = {"field": edge.value.var}
    else:
        calc = {"script": edge.value.to_ruby()}

    if edge.allowNulls:
        if isinstance(edge.value, Variable):
            missing_range = {"or": [
                {"range": {edge.value.var: {"lt": to_float(_min)}}},
                {"range": {edge.value.var: {"gte": to_float(_max)}}}
            ]}
        else:
            missing_range = {"script": {"script": OrOp("or", [
                BinaryOp("lt", [edge.value, Literal(None, to_float(_min))]),
                BinaryOp("gt", [edge.value, Literal(None, to_float(_max))]),
            ]).to_ruby()}}
        missing_filter = set_default(
            {"filter": {"or": [
                missing_range,
                edge.value.missing().to_esfilter()
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
        self.domain = edge.domain
        self.domain.limit =Math.min(coalesce(self.domain.limit, query.limit, 10), MAX_LIMIT)
        self.parts = list()

    def append_query(self, es_query, start):
        self.start = start

        if not isinstance(self.edge.value, Variable):
            script_field = self.edge.value.to_ruby()
            missing = self.edge.value.missing().to_esfilter()

            output = wrap({"aggs": {
                "_match": set_default(
                    {"terms": {
                        "script_field": script_field,
                        "size": self.domain.limit
                    }},
                    es_query
                ),
                "_missing": set_default({"filter": missing}, es_query)
            }})
            return output

        output = wrap({"aggs": {
            "_match": set_default(
                {"terms": {
                    "field": self.edge.value.var,
                    "size": self.domain.limit
                }},
                es_query
            ),
            "_missing": set_default({"missing": {"field": self.edge.value}}, es_query)
        }})
        return output

    def count(self, row):
        part = row[self.start]
        if part == None:
            self.edge.allowNulls = True  # OK! WE WILL ALLOW NULLS
        else:
            self.parts.append(part.key)

    def done_count(self):
        self.edge.domain = self.domain = SimpleSetDomain(
            partitions=qb.sort(set(self.parts))
        )
        self.parts = None

    @property
    def num_columns(self):
        return 1


class DimFieldListDecoder(SetDecoder):
    def __init__(self, edge, query):
        AggsDecoder.__init__(self, edge, query)
        self.fields = edge.domain.dimension.fields
        self.domain = self.edge.domain
        self.domain.limit =Math.min(coalesce(self.domain.limit, query.limit, 10), MAX_LIMIT)
        self.parts = list()


    def append_query(self, es_query, start):
        #TODO: USE "reverse_nested" QUERY TO PULL THESE

        self.start = start
        for i, v in enumerate(self.fields):
            nest = wrap({"aggs": {
                "_match": set_default({"terms": {
                    "field": v,
                    "size": self.domain.limit
                }}, es_query)
            }})
            if self.edge.allowNulls:
                nest.aggs._missing = set_default({"missing": {"field": v}}, es_query)
            es_query = nest

        if self.domain.where:
            filter = simplify_esfilter(self.domain.where)
            es_query = {"aggs": {"_filter": set_default({"filter": filter}, es_query)}}

        return es_query

    def count(self, row):
        part = row[self.start:self.start + len(self.fields):]
        value = tuple(p.key for p in part)
        self.parts.append(value)

    def done_count(self):
        columns = map(unicode, range(len(self.fields)))
        parts = wrap([{unicode(i): p for i, p in enumerate(part)} for part in set(self.parts)])
        self.parts = None
        sorted_parts = qb.sort(parts, columns)

        self.edge.domain = self.domain = SimpleSetDomain(
            key="value",
            partitions=[{"value": tuple(v[k] for k in columns), "dataIndex": i} for i, v in enumerate(sorted_parts)]
        )

    def get_index(self, row):
        find = tuple(p.key for p in row[self.start:self.start + self.num_columns:])
        return self.domain.getIndexByKey(find)

    @property
    def num_columns(self):
        return len(self.fields)


EMPTY = {}
EMPTY_LIST = []


def drill(agg):
    deeper = coalesce(agg.get("_filter"), agg.get("_nested"))
    while deeper:
        agg = deeper
        deeper = coalesce(agg.get("_filter"), agg.get("_nested"))
    return agg


def aggs_iterator(aggs, decoders):
    """
    DIG INTO ES'S RECURSIVE aggs DATA-STRUCTURE:
    RETURN AN ITERATOR OVER THE EFFECTIVE ROWS OF THE RESULTS
    """
    depth = max(d.start + d.num_columns for d in decoders)
    parts = [None] * depth

    def _aggs_iterator(agg, d):
        agg = drill(agg)

        if d > 0:
            for b in agg.get("_match", EMPTY).get("buckets", EMPTY_LIST):
                parts[d] = wrap(b)
                for a in _aggs_iterator(b, d - 1):
                    yield a
            parts[d] = Null
            for b in agg.get("_other", EMPTY).get("buckets", EMPTY_LIST):
                for a in _aggs_iterator(b, d - 1):
                    yield a
            b = drill(agg.get("_missing", EMPTY))
            if b.get("doc_count"):
                for a in _aggs_iterator(b, d - 1):
                    yield a
        else:
            for b in agg.get("_match", EMPTY).get("buckets", EMPTY_LIST):
                parts[d] = wrap(b)
                b = drill(b)
                if b.get("doc_count"):
                    yield b
            parts[d] = Null
            for b in agg.get("_other", EMPTY).get("buckets", EMPTY_LIST):
                b = drill(b)
                if b.get("doc_count"):
                    yield b
            b = drill(agg.get("_missing", EMPTY))
            if b.get("doc_count"):
                yield b

    for a in _aggs_iterator(unwrap(aggs), depth - 1):
        yield parts, wrap(a)


def count_dim(aggs, decoders):
    if any(isinstance(d, (DefaultDecoder, DimFieldListDecoder)) for d in decoders):
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

