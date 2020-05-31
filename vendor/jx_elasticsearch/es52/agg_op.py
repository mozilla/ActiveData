# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from collections import deque

from jx_base.domains import SetDomain
from jx_base.expressions import NULL, Variable as Variable_
from jx_base.language import is_op
from jx_base.query import DEFAULT_LIMIT
from jx_elasticsearch.es52 import agg_format
from jx_elasticsearch.es52.agg_format import agg_formatters
from jx_elasticsearch.es52.agg_op_field import agg_field
from jx_elasticsearch.es52.agg_op_formula import agg_formula
from jx_elasticsearch.es52.decoders import AggsDecoder
from jx_elasticsearch.es52.es_query import Aggs, FilterAggs, NestedAggs, simplify
from jx_elasticsearch.es52.expressions import AndOp, ES52, split_expression_by_path
from jx_elasticsearch.es52.painless import Painless
from jx_python import jx
from mo_dots import Data, Null, coalesce, listwrap, literal_field, unwrap, unwraplist, to_data
from mo_future import first, next
from mo_logs import Log
from mo_times.timer import Timer

DEBUG = False



def is_aggsop(es, query):
    if query.edges or query.groupby or any(a != None and a != "none" for a in listwrap(query.select).aggregate):
        return True
    return False


def get_decoders_by_path(query):
    """
    RETURN MAP FROM QUERY PATH TO LIST OF DECODER ARRAYS

    :param query:
    :return:
    """
    schema = query.frum.schema
    output = {}

    if query.edges:
        if query.sort and query.format != "cube":
            # REORDER EDGES/GROUPBY TO MATCH THE SORT
            query.edges = sort_edges(query, "edges")
    elif query.groupby:
        if query.sort and query.format != "cube":
            query.groupby = sort_edges(query, "groupby")

    for edge in to_data(coalesce(query.edges, query.groupby, [])):
        limit = coalesce(edge.domain.limit, query.limit, DEFAULT_LIMIT)
        if edge.value != None and not edge.value is NULL:
            edge = edge.copy()
            vars_ = edge.value.vars()
            for v in vars_:
                if not schema.leaves(v.var):
                    Log.error("{{var}} does not exist in schema", var=v)
        elif edge.range:
            vars_ = edge.range.min.vars() | edge.range.max.vars()
            for v in vars_:
                if not schema[v.var]:
                    Log.error("{{var}} does not exist in schema", var=v)
        elif edge.domain.dimension:
            vars_ = edge.domain.dimension.fields
            edge.domain.dimension = edge.domain.dimension.copy()
            edge.domain.dimension.fields = [schema[v].es_column for v in vars_]
        elif all(edge.domain.partitions.where):
            vars_ = set()
            for p in edge.domain.partitions:
                vars_ |= p.where.vars()

        vars_ |= edge.value.vars()
        depths = set(c.nested_path[0] for v in vars_ for c in schema.leaves(v.var))
        if not depths:
            Log.error(
                "Do not know of column {{column}}",
                column=unwraplist([v for v in vars_ if schema[v] == None])
            )
        if len(depths) > 1:
            Log.error("expression {{expr|quote}} spans tables, can not handle", expr=edge.value)

        decoder = AggsDecoder(edge, query, limit)
        output.setdefault(first(depths), []).append(decoder)
    return output


def sort_edges(query, prop):
    ordered_edges = []
    remaining_edges = getattr(query, prop)
    for s in jx.reverse(query.sort):
        for e in remaining_edges:
            if e.value == s.value:
                if isinstance(e.domain, SetDomain):
                    pass  # ALREADY SORTED?
                else:
                    e.domain.sort = s.sort
                ordered_edges.append(e)
                remaining_edges.remove(e)
                break
        else:
            Log.error("Can not sort by {{expr}}, can only sort by an existing edge expression", expr=s.value)

    ordered_edges.extend(remaining_edges)
    for i, o in enumerate(ordered_edges):
        o.dim = i  # REORDER THE EDGES
    return ordered_edges


def extract_aggs(select, query_path, schema):
    """
    RETURN ES AGGREGATIONS
    """

    new_select = Data()  # MAP FROM canonical_name (USED FOR NAMES IN QUERY) TO SELECT MAPPING
    formula = []
    for s in select:
        if is_op(s.value, Variable_):
            s.query_path = query_path
            if s.aggregate == "count":
                new_select["count_"+literal_field(s.value.var)] += [s]
            else:
                new_select[literal_field(s.value.var)] += [s]
        elif s.aggregate:
            op, split_select = split_expression_by_path(s.value, schema, lang=Painless)
            for si_key, si_value in split_select.items():
                if si_value:
                    if s.query_path:
                        Log.error("can not handle more than one depth per select")
                    s.query_path = si_key
            formula.append(s)

    acc = Aggs()
    agg_field(acc, new_select, query_path, schema)
    agg_formula(acc, formula, query_path, schema)
    return acc


def build_es_query(select, query_path, schema, query):
    acc = extract_aggs(select, query_path, schema)
    acc = NestedAggs(query_path).add(acc)
    split_decoders = get_decoders_by_path(query)
    op, split_wheres = split_expression_by_path(query.where, schema=schema, lang=ES52)
    start = 0
    decoders = [None] * (len(query.edges) + len(query.groupby))
    paths = list(reversed(sorted(set(split_wheres.keys()) | set(split_decoders.keys()))))
    for path in paths:
        decoder = split_decoders.get(path, Null)
        where = split_wheres.get(path, Null)

        for d in decoder:
            decoders[d.edge.dim] = d
            acc = d.append_query(path, acc)
            start += d.num_columns

        if where:
            acc = FilterAggs("_filter", where, None).add(acc)
        acc = NestedAggs(path).add(acc)
    acc = NestedAggs('.').add(acc)
    acc = simplify(acc)
    es_query = to_data(acc.to_es(schema))
    es_query.size = 0
    return acc, decoders, es_query


def es_aggsop(es, frum, query):
    query = query.copy()  # WE WILL MARK UP THIS QUERY
    schema = frum.schema
    query_path = schema.query_path[0]
    selects = listwrap(query.select)

    acc, decoders, es_query = build_es_query(selects, query_path, schema, query)

    with Timer("ES query time", verbose=DEBUG) as es_duration:
        result = es.search(es_query)

    # Log.note("{{result}}", result=result)

    try:
        format_time = Timer("formatting", verbose=DEBUG)
        with format_time:
            # result.aggregations.doc_count = coalesce(result.aggregations.doc_count, result.hits.total)
            # IT APPEARS THE OLD doc_count IS GONE
            aggs = unwrap(result.aggregations)

            edges_formatter, groupby_formatter, value_fomratter, mime_type = agg_formatters[query.format]
            if query.edges:
                output = edges_formatter(aggs, acc, query, decoders, selects)
            elif query.groupby:
                output = groupby_formatter(aggs, acc, query, decoders, selects)
            else:
                output = value_fomratter(aggs, acc, query, decoders, selects)

        output.meta.timing.formatting = format_time.duration
        output.meta.timing.es_search = es_duration.duration
        output.meta.content_type = mime_type
        output.meta.es_query = es_query
        return output
    except Exception as e:
        if query.format not in agg_formatters:
            Log.error("Format {{format|quote}} not supported yet", format=query.format, cause=e)
        Log.error("Some problem", cause=e)


EMPTY = {}
EMPTY_LIST = []


def drill(agg):
    while True:
        deeper = agg.get("_filter")
        if deeper:
            agg = deeper
            continue
        return agg


def _children(agg, children):
    for child in children:
        name = child.name
        if name is None:
            yield None, agg, child, None
            continue

        v = agg[name]
        if name == "_match":
            for i, b in enumerate(v.get("buckets", EMPTY_LIST)):
                yield i, b, child, b
        elif name.startswith("_match"):
            i = int(name[6:])
            yield i, v, child, v
        elif name.startswith("_missing"):
            if len(name) == 8:
                i = None
            else:
                i = int(name[8:])
            yield None, v, child, v
        else:
            yield None, v, child, None


def aggs_iterator(aggs, es_query, decoders, give_me_zeros=False):
    """
    DIG INTO ES'S RECURSIVE aggs DATA-STRUCTURE:
    RETURN AN ITERATOR OVER THE EFFECTIVE ROWS OF THE RESULTS

    :param aggs: ES AGGREGATE OBJECT
    :param es_query: THE ABSTRACT ES QUERY WE WILL TRACK ALONGSIDE aggs
    :param decoders: TO CONVERT PARTS INTO COORDINATES
    """
    coord = [0] * len(decoders)
    parts = deque()
    stack = []

    gen = _children(aggs, es_query.children)
    while True:
        try:
            index, c_agg, c_query, part = next(gen)
        except StopIteration:
            try:
                gen = stack.pop()
            except IndexError:
                return
            parts.popleft()
            continue

        if c_agg.get('doc_count') == 0 and not give_me_zeros:
            continue
        parts.appendleft(part)
        for d in c_query.decoders:
            coord[d.edge.dim] = d.get_index(tuple(p for p in parts if p is not None), c_query, index)

        children = c_query.children
        selects = c_query.selects
        if selects or not children:
            parts.popleft()  # c_agg WAS ON TOP
            yield (
                tuple(p for p in parts if p is not None),
                tuple(coord),
                c_agg,
                selects
            )
            continue

        stack.append(gen)
        gen = _children(c_agg, children)


def count_dim(aggs, es_query, decoders):
    if not any(hasattr(d, "done_count") for d in decoders):
        return [d.edge for d in decoders]

    def _count_dim(parts, aggs, es_query):
        children = es_query.children
        if not children:
            return

        for child in children:
            name = child.name
            if not name:
                if aggs.get('doc_count') != 0:
                    _count_dim(parts, aggs, child)
                continue

            agg = aggs[name]
            if agg.get('doc_count') == 0:
                continue
            elif name == "_match":
                for i, b in enumerate(agg.get("buckets", EMPTY_LIST)):
                    if not b.get('doc_count'):
                        continue
                    b["_index"] = i
                    new_parts = (b,) + parts
                    for d in child.decoders:
                        d.count(new_parts)
                    _count_dim(new_parts, b, child)
            elif name.startswith("_missing"):
                new_parts = (agg,) + parts
                for d in child.decoders:
                    d.count(new_parts)
                _count_dim(new_parts, agg, child)
            else:
                _count_dim(parts, agg, child)

    _count_dim(tuple(), aggs, es_query)
    for d in decoders:
        done_count = getattr(d, "done_count", Null)
        done_count()
    return [d.edge for d in decoders]


# EXPORT
agg_format.aggs_iterator = aggs_iterator
agg_format.count_dim = count_dim
