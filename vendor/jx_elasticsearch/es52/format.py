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

from jx_base.expressions import TupleOp
from jx_elasticsearch.es52.aggs import count_dim, aggs_iterator, format_dispatch
from jx_python.containers.cube import Cube
from mo_collections.matrix import Matrix
from mo_dots import Data, set_default, wrap, split_field, coalesce
from mo_future import sort_using_key
from mo_logs import Log
from mo_logs.strings import quote
from pyLibrary import convert


def format_cube(aggs, es_query, query, decoders, selects):
    new_edges = count_dim(aggs, es_query, decoders)

    dims = []
    for e in new_edges:
        if isinstance(e.value, TupleOp):
            e.allowNulls = False

        extra = 0 if e.allowNulls is False else 1
        dims.append(len(e.domain.partitions) + extra)

    dims = tuple(dims)
    matricies = {s.name: Matrix(dims=dims, zeros=s.default) for s in selects}
    for row, coord, agg, select in aggs_iterator(aggs, es_query, decoders):
        if not select:
            continue
        m = matricies[select.name]
        v = select.pull(agg)
        union(m, coord, v, select.aggregate)

    cube = Cube(
        query.select,
        sort_using_key(new_edges, key=lambda e: e.dim),  # ENSURE EDGES ARE IN SAME ORDER AS QUERY
        matricies
    )
    cube.frum = query
    return cube


def _value_drill(agg):
    while True:
        deeper = agg.get("_nested")
        if deeper:
            agg = deeper
            continue
        deeper = agg.get("_filter")
        if deeper:
            agg = deeper
            continue
        return agg


def format_table(aggs, es_query, query, decoders, selects):
    new_edges = wrap(count_dim(aggs, es_query, decoders))
    dims = tuple(len(e.domain.partitions) + (0 if e.allowNulls is False else 1) for e in new_edges)
    rank = len(dims)
    header = tuple(new_edges.name + selects.name)
    name2index = {s.name: i + rank for i, s in enumerate(selects)}

    def data():
        is_sent = Matrix(dims=dims)

        for row, coord, agg, select in aggs_iterator(aggs, es_query, decoders):
            output = is_sent[coord]
            if output == None:
                output = is_sent[coord] = [d.get_value(c) for c, d in zip(coord, decoders)] + [s.default for s in selects]
                yield output
            # THIS IS A TRICK!  WE WILL UPDATE A ROW THAT WAS ALREADY YIELDED
            union(output, name2index[select.name], select.pull(agg), select.aggregate)

        # EMIT THE MISSING CELLS IN THE CUBE
        if not query.groupby:
            for coord, output in is_sent:
                if output == None:
                    record = [d.get_value(c) for c, d in zip(coord, decoders)] + [s.default for s in selects]
                    yield record

    return Data(
        meta={"format": "table"},
        header=header,
        data=list(data())
    )


def format_table_from_groupby(aggs, es_query, query, decoders, selects):
    new_edges = wrap(count_dim(aggs, es_query, decoders))
    dims = tuple(len(e.domain.partitions) + (0 if e.allowNulls is False else 1) for e in new_edges)
    rank = len(dims)
    header = tuple(new_edges.name + selects.name)
    name2index = {s.name: i + rank for i, s in enumerate(selects)}

    def data():
        is_sent = Matrix(dims=dims)
        for row, coord, agg, select in aggs_iterator(aggs, es_query, decoders):
            output = is_sent[coord]
            if output == None:
                output = is_sent[coord] = [d.get_value(c) for c, d in zip(coord, decoders)] + [s.default for s in selects]
                yield output
            # THIS IS A TRICK!  WE WILL UPDATE A ROW THAT WAS ALREADY YIELDED
            union(output, name2index[select.name], select.pull(agg), select.aggregate)

    return Data(
        meta={"format": "table"},
        header=header,
        data=list(data())
    )


# def format_table_from_aggop(aggs, es_query, query, decoders, selects):
#     header = selects.name
#     row = [s.default for s in selects]
#     for _, _, agg, s in aggs_iterator(aggs, es_query, tuple()):
#         row.append(s.pull(agg))
#
#     return Data(
#         meta={"format": "table"},
#         header=header,
#         data=[row]
#     )


def format_tab(aggs, es_query, query, decoders, select):
    table = format_table(aggs, es_query, query, decoders, select)

    def data():
        yield "\t".join(map(quote, table.header))
        for d in table.data:
            yield "\t".join(map(quote, d))

    return data()


def format_csv(aggs, es_query, query, decoders, select):
    table = format_table(aggs, es_query, query, decoders, select)

    def data():
        yield ", ".join(map(quote, table.header))
        for d in table.data:
            yield ", ".join(map(quote, d))

    return data()


def format_list_from_groupby(aggs, es_query, query, decoders, selects):
    new_edges = wrap(count_dim(aggs, es_query, decoders))

    def data():
        groupby = query.groupby
        dims = tuple(len(e.domain.partitions) + (0 if e.allowNulls is False else 1) for e in new_edges)
        is_sent = Matrix(dims=dims)

        for row, coord, agg, s in aggs_iterator(aggs, es_query, decoders):
            output = is_sent[coord]
            if output == None:
                output = Data()
                for g, d in zip(groupby, decoders):
                    output[coalesce(g.put.name, g.name)] = d.get_value_from_row(row)
                for s in selects:
                    output[s.name] = s.default
                yield output
            # THIS IS A TRICK!  WE WILL UPDATE A ROW THAT WAS ALREADY YIELDED
            union(output, s.name, s.pull(agg), s.aggregate)

    for g in query.groupby:
        g.put.name = coalesce(g.put.name, g.name)

    output = Data(
        meta={"format": "list"},
        data=list(data())
    )
    return output


def format_list(aggs, es_query, query, decoders, select):
    table = format_table(aggs, es_query, query, decoders, select)
    header = table.header

    if query.edges or query.groupby:
        data = []
        for row in table.data:
            d = Data()
            for h, r in zip(header, row):
                d[h] = r
            data.append(d)
        format = "list"
    elif isinstance(query.select, list):
        data = Data()
        for h, r in zip(header, table.data[0]):
            data[h] = r
        format = "value"
    else:
        data = table.data[0][0]
        format = "value"

    output = Data(
        meta={"format": format},
        data=data
    )
    return output


def format_line(aggs, es_query, query, decoders, select):
    list = format_list(aggs, es_query, query, decoders, select)

    def data():
        for d in list.data:
            yield convert.value2json(d)

    return data()


set_default(format_dispatch, {
    None: (format_cube, format_table_from_groupby, format_cube, "application/json"),
    "cube": (format_cube, format_cube, format_cube, "application/json"),
    "table": (format_table, format_table_from_groupby, format_table,  "application/json"),
    "list": (format_list, format_list_from_groupby, format_list, "application/json"),
    # "csv": (format_csv, format_csv_from_groupby,  "text/csv"),
    # "tab": (format_tab, format_tab_from_groupby,  "text/tab-separated-values"),
    # "line": (format_line, format_line_from_groupby,  "application/json")
})


def _get(v, k, d):
    for p in split_field(k):
        try:
            v = v.get(p)
            if v is None:
                return d
        except Exception:
            v = [vv.get(p) for vv in v]
    return v


def union(matrix, coord, value, agg):
    # matrix[coord] = existing + value  WITH ADDITIONAL CHECKS
    existing = matrix[coord]
    if existing == None:
        matrix[coord] = value
    elif value == None:
        pass
    elif agg not in ['sum', 'count']:
        if agg == "cardinality" and (existing == 0 or value == 0):
            matrix[coord] = existing + value
            return
        elif agg == "stats" and (not existing or not value):
            matrix[coord] = existing + value
            return
        Log.warning("not ready")
    else:
        matrix[coord] = existing + value
