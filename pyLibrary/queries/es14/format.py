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

from pyLibrary import convert
from pyLibrary.collections.matrix import Matrix
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict, literal_field, set_default, coalesce, wrap
from pyLibrary.queries.cube import Cube
from pyLibrary.queries.es14.aggs import count_dim, aggs_iterator, format_dispatch


def format_cube(decoders, aggs, start, query, select):
    new_edges = count_dim(aggs, decoders)
    dims = tuple(len(e.domain.partitions) + (0 if e.allowNulls is False else 1) for e in new_edges)
    matricies = [(s, Matrix(dims=dims, zeros=(s.aggregate == "count"))) for s in select]
    for row, agg in aggs_iterator(aggs, decoders):
        coord = tuple(d.get_index(row) for d in decoders)
        for s, m in matricies:
            try:
                if m[coord]:
                    Log.error("Not expected")
                m[coord] = agg[s.pull]
            except Exception, e:
                tuple(d.get_index(row) for d in decoders)
                Log.error("", e)
    cube = Cube(query.select, new_edges, {s.name: m for s, m in matricies})
    cube.frum = query
    return cube


def format_cube_from_aggop(decoders, aggs, start, query, select):
    agg = aggs
    b = coalesce(agg._filter, agg._nested)
    while b:
        agg = b
        b = coalesce(agg._filter, agg._nested)

    matricies = [(s, Matrix(dims=[], zeros=(s.aggregate == "count"))) for s in select]
    for s, m in matricies:
        m[tuple()] = agg[s.pull]
    cube = Cube(query.select, [], {s.name: m for s, m in matricies})
    cube.frum = query
    return cube


def format_table(decoders, aggs, start, query, select):
    new_edges = count_dim(aggs, decoders)
    header = new_edges.name + select.name

    def data():
        dims = tuple(len(e.domain.partitions) + (0 if e.allowNulls is False else 1) for e in new_edges)
        is_sent = Matrix(dims=dims, zeros=True)
        for row, agg in aggs_iterator(aggs, decoders):
            coord = tuple(d.get_index(row) for d in decoders)
            is_sent[coord] = 1

            output = [d.get_value(c) for c, d in zip(coord, decoders)]
            for s in select:
                output.append(agg[s.pull])
            yield output

        # EMIT THE MISSING CELLS IN THE CUBE
        # for c, v in is_sent:
        #     if not v:
        #         record = [d.get_value(c[i]) for i, d in enumerate(decoders)]
        #         for s in select:
        #             if s.aggregate == "count":
        #                 record.append(0)
        #             else:
        #                 record.append(None)
        #         yield record

    return Dict(
        meta={"format": "table"},
        header=header,
        data=list(data())
    )


def format_table_from_groupby(decoders, aggs, start, query, select):
    header = [d.edge.name for d in decoders] + select.name

    def data():
        for row, agg in aggs_iterator(aggs, decoders):
            output = [d.get_value_from_row(row) for d in decoders]
            for s in select:
                output.append(agg[s.pull])
            yield output

    return Dict(
        meta={"format": "table"},
        header=header,
        data=list(data())
    )


def format_table_from_aggop(decoders, aggs, start, query, select):
    header = select.name

    agg = aggs
    b = coalesce(agg._filter, agg._nested)
    while b:
        agg = b
        b = coalesce(agg._filter, agg._nested)

    row = []
    for s in select:
        row.append(agg[s.pull])

    return Dict(
        meta={"format": "table"},
        header=header,
        data=[row]
    )


def format_tab(decoders, aggs, start, query, select):
    table = format_table(decoders, aggs, start, query, select)

    def data():
        yield "\t".join(map(convert.string2quote, table.header))
        for d in table.data:
            yield "\t".join(map(convert.string2quote, d))

    return data()


def format_csv(decoders, aggs, start, query, select):
    table = format_table(decoders, aggs, start, query, select)

    def data():
        yield ", ".join(map(convert.string2quote, table.header))
        for d in table.data:
            yield ", ".join(map(convert.string2quote, d))

    return data()


def format_list_from_groupby(decoders, aggs, start, query, select):
    def data():
        for row, agg in aggs_iterator(aggs, decoders):
            output = Dict()
            for g, d in zip(query.groupby, decoders):
                output[g.name] = d.get_value_from_row(row)

            for s in select:
                output[s.name] = agg[s.pull]
            yield output

    output = Dict(
        meta={"format": "list"},
        data=list(data())
    )
    return output


def format_list(decoders, aggs, start, query, select):
    new_edges = count_dim(aggs, decoders)

    def data():
        dims = tuple(len(e.domain.partitions) + (0 if e.allowNulls is False else 1) for e in new_edges)
        is_sent = Matrix(dims=dims, zeros=True)
        for row, agg in aggs_iterator(aggs, decoders):
            coord = tuple(d.get_index(row) for d in decoders)
            is_sent[coord] = 1

            output = Dict()
            for e, c, d in zip(query.edges, coord, decoders):
                output[e.name] = d.get_value(c)

            for s in select:
                output[s.name] = agg[s.pull]
            yield output

    output = Dict(
        meta={"format": "list"},
        data=list(data())
    )
    return output


def format_list_from_aggop(decoders, aggs, start, query, select):
    agg = aggs
    b = coalesce(agg._filter, agg._nested)
    while b:
        agg = b
        b = coalesce(agg._filter, agg._nested)

    item = Dict()
    for s in select:
        item[s.name] = agg[s.pull]

    return wrap({
        "meta": {"format": "list"},
        "data": [item]
    })








def format_line(decoders, aggs, start, query, select):
    list = format_list(decoders, aggs, start, query, select)

    def data():
        for d in list.data:
            yield convert.value2json(d)

    return data()


set_default(format_dispatch, {
    None: (format_cube, format_table_from_groupby, format_cube_from_aggop, "application/json"),
    "cube": (format_cube, format_cube, format_cube_from_aggop, "application/json"),
    "table": (format_table, format_table_from_groupby, format_table_from_aggop,  "application/json"),
    "list": (format_list, format_list_from_groupby, format_list_from_aggop, "application/json"),
    # "csv": (format_csv, format_csv_from_groupby,  "text/csv"),
    # "tab": (format_tab, format_tab_from_groupby,  "text/tab-separated-values"),
    # "line": (format_line, format_line_from_groupby,  "application/json")
})
