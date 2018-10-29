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
from jx_elasticsearch.es52.aggs import count_dim, aggs_iterator, format_dispatch, drill
from jx_python.containers.cube import Cube
from mo_collections.matrix import Matrix
from mo_dots import Data, set_default, wrap, split_field, coalesce
from mo_future import sort_using_key
from mo_logs import Log
from mo_logs.strings import quote
from pyLibrary import convert


def format_cube(decoders, aggs, start, query, select):
    new_edges = count_dim(aggs, decoders)

    dims = []
    for e in new_edges:
        if isinstance(e.value, TupleOp):
            e.allowNulls = False

        extra = 0 if e.allowNulls is False else 1
        dims.append(len(e.domain.partitions) + extra)

    dims = tuple(dims)
    matricies = tuple((s, Matrix(dims=dims, zeros=s.default)) for s in select)
    for row, coord, agg in aggs_iterator(aggs, decoders):
        for s, m in matricies:
            v = s.pull(agg)
            union(m, coord, v, s.aggregate)

    cube = Cube(
        query.select,
        sort_using_key(new_edges, key=lambda e: e.dim),  # ENSURE EDGES ARE IN SAME ORDER AS QUERY
        {s.name: m for s, m in matricies}
    )
    cube.frum = query
    return cube


def format_cube_from_aggop(decoders, aggs, start, query, select):
    agg = drill(aggs)
    matricies = [(s, Matrix(dims=[], zeros=s.default)) for s in select]
    for s, m in matricies:
        m[tuple()] = s.pull(agg)
    cube = Cube(query.select, [], {s.name: m for s, m in matricies})
    cube.frum = query
    return cube


def format_table(decoders, aggs, start, query, select):
    new_edges = count_dim(aggs, decoders)
    header = tuple(new_edges.name + select.name)

    def data():
        dims = tuple(len(e.domain.partitions) + (0 if e.allowNulls is False else 1) for e in new_edges)
        rank = len(dims)
        is_sent = Matrix(dims=dims)

        if query.sort and not query.groupby:
            all_coord = is_sent._all_combos()  # TRACK THE EXPECTED COMBINATIONS
            for row, coord, agg in aggs_iterator(aggs, decoders):
                missing_coord = all_coord.next()
                while coord != missing_coord:
                    record = [d.get_value(missing_coord[i]) for i, d in enumerate(decoders)]
                    for s in select:
                        if s.aggregate == "count":
                            record.append(0)
                        else:
                            record.append(None)
                    yield record
                    missing_coord = all_coord.next()

                output = [d.get_value(c) for c, d in zip(coord, decoders)]
                for s in select:
                    output.append(s.pull(agg))
                yield output
        else:
            for row, coord, agg in aggs_iterator(aggs, decoders):
                output = is_sent[coord]
                if output == None:
                    output = is_sent[coord] = [d.get_value(c) for c, d in zip(coord, decoders)]
                    for s in select:
                        output.append(s.pull(agg))
                    yield output
                else:
                    # THIS IS A TRICK!  WE WILL UPDATE A ROW THAT WAS ALREADY YIELDED
                    for i, s in enumerate(select):
                        union(output, rank+i, s.pull(agg), s.aggregate)

            # EMIT THE MISSING CELLS IN THE CUBE
            if not query.groupby:
                for c, v in is_sent:
                    if v == None:
                        record = [d.get_value(c[i]) for i, d in enumerate(decoders)]
                        for s in select:
                            if s.aggregate == "count":
                                record.append(0)
                            else:
                                record.append(None)
                        yield record

    return Data(
        meta={"format": "table"},
        header=header,
        data=list(data())
    )


def format_table_from_groupby(decoders, aggs, start, query, select):
    header = [d.edge.name.replace("\\.", ".") for d in decoders] + select.name

    def data():
        for row, coord, agg in aggs_iterator(aggs, decoders):
            if agg.get('doc_count', 0) == 0:
                continue
            output = [d.get_value_from_row(row) for d in decoders]
            for s in select:
                output.append(s.pull(agg))
            yield output

    return Data(
        meta={"format": "table"},
        header=header,
        data=list(data())
    )


def format_table_from_aggop(decoders, aggs, start, query, select):
    header = select.name
    agg = drill(aggs)
    row = []
    for s in select:
        row.append(s.pull(agg))

    return Data(
        meta={"format": "table"},
        header=header,
        data=[row]
    )


def format_tab(decoders, aggs, start, query, select):
    table = format_table(decoders, aggs, start, query, select)

    def data():
        yield "\t".join(map(quote, table.header))
        for d in table.data:
            yield "\t".join(map(quote, d))

    return data()


def format_csv(decoders, aggs, start, query, select):
    table = format_table(decoders, aggs, start, query, select)

    def data():
        yield ", ".join(map(quote, table.header))
        for d in table.data:
            yield ", ".join(map(quote, d))

    return data()


def format_list_from_groupby(decoders, aggs, start, query, select):
    def data():
        groupby = query.groupby

        for row, coord, agg in aggs_iterator(aggs, decoders):
            if agg.get('doc_count', 0) == 0:
                continue
            output = Data()
            for g, d in zip(groupby, decoders):
                output[coalesce(g.put.name, g.name)] = d.get_value_from_row(row)

            for s in select:
                output[s.name] = s.pull(agg)
            yield output

    for g in query.groupby:
        g.put.name = coalesce(g.put.name, g.name)

    output = Data(
        meta={"format": "list"},
        data=list(data())
    )
    return output


def format_list(decoders, aggs, start, query, select):
    table = format_table(decoders, aggs, start, query, select)
    header = table.header

    output = Data(
        meta={"format": "list"},
        data=[dict(zip(header, row)) for row in table.data]
    )
    return output


def format_list_from_aggop(decoders, aggs, start, query, select):
    agg = drill(aggs)

    if isinstance(query.select, list):
        item = Data()
        for s in select:
            item[s.name] = s.pull(agg)
    else:
        item = select[0].pull(agg)

    if query.edges or query.groupby:
        return wrap({
            "meta": {"format": "list"},
            "data": [item]
        })
    else:
        return wrap({
            "meta": {"format": "value"},
            "data": item
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
        Log.warning("not ready")
    else:
        matrix[coord] = existing + value
