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

from jx_base.expressions import LeavesOp
from jx_base.language import is_op
from jx_python.containers.cube import Cube
from mo_collections.matrix import Matrix
from mo_dots import Data, is_data, is_list, unwrap, unwraplist, to_data, listwrap
from mo_files import mimetype
from mo_future import transpose
from mo_logs import Log
from mo_math import MAX
from mo_times.timer import Timer


def doc_formatter(select, query=None):
    # RETURN A FUNCTION THAT RETURNS A FORMATTED ROW

    if is_list(query.select):
        def format_object(doc):
            r = Data()
            for s in select:
                v = unwraplist(s.pull(doc))
                if v is not None:
                    try:
                        if s.put.child == '.':
                            r[s.put.name] = v
                        else:
                            r[s.put.name][s.put.child] = v
                    except Exception as e:
                        Log.error("what's happening here?", cause=e)
            return r if r else None
        return format_object

    if is_op(query.select.value, LeavesOp):
        def format_deep(doc):
            r = Data()
            for s in select:
                r[s.put.name][s.put.child] = unwraplist(s.pull(doc))
            return r if r else None
        return format_deep
    else:
        def format_value(doc):
            r = None
            for s in select:
                v = unwraplist(s.pull(doc))
                if v is None:
                    continue
                if s.put.child == ".":
                    r = v
                else:
                    if r is None:
                        r = Data()
                    r[s.put.child] = v

            return r
        return format_value


def format_list(documents, select, query=None):
    f = doc_formatter(select, query)
    data = [f(row) for row in documents]

    return Data(meta={"format": "list"}, data=data)


def row_formatter(select):
    # RETURN A FUNCTION THAT RETURNS A FORMATTED ROW

    select = listwrap(select)
    num_columns = MAX(select.put.index) + 1

    def format_row(doc):
        row = [None] * num_columns
        for s in select:
            value = unwraplist(s.pull(doc))

            if value == None:
                continue

            index, child = s.put.index, s.put.child
            if child == ".":
                row[index] = value
            else:
                if row[index] is None:
                    row[index] = Data()
                row[index][child] = value
        return row

    return format_row


def format_table(T, select, query=None):
    form = row_formatter(select)

    data = [form(row) for row in T]
    header = format_table_header(select, query)

    return Data(meta={"format": "table"}, header=header, data=data)


def format_table_header(select, query):
    num_columns = MAX(select.put.index) + 1
    header = [None] * num_columns

    if is_data(query.select) and not is_op(query.select.value, LeavesOp):
        for s in select:
            header[s.put.index] = s.name
    else:
        for s in select:
            if header[s.put.index]:
                continue
            header[s.put.index] = s.name

    return header


def format_cube(T, select, query=None):
    with Timer("format table"):
        table = format_table(T, select, query)

    if len(table.data) == 0:
        return Cube(
            scrub_select(select),
            edges=[
                {
                    "name": "rownum",
                    "domain": {"type": "rownum", "min": 0, "max": 0, "interval": 1},
                }
            ],
            data={h: Matrix(list=[]) for i, h in enumerate(table.header)},
        )

    cols = transpose(*unwrap(table.data))
    return Cube(
        scrub_select(select),
        edges=[
            {
                "name": "rownum",
                "domain": {
                    "type": "rownum",
                    "min": 0,
                    "max": len(table.data),
                    "interval": 1,
                },
            }
        ],
        data={h: Matrix(list=cols[i]) for i, h in enumerate(table.header)},
    )


def scrub_select(select):
    return to_data(
        [{"name": s.name} for s in select]
    )

set_formatters = {
    # RESPONSE FORMATTER, SETUP_ROW_FORMATTER, DATATYPE
    None: (format_cube, None, mimetype.JSON),
    "cube": (format_cube, None, mimetype.JSON),
    "table": (format_table, row_formatter, mimetype.JSON),
    "list": (format_list, doc_formatter, mimetype.JSON),
}
