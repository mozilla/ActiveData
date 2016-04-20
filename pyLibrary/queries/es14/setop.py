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

from pyLibrary import queries
from pyLibrary.collections.matrix import Matrix
from pyLibrary.collections import AND
from pyLibrary.dot import coalesce, split_field, set_default, Dict, unwraplist, literal_field, join_field, unwrap, wrap
from pyLibrary.dot.lists import DictList
from pyLibrary.dot import listwrap
from pyLibrary.maths import Math
from pyLibrary.debugs.logs import Log
from pyLibrary.queries import es14, es09
from pyLibrary.queries.containers.cube import Cube
from pyLibrary.queries.domains import is_keyword, ALGEBRAIC
from pyLibrary.queries.es14.util import jx_sort_to_es_sort
from pyLibrary.queries.expressions import simplify_esfilter, jx_expression, Variable, LeavesOp
from pyLibrary.queries.query import DEFAULT_LIMIT
from pyLibrary.times.timer import Timer


format_dispatch = {}


def is_setop(es, query):
    if not any(map(es.cluster.version.startswith, ["1.4.", "1.5.", "1.6.", "1.7."])):
        return False

    select = listwrap(query.select)

    if not query.edges:
        isDeep = len(split_field(query.frum.name)) > 1  # LOOKING INTO NESTED WILL REQUIRE A SCRIPT
        simpleAgg = AND([s.aggregate in ("count", "none") for s in select])  # CONVERTING esfilter DEFINED PARTS WILL REQUIRE SCRIPT

        # NO EDGES IMPLIES SIMPLER QUERIES: EITHER A SET OPERATION, OR RETURN SINGLE AGGREGATE
        if simpleAgg or isDeep:
            return True
    else:
        isSmooth = AND((e.domain.type in ALGEBRAIC and e.domain.interval == "none") for e in query.edges)
        if isSmooth:
            return True

    return False


def es_setop(es, query):
    es_query, filters = es14.util.es_query_template(query.frum.name)
    set_default(filters[0], simplify_esfilter(query.where.to_esfilter()))
    es_query.size = coalesce(query.limit, queries.query.DEFAULT_LIMIT)
    es_query.sort = jx_sort_to_es_sort(query.sort)
    es_query.fields = DictList()

    return extract_rows(es, es_query, query)


def extract_rows(es, es_query, query):
    is_list = isinstance(query.select, list)
    select = wrap([s.copy() for s in listwrap(query.select)])
    new_select = DictList()
    columns = query.frum.get_columns()
    leaf_columns = set(c.name for c in columns if c.type not in ["object", "nested"] and (not c.nested_path or c.es_column == c.nested_path))
    nested_columns = set(c.name for c in columns if c.nested_path)

    i = 0
    source = "fields"
    for s in select:
        # IF THERE IS A *, THEN INSERT THE EXTRA COLUMNS
        if isinstance(s.value, LeavesOp):
            if isinstance(s.value.term, Variable):
                if s.value.term.var == ".":
                    es_query.fields = None
                    source = "_source"

                    net_columns = leaf_columns - set(select.name)
                    for n in net_columns:
                        new_select.append({
                            "name": n,
                            "value": n,
                            "put": {"name": n, "index": i, "child": "."}
                        })
                        i += 1
                else:
                    parent = s.value.var + "."
                    prefix = len(parent)
                    for c in leaf_columns:
                        if c.startswith(parent):
                            if es_query.fields is not None:
                                es_query.fields.append(c)

                            new_select.append({
                                "name": s.name + "." + c[prefix:],
                                "value": c,
                                "put": {"name": s.name + "." + c[prefix:], "index": i, "child": "."}
                            })
                            i += 1

        elif isinstance(s.value, Variable):
            if s.value.var == ".":
                es_query.fields = None
                source = "_source"

                new_select.append({
                    "name": s.name,
                    "value": s.value.var,
                    "put": {"name": s.name, "index": i, "child": "."}
                })
                i += 1
            elif s.value.var == "_id":
                new_select.append({
                    "name": s.name,
                    "value": s.value.var,
                    "pull": "_id",
                    "put": {"name": s.name, "index": i, "child": "."}
                })
                i += 1
            elif s.value.var in nested_columns:
                es_query.fields = None
                source = "_source"

                new_select.append({
                    "name": s.name,
                    "value": s.value,
                    "put": {"name": s.name, "index": i, "child": "."}
                })
                i += 1
            else:
                parent = s.value.var + "."
                prefix = len(parent)
                net_columns = [c for c in leaf_columns if c.startswith(parent)]
                if not net_columns:
                    # LEAF
                    if es_query.fields is not None:
                        es_query.fields.append(s.value.var)
                    new_select.append({
                        "name": s.name,
                        "value": s.value,
                        "put": {"name": s.name, "index": i, "child": "."}
                    })
                else:
                    # LEAVES OF OBJECT
                    for n in net_columns:
                        if es_query.fields is not None:
                            es_query.fields.append(n)
                        new_select.append({
                            "name": s.name,
                            "value": n,
                            "put": {"name": s.name, "index": i, "child": n[prefix:]}
                        })
                i += 1
        else:
            es_query.script_fields[literal_field(s.name)] = {"script": s.value.to_ruby()}
            new_select.append({
                "name": s.name,
                "pull": "fields." + literal_field(s.name),
                "put": {"name": s.name, "index": i, "child": "."}
            })
            i += 1

    for n in new_select:
        if n.pull:
            continue
        if source == "_source":
            n.pull = join_field(["_source"] + split_field(n.value))
        else:
            n.pull = "fields." + literal_field(n.value)

    with Timer("call to ES") as call_timer:
        data = es09.util.post(es, es_query, query.limit)

    T = data.hits.hits

    try:
        formatter, groupby_formatter, mime_type = format_dispatch[query.format]

        output = formatter(T, new_select, query)
        output.meta.timing.es = call_timer.duration
        output.meta.content_type = mime_type
        output.meta.es_query = es_query
        return output
    except Exception, e:
        Log.error("problem formatting", e)


def format_list(T, select, query=None):
    data = []
    if isinstance(query.select, list) or (isinstance(query.select.value, basestring) and query.select.value.endswith("*")):
        for row in T:
            r = Dict()
            for s in select:
                r[s.put.name][s.put.child] = unwraplist(row[s.pull])
            data.append(r if r else None)
    else:
        for row in T:
            r = Dict()
            for s in select:
                r[s.put.name][s.put.child] = unwraplist(row[s.pull])
            data.append(r if r else None)

    return Dict(
        meta={"format": "list"},
        data=data
    )


def format_table(T, select, query=None):
    data = []
    num_columns = (Math.MAX(select.put.index) + 1)
    for row in T:
        r = [None] * num_columns
        for s in select:
            value = unwraplist(row[s.pull])

            if value == None:
                continue

            index, child = s.put.index, s.put.child
            if child == ".":
                r[index] = value
            else:
                if r[index] is None:
                    r[index] = Dict()
                r[index][child] = value

        data.append(r)

    header = [None] * num_columns
    for s in select:
        if header[s.put.index]:
            continue
        header[s.put.index] = s.name

    return Dict(
        meta={"format": "table"},
        header=header,
        data=data
    )


def format_cube(T, select, query=None):
    table = format_table(T, select, query)

    if len(table.data) == 0:
        return Cube(
            select,
            edges=[{"name": "rownum", "domain": {"type": "rownum", "min": 0, "max": 0, "interval": 1}}],
            data={h: Matrix(list=[]) for i, h in enumerate(table.header)}
        )

    cols = zip(*unwrap(table.data))
    return Cube(
        select,
        edges=[{"name": "rownum", "domain": {"type": "rownum", "min": 0, "max": len(table.data), "interval": 1}}],
        data={h: Matrix(list=cols[i]) for i, h in enumerate(table.header)}
    )


set_default(format_dispatch, {
    None: (format_cube, None, "application/json"),
    "cube": (format_cube, None, "application/json"),
    "table": (format_table, None, "application/json"),
    "list": (format_list, None, "application/json")
})
