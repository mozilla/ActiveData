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

from pyLibrary import queries
from pyLibrary.collections.matrix import Matrix
from pyLibrary.collections import AND
from pyLibrary.dot import coalesce, split_field, set_default, Dict, unwraplist, literal_field
from pyLibrary.dot.lists import DictList
from pyLibrary.dot import listwrap
from pyLibrary.maths import Math
from pyLibrary.debugs.logs import Log
from pyLibrary.queries import domains, es14, es09, qb
from pyLibrary.queries.containers.cube import Cube
from pyLibrary.queries.domains import is_keyword
from pyLibrary.queries.es14.util import qb_sort_to_es_sort
from pyLibrary.queries.expressions import qb_expression_to_esfilter, simplify_esfilter, qb_expression_to_ruby
from pyLibrary.queries.query import DEFAULT_LIMIT
from pyLibrary.times.timer import Timer


format_dispatch = {}

def is_fieldop(es, query):
    if not any(map(es.cluster.version.startswith, ["1.4.", "1.5.", "1.6.", "1.7."])):
        return False

    # THESE SMOOTH EDGES REQUIRE ALL DATA (SETOP)
    select = listwrap(query.select)
    if not query.edges:
        isDeep = len(split_field(query.frum.name)) > 1  # LOOKING INTO NESTED WILL REQUIRE A SCRIPT
        isSimple = AND(s.value != None and (s.value in ["*", "."] or is_keyword(s.value)) for s in select)
        noAgg = AND(s.aggregate == "none" for s in select)

        if not isDeep and isSimple and noAgg:
            return True
    else:
        isSmooth = AND((e.domain.type in domains.ALGEBRAIC and e.domain.interval == "none") for e in query.edges)
        if isSmooth:
            return True

    return False


def es_fieldop(es, query):
    es_query, es_filter = es14.util.es_query_template(query.frum.name)
    es_query[es_filter] = simplify_esfilter(qb_expression_to_esfilter(query.where))
    es_query.size = coalesce(query.limit, DEFAULT_LIMIT)
    es_query.sort = qb_sort_to_es_sort(query.sort)
    es_query.fields = DictList()

    return extract_rows(es, es_query, query)


def extract_rows(es, es_query, query):

    new_select = DictList()
    column_names = set(c.name for c in query.frum.get_columns() if (c.type not in ["object"] or c.useSource) and not c.depth)
    source = "fields"

    i = 0
    for s in listwrap(query.select):
        # IF THERE IS A *, THEN INSERT THE EXTRA COLUMNS
        if s.value == "*":
            es_query.fields = None
            source = "_source"

            net_columns = column_names - set(listwrap(query.select).name)
            for n in net_columns:
                new_select.append({"name": n, "value": n, "put": {"index": i, "child": "."}})
                i += 1
        elif s.value == ".":
            es_query.fields = None
            source = "_source"

            new_select.append({"name": s.name, "value": s.value, "put": {"index": i, "child": "."}})
            i += 1
        elif isinstance(s.value, basestring) and s.value.endswith(".*") and is_keyword(s.value[:-2]):
            parent = s.value[:-1]
            prefix = len(parent)
            for c in column_names:
                if c.startswith(parent):
                    if es_query.fields is not None:
                        es_query.fields.append(c)

                    new_select.append({"name": s.name+"."+c[prefix:], "value": c, "put": {"index": i, "child": "."}})
                    i += 1
        elif isinstance(s.value, basestring) and is_keyword(s.value):
            parent = s.value + "."
            prefix = len(parent)
            net_columns = [c for c in column_names if c.startswith(parent)]
            if not net_columns:
                if es_query.fields is not None:
                    es_query.fields.append(s.value)
                new_select.append({"name": s.name, "value": s.value, "put": {"index": i, "child": "."}})
            else:
                for n in net_columns:
                    if es_query.fields is not None:
                        es_query.fields.append(n)
                    new_select.append({"name": s.name, "value": n, "put": {"index": i, "child": n[prefix:]}})
            i += 1
        elif isinstance(s.value, list):
            Log.error("need an example")
            if es_query.fields is not None:
                es_query.fields.extend([v for v in s.value])
        else:
            es_query.script_fields[literal_field(s.name)] = {"script": qb_expression_to_ruby(s.value)}
            new_select.append({"name": s.name, "value": s.name, "put": {"index": i, "child": "."}})
            i += 1



    with Timer("call to ES") as call_timer:
        data = es09.util.post(es, es_query, query.limit)

    T = data.hits.hits

    try:
        formatter, groupby_formatter, mime_type = format_dispatch[query.format]

        output = formatter(T, new_select, source, query)
        output.meta.es_response_time = call_timer.duration
        output.meta.content_type = mime_type
        output.meta.es_query = es_query
        return output
    except Exception, e:
        Log.error("problem formatting", e)


def is_setop(es, query):
    if not any(map(es.cluster.version.startswith, ["1.4.", "1.5.", "1.6.", "1.7."])):
        return False

    select = listwrap(query.select)

    if not query.edges:
        isDeep = len(split_field(query.frum.name)) > 1  # LOOKING INTO NESTED WILL REQUIRE A SCRIPT
        simpleAgg = AND([s.aggregate in ("count", "none") for s in select])   # CONVERTING esfilter DEFINED PARTS WILL REQUIRE SCRIPT

        # NO EDGES IMPLIES SIMPLER QUERIES: EITHER A SET OPERATION, OR RETURN SINGLE AGGREGATE
        if simpleAgg or isDeep:
            return True
    else:
        isSmooth = AND((e.domain.type in domains.ALGEBRAIC and e.domain.interval == "none") for e in query.edges)
        if isSmooth:
            return True

    return False


def es_setop(es, query):
    es_query, es_filter = es14.util.es_query_template(query.frum.name)
    es_query[es_filter] = simplify_esfilter(qb_expression_to_esfilter(query.where))
    es_query.size = coalesce(query.limit, queries.query.DEFAULT_LIMIT)
    es_query.fields = DictList()
    es_query.sort = qb_sort_to_es_sort(query.sort)

    return extract_rows(es, es_query, query)


def format_list(T, select, source, query=None):
    data = []
    for row in T:
        r = Dict()
        for s in select:
            if source == "_source":
                r[s.name][s.put.child] = unwraplist(row[source][s.value])
            elif isinstance(s.value, basestring):  # fields
                r[s.name][s.put.child] = unwraplist(row[source][literal_field(s.value)])
            else:
                r[s.name][s.put.child] = unwraplist(row[source][literal_field(s.name)])
        data.append(r if r else None)
    return Dict(
        meta={"format": "list"},
        data=data
    )


def format_table(T, select, source, query=None):
    data = []
    num_columns=(Math.MAX(select.put.index)+1)
    for row in T:
        r = [None] * num_columns
        for s in select:
            if source == "_source":
                value = unwraplist(row[source][s.value])
            elif isinstance(s.value, basestring):  # fields
                value = unwraplist(row[source][literal_field(s.value)])
            else:
                value = unwraplist(row[source][literal_field(s.name)])

            if value != None:
                i, n = s.put.index, s.put.child
                col = r[i]
                if col is None:
                    r[i] = Dict()
                r[i][n] = value

        data.append(r)

    header = [None]*num_columns
    for s in select:
        if header[s.put.index]:
            continue
        header[s.put.index] = s.name

    return Dict(
        meta={"format": "table"},
        header=header,
        data=data
    )


def format_cube(T, select, source, query=None):
    matricies = {s: Matrix(dims=(len(T),)) for s in set(select.name)}
    for i, t in enumerate(T):
        for s in select:
            try:
                if isinstance(s.value, list):
                    value = tuple(unwraplist(t[source][ss]) for ss in s.value)
                else:
                    if source == "_source":
                        value = unwraplist(t[source][s.value])
                    elif isinstance(s.value, basestring):  # fields
                        value = unwraplist(t[source].get(s.value))
                    else:
                        value = unwraplist(t[source].get(s.name))

                if value == None:
                    continue

                p, n = s.name, s.put.child
                col = matricies[p][(i,)]
                if col == None:
                    matricies[p][(i,)] = Dict()
                matricies[p][(i,)][n] = value

            except Exception, e:
                Log.error("", e)
    cube = Cube(select, edges=[{"name": "rownum", "domain": {"type": "rownum", "min": 0, "max": len(T), "interval": 1}}], data=matricies)
    return cube


set_default(format_dispatch, {
    None: (format_cube, None, "application/json"),
    "cube": (format_cube, None, "application/json"),
    "table": (format_table, None, "application/json"),
    "list": (format_list, None, "application/json")
})
