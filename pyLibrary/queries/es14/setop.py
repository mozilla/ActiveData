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
from pyLibrary import queries

from pyLibrary.collections.matrix import Matrix
from pyLibrary.collections import AND, SUM, OR, UNION
from pyLibrary.dot import nvl, split_field, set_default, Dict, unwraplist
from pyLibrary.dot.lists import DictList
from pyLibrary.dot import listwrap
from pyLibrary.queries.domains import is_keyword
from pyLibrary.queries import domains
from pyLibrary.queries.filters import simplify_esfilter, TRUE_FILTER
from pyLibrary.debugs.logs import Log
from pyLibrary.queries import filters
from pyLibrary.queries.cube import Cube
from pyLibrary.queries.es14.util import aggregates1_4
from pyLibrary.times.timer import Timer
from pyLibrary.queries import es14, es09



format_dispatch = {}

def is_fieldop(query):
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
    es_query = es14.util.build_es_query(query)
    select = listwrap(query.select)
    es_query.query = {
        "filtered": {
            "query": {
                "match_all": {}
            },
            "filter": filters.simplify_esfilter(query.where)
        }
    }
    es_query.size = nvl(query.limit, queries.query.DEFAULT_LIMIT)
    es_query.fields = DictList()
    source = "fields"
    for s in select.value:
        if s == "*":
            es_query.fields=None
            source = "_source"
        elif s == ".":
            es_query.fields=None
            source = "_source"
        elif isinstance(s, list) and es_query.fields is not None:
            es_query.fields.extend(s)
        elif isinstance(s, dict) and es_query.fields is not None:
            es_query.fields.extend(s.values())
        elif es_query.fields is not None:
            es_query.fields.append(s)
    es_query.sort = [{s.field: "asc" if s.sort >= 0 else "desc"} for s in query.sort]

    with Timer("call to ES") as es_duration:
        data = es09.util.post(es, es_query, query.limit)

    T = data.hits.hits
    for i, s in enumerate(select.copy()):
        # IF THERE IS A *, THEN INSERT THE EXTRA COLUMNS
        if s.value == "*":
            try:
                column_names = set(query.frum.get_column_names())
            except Exception, e:
                Log.warning("can not get columns", e)
                column_names = UNION(*[[k for k, v in row.items()] for row in T.select(source)])
            column_names -= set(select.name)
            select = select[:i:] + [{"name": n, "value": n} for n in column_names] + select[i + 1::]
            break

    try:
        formatter, groupby_formatter, mime_type = format_dispatch[query.format]

        output = formatter(T, select, source)
        output.meta.es_response_time = es_duration.seconds
        output.meta.content_type = mime_type
        output.meta.es_query = es_query
        return output
    except Exception, e:
        Log.error("problem formatting")


def is_setop(query):
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


def es_setop(es, mvel, query):
    FromES = es14.util.build_es_query(query)
    select = listwrap(query.select)

    isDeep = len(split_field(query.frum.name)) > 1  # LOOKING INTO NESTED WILL REQUIRE A SCRIPT
    isComplex = OR([s.value == None and s.aggregate not in ("count", "none") for s in select])   # CONVERTING esfilter DEFINED PARTS WILL REQUIRE SCRIPT

    if not isDeep and not isComplex and len(select) == 1:
        if not select[0].value:
            FromES.query = {"filtered": {
                "query": {"match_all": {}},
                "filter": simplify_esfilter(query.where)
            }}
            FromES.size = 1  # PREVENT QUERY CHECKER FROM THROWING ERROR
        elif is_keyword(select[0].value):
            FromES.facets.mvel = {
                "terms": {
                    "field": select[0].value,
                    "size": nvl(query.limit, 200000)
                },
                "facet_filter": simplify_esfilter(query.where)
            }
            if query.sort:
                s = query.sort
                if len(s) > 1:
                    Log.error("can not sort by more than one field")

                s0 = s[0]
                if s0.field != select[0].value:
                    Log.error("can not sort by anything other than count, or term")

                FromES.facets.mvel.terms.order = "term" if s0.sort >= 0 else "reverse_term"
    elif not isDeep:
        simple_query = query.copy()
        simple_query.where = TRUE_FILTER  # THE FACET FILTER IS FASTER
        FromES.facets.mvel = {
            "terms": {
                "script_field": mvel.code(simple_query),
                "size": nvl(simple_query.limit, 200000)
            },
            "facet_filter": simplify_esfilter(query.where)
        }
    else:
        FromES.facets.mvel = {
            "terms": {
                "script_field": mvel.code(query),
                "size": nvl(query.limit, 200000)
            },
            "facet_filter": simplify_esfilter(query.where)
        }

    data = es09.util.post(es, FromES, query.limit)

    if len(select) == 1:
        if not select[0].value:
            # SPECIAL CASE FOR SINGLE COUNT
            output = Matrix(value=data.hits.total)
            cube = Cube(query.select, [], {select[0].name: output})
        elif is_keyword(select[0].value):
            # SPECIAL CASE FOR SINGLE TERM
            T = data.facets.mvel.terms
            output = Matrix.wrap([t.term for t in T])
            cube = Cube(query.select, [], {select[0].name: output})
    else:
        data_list = es09.expressions.unpack_terms(data.facets.mvel, select)
        if not data_list:
            cube = Cube(select, [], {s.name: Matrix.wrap([]) for s in select})
        else:
            output = zip(*data_list)
            cube = Cube(select, [], {s.name: Matrix(list=output[i]) for i, s in enumerate(select)})

    cube.frum = query
    return cube



def is_deep(query):
    select = listwrap(query.select)
    if len(select) > 1:
        return False

    if aggregates1_4[select[0].aggregate] not in ("none", "count"):
        return False

    if len(query.edges)<=1:
        return False

    isDeep = len(split_field(query["from"].name)) > 1  # LOOKING INTO NESTED WILL REQUIRE A SCRIPT
    if not isDeep:
        return False   # BETTER TO USE TERM QUERY

    return True


def es_deepop(es, mvel, query):
    FromES = es14.util.build_es_query(query)

    select = query.edges

    temp_query = query.copy()
    temp_query.select = select
    temp_query.edges = DictList()
    FromES.facets.mvel = {
        "terms": {
            "script_field": mvel.code(temp_query),
            "size": query.limit
        },
        "facet_filter": simplify_esfilter(query.where)
    }

    data = es09.util.post(es, FromES, query.limit)

    rows = es09.expressions.unpack_terms(data.facets.mvel, query.edges)
    terms = zip(*rows)

    # NUMBER ALL EDGES FOR qb INDEXING
    edges = query.edges
    for f, e in enumerate(edges):
        for r in terms[f]:
            e.domain.getPartByKey(r)

        e.index = f
        for p, part in enumerate(e.domain.partitions):
            part.dataIndex = p
        e.domain.NULL.dataIndex = len(e.domain.partitions)

    # MAKE CUBE
    dims = [len(e.domain.partitions) for e in query.edges]
    output = Matrix(*dims)

    # FILL CUBE
    for r in rows:
        term_coord = [e.domain.getPartByKey(r[i]).dataIndex for i, e in enumerate(edges)]
        output[term_coord] = SUM(output[term_coord], r[-1])

    cube = Cube(query.select, query.edges, {query.select.name: output})
    cube.frum = query
    return cube




def format_list(T, select, source):
    data = []
    for row in T:
        r = {}
        for s in select:
            if s.value == ".":
                r[s.name] = row[source]
            else:
                r[s.name] = unwraplist(row[source][s.value])
        data.append(r)
    return Dict(
        meta={"format": "list"},
        data=data
    )


def format_table(T, select, source):
    header = [s.name for s in select]
    map = {s.name: i for i, s in enumerate(select)}  # MAP FROM name TO COLUMN INDEX
    data = []
    for row in T:
        r = [None] * len(header)
        for s in select:
            if s.value == ".":
                r[map[s.name]] = row[source]
            else:
                r[map[s.name]] = unwraplist(row[source][s.value])
        data.append(r)
    return Dict(
        meta={"format": "table"},
        header=header,
        data=data
    )


def format_cube(T, select, source):
    matricies = {}
    for s in select:
        try:
            if s.value == ".":
                matricies[s.name] = Matrix.wrap(T.select(source))
            elif isinstance(s.value, dict):
                # for k, v in s.value.items():
                # matricies[join_field(split_field(s.name)+[k])] = Matrix.wrap([unwrap(t.fields)[v] for t in T])
                matricies[s.name] = Matrix.wrap([{k: unwraplist(t[source][v]) for k, v in s.value.items()} for t in T])
            elif isinstance(s.value, list):
                matricies[s.name] = Matrix.wrap([tuple(unwraplist(t[source][ss]) for ss in s.value) for t in T])
            else:
                matricies[s.name] = Matrix.wrap([unwraplist(t[source][s.value]) for t in T])
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
