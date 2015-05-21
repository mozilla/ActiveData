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

from pyLibrary.collections.matrix import Matrix
from pyLibrary.collections import AND, SUM, OR
from pyLibrary.dot import coalesce, split_field
from pyLibrary.dot.lists import DictList
from pyLibrary.dot import listwrap, unwrap
from pyLibrary.queries.es09.expressions import unpack_terms
from pyLibrary.queries.es09.util import aggregates
from pyLibrary.queries import domains, es09
from pyLibrary.debugs.logs import Log
from pyLibrary.queries.cube import Cube
from pyLibrary.queries.expressions import simplify_esfilter, TRUE_FILTER


def is_fieldop(query):
    # THESE SMOOTH EDGES REQUIRE ALL DATA (SETOP)

    select = listwrap(query.select)
    if not query.edges:
        isDeep = len(split_field(query.frum.name)) > 1  # LOOKING INTO NESTED WILL REQUIRE A SCRIPT
        isSimple = AND(s.value != None and (s.value == "*" or isKeyword(s.value)) for s in select)
        noAgg = AND(s.aggregate == "none" for s in select)

        if not isDeep and isSimple and noAgg:
            return True
    else:
        isSmooth = AND((e.domain.type in domains.ALGEBRAIC and e.domain.interval == "none") for e in query.edges)
        if isSmooth:
            return True

    return False

def isKeyword(value):
    if isinstance(value, Mapping):
        return AND(isKeyword(v) for k, v in value.items())
    if isinstance(value, list):
        return AND(isKeyword(v) for v in value)
    return isKeyword(value)


def es_fieldop(es, query):
    FromES = es09.util.build_es_query(query)
    select = listwrap(query.select)
    FromES.query = {
        "filtered": {
            "query": {
                "match_all": {}
            },
            "filter": simplify_esfilter(query.where)
        }
    }
    FromES.size = coalesce(query.limit, 200000)
    FromES.fields = DictList()
    for s in select.value:
        if s == "*":
            FromES.fields = None
        elif isinstance(s, list):
            FromES.fields.extend(s)
        elif isinstance(s, Mapping):
            FromES.fields.extend(s.values())
        else:
            FromES.fields.append(s)
    FromES.sort = [{s.field: "asc" if s.sort >= 0 else "desc"} for s in query.sort]

    data = es09.util.post(es, FromES, query.limit)

    T = data.hits.hits
    matricies = {}
    for s in select:
        if s.value == "*":
            matricies[s.name] = Matrix.wrap([t._source for t in T])
        elif isinstance(s.value, Mapping):
            # for k, v in s.value.items():
            #     matricies[join_field(split_field(s.name)+[k])] = Matrix.wrap([unwrap(t.fields)[v] for t in T])
            matricies[s.name] = Matrix.wrap([{k: unwrap(t.fields).get(v, None) for k, v in s.value.items()}for t in T])
        elif isinstance(s.value, list):
            matricies[s.name] = Matrix.wrap([tuple(unwrap(t.fields).get(ss, None) for ss in s.value) for t in T])
        elif not s.value:
            matricies[s.name] = Matrix.wrap([unwrap(t.fields).get(s.value, None) for t in T])
        else:
            try:
                matricies[s.name] = Matrix.wrap([unwrap(t.fields).get(s.value, None) for t in T])
            except Exception, e:
                Log.error("", e)

    cube = Cube(query.select, query.edges, matricies, frum=query)
    cube.frum = query
    return cube


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
    FromES = es09.util.build_es_query(query)
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
        elif isKeyword(select[0].value):
            FromES.facets.mvel = {
                "terms": {
                    "field": select[0].value,
                    "size": coalesce(query.limit, 200000)
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

                FromES.facets.terms.order = "term" if s0.sort >= 0 else "reverse_term"
    elif not isDeep:
        simple_query = query.copy()
        simple_query.where = TRUE_FILTER  # THE FACET FILTER IS FASTER
        FromES.facets.mvel = {
            "terms": {
                "script_field": mvel.code(simple_query),
                "size": coalesce(simple_query.limit, 200000)
            },
            "facet_filter": simplify_esfilter(query.where)
        }
    else:
        FromES.facets.mvel = {
            "terms": {
                "script_field": mvel.code(query),
                "size": coalesce(query.limit, 200000)
            },
            "facet_filter": simplify_esfilter(query.where)
        }

    data = es09.util.post(es, FromES, query.limit)

    if len(select) == 1:
        if not select[0].value:
            # SPECIAL CASE FOR SINGLE COUNT
            output = Matrix(value=data.hits.total)
            cube = Cube(query.select, [], {select[0].name: output})
        elif isKeyword(select[0].value):
            # SPECIAL CASE FOR SINGLE TERM
            T = data.facets.terms
            output = Matrix.wrap([t.term for t in T])
            cube = Cube(query.select, [], {select[0].name: output})
    else:
        data_list = unpack_terms(data.facets.mvel, select)
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

    if aggregates[select[0].aggregate] not in ("none", "count"):
        return False

    if len(query.edges)<=1:
        return False

    isDeep = len(split_field(query["from"].name)) > 1  # LOOKING INTO NESTED WILL REQUIRE A SCRIPT
    if not isDeep:
        return False   # BETTER TO USE TERM QUERY

    return True


def es_deepop(es, mvel, query):
    FromES = es09.util.build_es_query(query)

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

    rows = unpack_terms(data.facets.mvel, query.edges)
    terms = zip(*rows)

    # NUMBER ALL EDGES FOR Qb INDEXING
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
