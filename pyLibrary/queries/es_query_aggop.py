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

from pyLibrary.collections.matrix import Matrix
from pyLibrary.collections import AND
from pyLibrary.dot import listwrap, unwrap, literal_field
from pyLibrary.queries import es_query_util
from pyLibrary.queries.es_query_util import aggregates, fix_es_stats, buildESQuery
from pyLibrary.queries.filters import simplify
from pyLibrary.queries import MVEL
from pyLibrary.queries.cube import Cube


def is_aggop(query):
    if not query.edges:
        return True
    return False


def es_aggop(es, mvel, query):
    select = listwrap(query.select)
    esQuery = buildESQuery(query)

    isSimple = AND(aggregates[s.aggregate] == "count" for s in select)
    if isSimple:
        return es_countop(es, query)  # SIMPLE, USE TERMS FACET INSTEAD


    value2facet = dict()  # ONLY ONE FACET NEEDED PER
    name2facet = dict()   # MAP name TO FACET WITH STATS

    for s in select:
        if s.value not in value2facet:
            if MVEL.isKeyword(s.value):
                unwrap(esQuery.facets)[s.name] = {
                    "statistical": {
                        "field": s.value
                    },
                    "facet_filter": simplify(query.where)
                }
            else:
                unwrap(esQuery.facets)[s.name] = {
                    "statistical": {
                        "script": mvel.compile_expression(s.value, query)
                    },
                    "facet_filter": simplify(query.where)
                }
            value2facet[s.value] = s.name
        name2facet[s.name] = value2facet[s.value]

    data = es_query_util.post(es, esQuery, query.limit)

    matricies = {s.name: Matrix(value=fix_es_stats(data.facets[literal_field(s.name)])[aggregates[s.aggregate]]) for s in select}
    cube = Cube(query.select, [], matricies)
    cube.frum = query
    return cube



def es_countop(es, mvel, query):
    """
    RETURN SINGLE COUNT
    """
    select = listwrap(query.select)
    esQuery = buildESQuery(query)
    for s in select:

        if MVEL.isKeyword(s.value):
            esQuery.facets[s.name] = {
                "terms": {
                    "field": s.value,
                    "size": query.limit,
                },
                "facet_filter":{"exists":{"field":s.value}}
            }
        else:
            # COMPLICATED value IS PROBABLY A SCRIPT, USE IT
            esQuery.facets[s.name] = {
                "terms": {
                    "script_field": mvel.compile_expression(s.value, query),
                    "size": 200000
                }
            }

    data = es_query_util.post(es, esQuery, query.limit)

    matricies = {}
    for s in select:
        matricies[s.name] = Matrix(value=data.hits.facets[s.name].total)

    cube = Cube(query.select, query.edges, matricies)
    cube.frum = query
    return cube
