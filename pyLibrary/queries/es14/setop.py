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
from copy import copy
from pyLibrary import queries, convert

from pyLibrary.collections.matrix import Matrix
from pyLibrary.collections import AND, SUM, OR, UNION
from pyLibrary.dot import coalesce, split_field, set_default, Dict, unwraplist, unwrap, literal_field
from pyLibrary.dot.lists import DictList
from pyLibrary.dot import listwrap
from pyLibrary.queries.domains import is_keyword
from pyLibrary.queries import domains
from pyLibrary.queries.expressions import qb_expression_to_esfilter, simplify_esfilter, TRUE_FILTER, qb_expression_to_ruby
from pyLibrary.debugs.logs import Log
from pyLibrary.queries.containers.cube import Cube
from pyLibrary.queries.es14.util import aggregates1_4, qb_sort_to_es_sort
from pyLibrary.queries.query import DEFAULT_LIMIT
from pyLibrary.queries.unique_index import UniqueIndex
from pyLibrary.times.timer import Timer
from pyLibrary.queries import es14, es09



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
    source = "fields"
    select = copy(listwrap(query.select))  # ADD FIELDS TO select IF REQUIRED
    columns = query.frum.get_columns()

    for s in listwrap(query.select):
        if s.value == "*":
            es_query.fields=None
            source = "_source"
        elif s.value == ".":
            es_query.fields=None
            source = "_source"
        elif isinstance(s.value, basestring) and is_keyword(s.value):
            match = columns.filter(lambda c: c.name == s.value and c.type=="object")[0]
            if match:
                select = select.remove(s)
                leaves = columns.filter(lambda c: c.name.startswith(s.value + ".") and c.type not in ["object", "nested"] and coalesce(c.depth, 0)<=coalesce(match.depth, 0))
                for l in leaves:
                    select.append({"name": s.name + l.name[len(s.value)::], "value": l.name})
                es_query.fields.extend(leaves.name)
            else:
                es_query.fields.append(s.value)
        elif isinstance(s.value, list) and es_query.fields is not None:
            es_query.fields.extend(s.value)
        elif isinstance(s.value, Mapping) and es_query.fields is not None:
            es_query.fields.extend(s.value.values())
        elif es_query.fields is not None:
            es_query.fields.append(s.value)
    es_query.sort = qb_sort_to_es_sort(query.sort)

    return extract_rows(es, es_query, source, select, query)


def extract_rows(es, es_query, source, select, query):
    with Timer("call to ES") as call_timer:
        data = es09.util.post(es, es_query, query.limit)

    T = data.hits.hits
    for i, s in enumerate(select.copy()):
        # IF THERE IS A *, THEN INSERT THE EXTRA COLUMNS
        if s.value == "*":
            try:
                column_names = set(c.name for c in query.frum.get_columns() if (c.type not in ["object"] or c.useSource) and not c.depth)
            except Exception, e:
                Log.warning("can not get columns", e)
                column_names = UNION(*[[k for k, v in row.items()] for row in T.select(source)])
            column_names -= set(select.name)
            select = select[:i:] + [{"name": n, "value": n} for n in column_names] + select[i + 1::]
            break

    try:
        formatter, groupby_formatter, mime_type = format_dispatch[query.format]

        output = formatter(T, select, source)
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
    es_query[es_filter]=simplify_esfilter(qb_expression_to_esfilter(query.where))
    es_query.size = coalesce(query.limit, queries.query.DEFAULT_LIMIT)
    es_query.fields = DictList()
    es_query.sort = qb_sort_to_es_sort(query.sort)

    source = "fields"
    select = listwrap(query.select)
    for s in select:
        if s.value == "*":
            es_query.fields = None
            es_query.script_fields = None
            source = "_source"
        elif s.value == ".":
            es_query.fields = None
            es_query.script_fields = None
            source = "_source"
        elif isinstance(s.value, basestring) and is_keyword(s.value):
            es_query.fields.append(s.value)
        elif isinstance(s.value, list) and es_query.fields is not None:
            es_query.fields.extend([v for v in s.value])
        else:
            es_query.script_fields[literal_field(s.name)] = {"script": qb_expression_to_ruby(s.value)}

    return extract_rows(es, es_query, source, select, query)


def format_list(T, select, source):
    data = []
    for row in T:
        r = Dict()
        for s in select:
            if s.value == ".":
                r[s.name] = row[source]
            else:
                if source=="_source":
                    r[s.name] = unwraplist(row[source][s.value])
                elif isinstance(s.value, basestring):  # fields
                    r[s.name] = unwraplist(row[source][literal_field(s.value)])
                else:
                    r[s.name] = unwraplist(row[source][literal_field(s.name)])
        data.append(r if r else None)
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
                if source == "_source":
                    r[map[s.name]] = unwraplist(row[source][s.value])
                elif isinstance(s.value, basestring):  # fields
                    r[map[s.name]] = unwraplist(row[source][literal_field(s.value)])
                else:
                    r[map[s.name]] = unwraplist(row[source][literal_field(s.name)])
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
            elif isinstance(s.value, list):
                matricies[s.name] = Matrix.wrap([tuple(unwraplist(t[source][ss]) for ss in s.value) for t in T])
            else:
                if source == "_source":
                    matricies[s.name] = Matrix.wrap([unwraplist(t[source][s.value]) for t in T])

                elif isinstance(s.value, basestring):  # fields
                    matricies[s.name] = Matrix.wrap([unwraplist(t[source].get(s.value)) for t in T])
                else:
                    matricies[s.name] = Matrix.wrap([unwraplist(t[source].get(s.name)) for t in T])
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
