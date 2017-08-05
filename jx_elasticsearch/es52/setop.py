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

from jx_elasticsearch import es52, es09
from mo_dots import coalesce, split_field, set_default, Data, unwraplist, literal_field, unwrap, wrap, \
    concat_field, startswith_field, relative_field, join_field
from mo_dots import listwrap
from mo_logs import Log
from mo_math import AND
from mo_math import MAX

from jx_base.expressions import jx_expression_to_function
from jx_elasticsearch.es52.expressions import Variable, LeavesOp
from jx_elasticsearch.es52.util import jx_sort_to_es_sort
from jx_python.containers import STRUCT
from jx_python.containers.cube import Cube
from jx_python.domains import ALGEBRAIC
from jx_python.query import DEFAULT_LIMIT
from mo_collections.matrix import Matrix
from mo_dots.lists import FlatList
from mo_json.typed_encoder import decode_property
from mo_times.timer import Timer

format_dispatch = {}


def is_setop(es, query):
    if not any(map(es.cluster.version.startswith, ["1.4.", "1.5.", "1.6.", "1.7.", "5.2."])):
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
    schema = query.frum.schema
    map_to_es_columns = {c.names["."]: c.es_column for c in schema.leaves(".")}
    query_for_es = query.map(map_to_es_columns)

    es_query, filters = es52.util.es_query_template(query.frum.name)
    set_default(filters[0], query_for_es.where.partial_eval().to_esfilter())
    es_query.size = coalesce(query.limit, DEFAULT_LIMIT)
    es_query.stored_fields = FlatList()

    selects = wrap([s.copy() for s in listwrap(query.select)])
    new_select = FlatList()
    schema = query.frum.schema
    columns = schema.columns
    nested_columns = set(c.names["."] for c in columns if c.nested_path[0] != ".")

    es_query.sort = jx_sort_to_es_sort(query_for_es.sort)

    put_index = 0
    for select, es_select in zip(selects, listwrap(query_for_es.select)):
        # IF THERE IS A *, THEN INSERT THE EXTRA COLUMNS
        if isinstance(select.value, LeavesOp):
            term = select.value.term
            if isinstance(term, Variable):
                for cname, cs in schema.lookup.items():
                    if startswith_field(cname, term.var) and cname != "_id":
                        for c in cs:
                            if c.type not in STRUCT:
                                es_query.stored_fields += [c.es_column]
                                if select.name == ".":
                                    new_name = literal_field(relative_field(cname, term.var))
                                else:
                                    new_name = select.name + "\\." + literal_field(relative_field(cname, term.var))
                                new_select.append({
                                    "name": new_name,
                                    "value": Variable(c.es_column, verify=False),
                                    "put": {"name": new_name, "index": put_index, "child": "."}
                                })
                                put_index += 1

        elif isinstance(select.value, Variable):
            if select.value.var == "_id":
                new_select.append({
                    "name": select.name,
                    "value": select.value,
                    "pull": jx_expression_to_function("_id"),
                    "put": {"name": select.name, "index": put_index, "child": "."}
                })
                put_index += 1
            elif select.value.var in nested_columns:
                nested_path = None
                wheres = es_query.query.bool.must
                for c in schema.leaves(select.value.var):
                    nested_path = coalesce(nested_path, c.nested_path)
                    if len(wheres) == 1:
                        where = Data()
                        wheres.append(where)
                    else:
                        where = wheres[1]
                    where.nested.path = c.nested_path[0]
                    where.nested.query.match_all = {}
                    where.nested.inner_hits._source = False
                    where.nested.inner_hits.stored_fields += [c.es_column]

                pull = accumulate_nested_doc(nested_path)
                new_select.append({
                    "name": select.name,
                    "value": es_select.value,
                    "put": {"name": select.name, "index": put_index, "child": "."},
                    "pull": pull
                })
                put_index += 1
            else:
                s_column = select.value.var
                # LEAVES OF OBJECT
                for cname, cs in schema.lookup.items():
                    if startswith_field(cname, s_column):
                        for c in cs:
                            if c.type not in STRUCT:
                                es_query.stored_fields += [c.es_column]
                                new_select.append({
                                    "name": select.name,
                                    "value": Variable(c.es_column, verify=False),
                                    "put": {"name": select.name, "index": put_index, "child": relative_field(cname, s_column)}
                                })
                put_index += 1
        else:
            es_query.script_fields[literal_field(select.name)] = {"script": select.value.map(map_to_es_columns).to_painless()}
            new_select.append({
                "name": select.name,
                "pull": jx_expression_to_function("fields." + literal_field(select.name)),
                "put": {"name": select.name, "index": put_index, "child": "."}
            })
            put_index += 1

    for n in new_select:
        if n.pull:
            continue
        elif isinstance(n.value, Variable):
            n.pull = jx_expression_to_function(concat_field("fields", literal_field(n.value.map(map_to_es_columns).var)))
        else:
            Log.error("Do not know what to do")

    with Timer("call to ES") as call_timer:
        Log.note("{{data}}", data=es_query)
        data = es09.util.post(es, es_query, query.limit)

    T = data.hits.hits

    try:
        formatter, groupby_formatter, mime_type = format_dispatch[query.format]

        output = formatter(T, new_select, query)
        output.meta.timing.es = call_timer.duration
        output.meta.content_type = mime_type
        output.meta.es_query = es_query
        return output
    except Exception as e:
        Log.error("problem formatting", e)


def accumulate_nested_doc(nested_path):
    """
    :param nested_path: THE PATH USED TO EXTRACT THE NESTED RECORDS 
    :return: THE DE_TYPED NESTED OBJECT ARRAY
    """
    def output(doc):
        acc = []
        for h in doc.inner_hits[nested_path[0]].hits.hits:
            i = h._nested.offset
            obj = Data()
            for f, v in h.fields.items():
                local_path = join_field(split_field(relative_field(decode_property(f), nested_path[0]))[:-1])
                obj[local_path] = unwraplist(v)
            # EXTEND THE LIST TO THE LENGTH WE REQUIRE
            for _ in range(len(acc), i+1):
                acc.append(None)
            acc[i] = obj
        return acc
    return output


def format_list(T, select, query=None):
    data = []
    if isinstance(query.select, list):
        for row in T:
            r = Data()
            for s in select:
                r[s.put.name][s.put.child] = unwraplist(s.pull(row))
            data.append(r if r else None)
    elif isinstance(query.select.value, LeavesOp):
        for row in T:
            r = Data()
            for s in select:
                r[s.put.name][s.put.child] = unwraplist(s.pull(row))
            data.append(r if r else None)
    else:
        for row in T:
            r = None
            for s in select:
                v = unwraplist(s.pull(row))
                if v is None:
                    continue
                if s.put.child == ".":
                    r = v
                else:
                    if r is None:
                        r = Data()
                    r[s.put.child] = v

            data.append(r)

    return Data(
        meta={"format": "list"},
        data=data
    )


def format_table(T, select, query=None):
    data = []
    num_columns = (MAX(select.put.index) + 1)
    for row in T:
        r = [None] * num_columns
        for s in select:
            value = unwraplist(s.pull(row))

            if value == None:
                continue

            index, child = s.put.index, s.put.child
            if child == ".":
                r[index] = value
            else:
                if r[index] is None:
                    r[index] = Data()
                r[index][child] = value

        data.append(r)

    header = [None] * num_columns
    for s in select:
        if header[s.put.index]:
            continue
        header[s.put.index] = s.name

    return Data(
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
