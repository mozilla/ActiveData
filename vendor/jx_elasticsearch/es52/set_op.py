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

from collections import OrderedDict
from copy import copy

from jx_base.domains import ALGEBRAIC
from jx_base.expressions import LeavesOp, Variable, TRUE, NULL
from jx_base.expressions.query_op import DEFAULT_LIMIT
from jx_base.language import is_op
from jx_elasticsearch.es52.expressions import (
    split_expression_by_path,
    NestedOp,
    ESSelectOp,
)
from jx_elasticsearch.es52.expressions.utils import setop_to_es_queries, pre_process
from jx_elasticsearch.es52.painless import Painless
from jx_elasticsearch.es52.set_format import set_formatters
from jx_elasticsearch.es52.util import jx_sort_to_es_sort
from jx_python.expressions import jx_expression_to_function
from mo_dots import (
    Data,
    FlatList,
    coalesce,
    concat_field,
    join_field,
    listwrap,
    literal_field,
    relative_field,
    split_field,
    unwrap,
    Null,
    list_to_data,
    unwraplist)
from mo_future import text
from mo_json import NESTED, INTERNAL, OBJECT, EXISTS
from mo_json.typed_encoder import decode_property, untype_path, untyped
from mo_logs import Log
from mo_math import AND
from mo_times.timer import Timer

DEBUG = False


def is_setop(es, query):
    select = listwrap(query.select)

    if not query.edges:
        isDeep = len(split_field(query.frum.name)) > 1  # LOOKING INTO NESTED WILL REQUIRE A SCRIPT
        simpleAgg = AND([
            s.aggregate in ("count", "none") for s in select
        ])  # CONVERTING esfilter DEFINED PARTS WILL REQUIRE SCRIPT

        # NO EDGES IMPLIES SIMPLER QUERIES: EITHER A SET OPERATION, OR RETURN SINGLE AGGREGATE
        if simpleAgg or isDeep:
            return True
    else:
        isSmooth = AND(
            (e.domain.type in ALGEBRAIC and e.domain.interval == "none")
            for e in query.edges
        )
        if isSmooth:
            return True

    return False


def get_selects(query):
    schema = query.frum.schema
    query_level = len(schema.query_path)
    query_path = schema.query_path[0]
    # SPLIT select INTO ES_SELECT AND RESULTSET SELECT
    split_select = OrderedDict((p, ESSelectOp(p)) for p in schema.query_path)

    def expand_split_select(c_nested_path):
        es_select = split_select.get(c_nested_path)
        if not es_select:
            temp = [(k, v) for k, v in split_select.items()]
            split_select.clear()
            split_select.update({c_nested_path: ESSelectOp(c_nested_path)})
            split_select.update(temp)
        return split_select[c_nested_path]

    new_select = FlatList()
    post_expressions = {}

    selects = list_to_data([unwrap(s.copy()) for s in listwrap(query.select)])

    # WHAT PATH IS _source USED, IF ANY?
    for select in selects:
        # IF THERE IS A *, THEN INSERT THE EXTRA COLUMNS
        if is_op(select.value, LeavesOp) and is_op(select.value.term, Variable):
            term = select.value.term
            leaves = schema.leaves(term.var)
            if any(c.jx_type == NESTED for c in leaves):
                split_select["."].source_path = "."
        elif is_op(select.value, Variable):
            for selected_column in schema.values(
                select.value.var, exclude_type=(OBJECT, EXISTS)
            ):
                if selected_column.jx_type == NESTED:
                    expand_split_select(selected_column.es_column).source_path = selected_column.es_column
                    continue
                leaves = schema.leaves(selected_column.es_column)
                for c in leaves:
                    if c.jx_type == NESTED:
                        split_select[c.es_column].source_path = c.es_column

    # IF WE GET THE SOURCE FOR PARENT, WE ASSUME WE GOT SOURCE FOR CHILD
    source_path = None
    source_level = 0
    for level, es_select in enumerate(reversed(list(split_select.values()))):
        if source_path:
            es_select.source_path = source_path
        elif es_select.source_path:
            source_level = level + 1
            source_path = es_select.source_path

    def get_pull_source(c):
        nested_path = c.nested_path
        nested_level = len(nested_path)
        pos = text(nested_level)

        if nested_level <= query_level:
            if not source_level or nested_level < source_level:
                field = join_field([pos, "fields", c.es_column])
                return jx_expression_to_function(field)
            elif nested_level == source_level:
                field = relative_field(c.es_column, nested_path[0])

                def pull_source(row):
                    return untyped(row.get(pos, Null)._source[field])

                return pull_source
            else:
                field = relative_field(c.es_column, nested_path[0])

                def pull_property(row):
                    return untyped(row.get(pos, Null)[field])

                return pull_property
        else:
            pos = text(query_level)

            if not source_level or nested_level < source_level:
                # PULL FIELDS AND THEN AGGREGATE THEM
                value = jx_expression_to_function(join_field(["fields", c.es_column]))
                name = literal_field(nested_path[0])
                index = jx_expression_to_function("_nested.offset")
                def pull_nested_field(doc):
                    hits = doc.get(pos, Null).inner_hits[name].hits.hits
                    if not hits:
                        return []

                    temp = [(index(h), value(h)) for h in hits]
                    acc = [None] * len(temp)
                    for i, v in temp:
                        acc[i] = unwraplist(v)
                    return acc

                return pull_nested_field
            else:
                # PULL SOURCES
                value = jx_expression_to_function(concat_field(
                    "_source", relative_field(c.es_column, nested_path[0])
                ))
                name = literal_field(nested_path[0])
                index = jx_expression_to_function(join_field(
                    ["_nested"] * (len(c.nested_path) - 1) + ["offset"]
                ))

                def pull_nested_source(doc):
                    hits = doc.get(pos, Null).inner_hits[name].hits.hits
                    if not hits:
                        return []

                    temp = [(index(h), value(h)) for h in hits]
                    acc = [None] * len(temp)
                    for i, v in temp:
                        acc[i] = untyped(v)
                    return acc

                return pull_nested_source

    put_index = 0
    for select in selects:
        # IF THERE IS A *, THEN INSERT THE EXTRA COLUMNS
        if is_op(select.value, LeavesOp) and is_op(select.value.term, Variable):
            term = select.value.term
            leaves = schema.leaves(term.var)
            for c in leaves:
                c_nested_path = c.nested_path[0]
                simple_name = relative_field(c.es_column, query_path).lstrip(".")
                name = concat_field(select.name, untype_path(simple_name))
                put_name = concat_field(
                    select.name, literal_field(untype_path(simple_name))
                )
                split_select[c_nested_path].fields.append(c.es_column)
                new_select.append({
                    "name": name,
                    "value": Variable(c.es_column),
                    "put": {"name": put_name, "index": put_index, "child": ".",},
                    "pull": get_pull_source(c),
                })
                put_index += 1
        elif is_op(select.value, Variable):
            if select.value.var == ".":
                # PULL ALL SOURCE
                new_select.append({
                    "name": select.name,
                    "value": select.value,
                    "put": {"name": select.name, "index": put_index, "child": "."},
                    "pull": get_pull_source(Data(
                        es_column=query_path, nested_path=schema.query_path
                    )),
                })
                continue

            for selected_column in schema.values(
                select.value.var, exclude_type=(EXISTS, OBJECT)
            ):
                if selected_column.jx_type == NESTED:
                    new_select.append({
                        "name": select.name,
                        "value": select.value,
                        "put": {"name": select.name, "index": put_index, "child": "."},
                        "pull": get_pull_source(Data(
                            es_column=selected_column.es_column,
                            nested_path=(selected_column.es_column,)
                            + selected_column.nested_path,
                        )),
                    })
                    continue

                leaves = schema.leaves(
                    selected_column.es_column, exclude_type=INTERNAL
                )  # LEAVES OF OBJECT
                if leaves:
                    for c in leaves:
                        if c.es_column == "_id":
                            new_select.append({
                                "name": select.name,
                                "value": Variable(c.es_column),
                                "put": {
                                    "name": select.name,
                                    "index": put_index,
                                    "child": ".",
                                },
                                "pull": pull_id,
                            })
                            continue
                        c_nested_path = c.nested_path[0]
                        expand_split_select(c_nested_path).fields.append(c.es_column)
                        child = untype_path(relative_field(
                            c.es_column, selected_column.es_column,
                        ))
                        new_select.append({
                            "name": select.name,
                            "value": Variable(c.es_column),
                            "put": {
                                "name": select.name,
                                "index": put_index,
                                "child": child,
                            },
                            "pull": get_pull_source(c),
                        })

                else:
                    new_select.append({
                        "name": select.name,
                        "value": NULL,
                        "put": {"name": select.name, "index": put_index, "child": "."},
                    })
                put_index += 1
        else:
            op, split_scripts = split_expression_by_path(
                select.value, schema, lang=Painless
            )
            for p, script in split_scripts.items():
                es_select = split_select[p]
                es_select.scripts[select.name] = {"script": text(
                    Painless[script].partial_eval().to_es_script(schema)
                )}
                new_select.append({
                    "name": select.name,
                    "pull": jx_expression_to_function(join_field([
                        text(p),
                        "fields",
                        select.name,
                    ])),
                    "put": {"name": select.name, "index": put_index, "child": "."},
                })
                put_index += 1

    def inners(query_path, parent_pos):
        """
        :param query_path:
        :return:  ITERATOR OVER TUPLES ROWS AS TUPLES, WHERE  row[len(nested_path)] HAS INNER HITS
                  AND row[0] HAS post_expressions
        """
        pos = text(int(parent_pos) + 1)
        if not query_path:

            def base_case(row):
                extra = {}
                for k, e in post_expressions.items():
                    extra[k] = e(row)
                row["0"] = extra
                yield row

            return base_case

        if pos == "1":
            more = inners(query_path[:-1], "1")

            def first_case(results):
                for result in results:
                    for hit in result.hits.hits:
                        seed = {"0": None, pos: hit}
                        for row in more(seed):
                            yield row

            return first_case

        else:
            more = inners(query_path[:-1], pos)
            if source_path and source_path < query_path[-1]:
                rel_path = relative_field(query_path[-1], source_path)

                def source(acc):
                    for inner_row in acc[parent_pos][rel_path]:
                        acc[pos] = inner_row
                        for tt in more(acc):
                            yield tt

                return source
            else:
                path = literal_field(query_path[-1])

                def recurse(acc):
                    hits = acc[parent_pos].inner_hits[path].hits.hits
                    if hits:
                        for inner_row in hits:
                            acc[pos] = inner_row
                            for tt in more(acc):
                                yield tt
                    else:
                        for tt in more(acc):
                            yield tt

                return recurse

    return new_select, split_select, inners(schema.query_path, "0")


def es_setop(es, query):
    schema = query.frum.schema
    all_paths, split_decoders, var_to_columns = pre_process(query)
    new_select, split_select, flatten = get_selects(query)
    # THE SELECTS MAY BE REACHING DEEPER INTO THE NESTED RECORDS
    all_paths = list(reversed(sorted(set(split_select.keys()) | set(all_paths))))
    es_query = setop_to_es_queries(query, all_paths, split_select, var_to_columns)
    if not es_query:
        # NO QUERY TO SEND
        formatter, _, mime_type = set_formatters[query.format]
        output = formatter([], new_select, query)
        output.meta.content_type = mime_type
        output.meta.es_query = es_query
        return output

    size = coalesce(query.limit, DEFAULT_LIMIT)
    sort = jx_sort_to_es_sort(query.sort, schema)
    for q in es_query:
        q["size"] = size
        q["sort"] = sort

    with Timer("call to ES", verbose=DEBUG) as call_timer:
        results = es.multisearch(es_query)

    T = [copy(row) for row in flatten(results)]
    try:
        formatter, _, mime_type = set_formatters[query.format]

        with Timer("formatter", silent=True):
            output = formatter(T, new_select, query)
        output.meta.timing.es = call_timer.duration
        output.meta.content_type = mime_type
        output.meta.es_query = es_query
        return output
    except Exception as e:
        Log.error("problem formatting", e)


def pull_id(row):
    return (row["1"]._id,)


def get_pull(column):
    if column.nested_path[0] == ".":
        return concat_field("fields", literal_field(column.es_column))
    else:
        rel_name = relative_field(column.es_column, column.nested_path[0])
        return concat_field("_inner", rel_name)


def get_pull_function(column):
    func = jx_expression_to_function(get_pull(column))
    if column.jx_type in INTERNAL:
        return lambda doc: untyped(func(doc))
    else:
        return func


def get_pull_stats():
    return jx_expression_to_function({"select": [
        {"name": "count", "value": "count"},
        {"name": "sum", "value": "sum"},
        {"name": "min", "value": "min"},
        {"name": "max", "value": "max"},
        {"name": "avg", "value": "avg"},
        {"name": "sos", "value": "sum_of_squares"},
        {"name": "std", "value": "std_deviation"},
        {"name": "var", "value": "variance"},
    ]})


def es_query_proto(selects, op, wheres, schema):
    """
    RETURN AN ES QUERY
    :param selects: MAP FROM path TO ESSelect INSTANCE
    :param wheres: MAP FROM path TO LIST OF WHERE CONDITIONS
    :return: es_query
    """
    es_query = op.zero
    for p in reversed(sorted(set(wheres.keys()) | set(selects.keys()))):
        # DEEPEST TO SHALLOW
        where = wheres.get(p, TRUE)
        select = selects.get(p, Null)

        es_where = op([es_query, where])
        es_query = NestedOp(path=Variable(p), query=es_where, select=select)
    return es_query.partial_eval().to_es(schema)
