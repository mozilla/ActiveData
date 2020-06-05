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

from jx_base.domains import ALGEBRAIC
from jx_base.expressions import LeavesOp, Variable, IDENTITY, TRUE, NULL, FALSE, MissingOp
from jx_base.language import is_op
from jx_base.query import DEFAULT_LIMIT
from jx_elasticsearch.es52.expressions import (
    AndOp,
    ES52,
    split_expression_by_path,
    EsNestedOp,
    OrOp)
from jx_elasticsearch.es52.expressions._utils import split_expression_by_path_for_setop
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
    unwraplist,
    dict_to_data,
    Null,
    to_data, list_to_data)
from mo_future import first, text
from mo_json import NESTED, INTERNAL
from mo_json.typed_encoder import decode_property, unnest_path, untype_path, untyped, EXISTS_TYPE
from mo_logs import Log
from mo_math import AND
from mo_times.timer import Timer

DEBUG = False


def is_setop(es, query):
    select = listwrap(query.select)

    if not query.edges:
        isDeep = (
            len(split_field(query.frum.name)) > 1
        )  # LOOKING INTO NESTED WILL REQUIRE A SCRIPT
        simpleAgg = AND(
            [s.aggregate in ("count", "none") for s in select]
        )  # CONVERTING esfilter DEFINED PARTS WILL REQUIRE SCRIPT

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
    split_select = {".": ESSelect(".")}

    def get_select(path):
        es_select = split_select.get(path)
        if not es_select:
            es_select = split_select[path] = ESSelect(path)
        return es_select

    selects = list_to_data([unwrap(s.copy()) for s in listwrap(query.select)])
    new_select = FlatList()
    put_index = 0
    for select in selects:
        # IF THERE IS A *, THEN INSERT THE EXTRA COLUMNS
        if is_op(select.value, LeavesOp) and is_op(select.value.term, Variable):
            term = select.value.term
            leaves = schema.leaves(term.var)
            for c in leaves:
                full_name = concat_field(
                    select.name, relative_field(untype_path(c.name), term.var)
                )
                if c.jx_type == NESTED:
                    get_select(".").set_op = True
                    new_select.append(
                        {
                            "name": full_name,
                            "value": Variable(c.es_column),
                            "put": {
                                "name": literal_field(full_name),
                                "index": put_index,
                                "child": ".",
                            },
                            "pull": get_pull_source(c.es_column),
                        }
                    )
                    put_index += 1
                else:
                    get_select(c.nested_path[0]).fields.append(c.es_column)
                    new_select.append(
                        {
                            "name": full_name,
                            "value": Variable(c.es_column),
                            "put": {
                                "name": literal_field(full_name),
                                "index": put_index,
                                "child": ".",
                            },
                        }
                    )
                    put_index += 1
        elif is_op(select.value, Variable):
            s_column = select.value.var

            if s_column == ".":
                # PULL ALL SOURCE
                get_select(".").set_op = True
                new_select.append(
                    {
                        "name": select.name,
                        "value": select.value,
                        "put": {"name": select.name, "index": put_index, "child": "."},
                        "pull": get_pull_source("."),
                    }
                )
                continue

            leaves = schema.leaves(s_column)  # LEAVES OF OBJECT
            # nested_selects = {}
            if leaves:
                if any(c.jx_type == NESTED for c in leaves):
                    # PULL WHOLE NESTED ARRAYS
                    get_select(".").set_op = True
                    for c in leaves:
                        if (
                            len(c.nested_path) == 1
                        ):  # NESTED PROPERTIES ARE IGNORED, CAPTURED BY THESE FIRST LEVEL PROPERTIES
                            pre_child = join_field(
                                decode_property(n) for n in split_field(c.name)
                            )
                            new_select.append(
                                {
                                    "name": select.name,
                                    "value": Variable(c.es_column),
                                    "put": {
                                        "name": select.name,
                                        "index": put_index,
                                        "child": untype_path(
                                            relative_field(pre_child, s_column)
                                        ),
                                    },
                                    "pull": get_pull_source(c.es_column),
                                }
                            )
                else:
                    # PULL ONLY WHAT'S NEEDED
                    for c in leaves:
                        c_nested_path = c.nested_path[0]
                        if c_nested_path == ".":
                            if c.es_column == "_id":
                                new_select.append(
                                    {
                                        "name": select.name,
                                        "value": Variable(c.es_column),
                                        "put": {
                                            "name": select.name,
                                            "index": put_index,
                                            "child": ".",
                                        },
                                        "pull": lambda row: row._id,
                                    }
                                )
                            elif c.jx_type == NESTED:
                                get_select(".").set_op = True
                                pre_child = join_field(
                                    decode_property(n) for n in split_field(c.name)
                                )
                                new_select.append(
                                    {
                                        "name": select.name,
                                        "value": Variable(c.es_column),
                                        "put": {
                                            "name": select.name,
                                            "index": put_index,
                                            "child": untype_path(
                                                relative_field(pre_child, s_column)
                                            ),
                                        },
                                        "pull": get_pull_source(c.es_column),
                                    }
                                )
                            else:
                                get_select(c_nested_path).fields.append(c.es_column)
                                pre_child = join_field(
                                    decode_property(n) for n in split_field(c.name)
                                )
                                new_select.append(
                                    {
                                        "name": select.name,
                                        "value": Variable(c.es_column),
                                        "put": {
                                            "name": select.name,
                                            "index": put_index,
                                            "child": untype_path(
                                                relative_field(pre_child, s_column)
                                            ),
                                        },
                                    }
                                )
                        else:
                            es_select = get_select(c_nested_path)
                            es_select.fields.append(c.es_column)

                            child = relative_field(
                                untype_path(
                                    relative_field(c.name, schema.query_path[0])
                                ),
                                s_column,
                            )
                            pull = accumulate_nested_doc(
                                c_nested_path,
                                Variable(
                                    relative_field(s_column, unnest_path(c_nested_path))
                                ),
                            )
                            new_select.append(
                                {
                                    "name": select.name,
                                    "value": select.value,
                                    "put": {
                                        "name": select.name,
                                        "index": put_index,
                                        "child": child,
                                    },
                                    "pull": pull,
                                }
                            )
            else:
                new_select.append(
                    {
                        "name": select.name,
                        "value": Variable("$dummy"),
                        "put": {"name": select.name, "index": put_index, "child": "."},
                    }
                )
            put_index += 1
        else:
            op, split_scripts = split_expression_by_path(
                select.value, schema, lang=Painless
            )
            for p, script in split_scripts.items():
                es_select = get_select(p)
                es_select.scripts[select.name] = {
                    "script": text(
                        Painless[script].partial_eval().to_es_script(schema)
                    )
                }
                new_select.append(
                    {
                        "name": select.name,
                        "pull": jx_expression_to_function(
                            "fields." + literal_field(select.name)
                        ),
                        "put": {"name": select.name, "index": put_index, "child": "."},
                    }
                )
                put_index += 1
    for n in new_select:
        if n.pull:
            continue
        elif is_op(n.value, Variable):
            if get_select(".").set_op:
                n.pull = get_pull_source(n.value.var)
            elif n.value == "_id":
                n.pull = jx_expression_to_function("_id")
            else:
                n.pull = jx_expression_to_function(
                    concat_field("fields", literal_field(n.value.var))
                )
        else:
            Log.error("Do not know what to do")
    return new_select, split_select


def es_setop(es, query):
    schema = query.frum.schema

    new_select, split_select = get_selects(query)

    op, split_wheres = split_expression_by_path_for_setop(query.where, schema, split_select.keys())
    es_query = es_query_proto(split_select, op, split_wheres, schema)
    es_query.size = coalesce(query.limit, DEFAULT_LIMIT)
    es_query.sort = jx_sort_to_es_sort(query.sort, schema)

    with Timer("call to ES", verbose=DEBUG) as call_timer:
        result = es.search(es_query)

    T = result.hits.hits

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


def accumulate_nested_doc(nested_path, expr=IDENTITY):
    """
    :param nested_path: THE PATH USED TO EXTRACT THE NESTED RECORDS
    :param expr: FUNCTION USED ON THE NESTED OBJECT TO GET SPECIFIC VALUE
    :return: THE DE_TYPED NESTED OBJECT ARRAY
    """
    name = literal_field(nested_path)

    def output(doc):
        acc = []
        for h in doc.inner_hits[name].hits.hits:
            i = h._nested.offset
            obj = Data()
            for f, v in h.fields.items():
                local_path = untype_path(relative_field(f, nested_path))
                obj[local_path] = unwraplist(v)
            # EXTEND THE LIST TO THE LENGTH WE REQUIRE
            for _ in range(len(acc), i + 1):
                acc.append(None)
            acc[i] = expr(obj)
        return acc

    return output


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


def get_pull_source(es_column):
    def output(row):
        return untyped(row._source[es_column])

    return output


def get_pull_stats():
    return jx_expression_to_function(
        {
            "select": [
                {"name": "count", "value": "count"},
                {"name": "sum", "value": "sum"},
                {"name": "min", "value": "min"},
                {"name": "max", "value": "max"},
                {"name": "avg", "value": "avg"},
                {"name": "sos", "value": "sum_of_squares"},
                {"name": "std", "value": "std_deviation"},
                {"name": "var", "value": "variance"},
            ]
        }
    )


class ESSelect(object):
    """
    ACCUMULATE THE FIELDS WE ARE INTERESTED IN
    """

    def __init__(self, path):
        self.path = path
        self.set_op = False
        self.fields = []
        self.scripts = {}

    def to_es(self):
        return dict_to_data(
            {
                "_source": self.set_op,
                "stored_fields": self.fields if not self.set_op else None,
                "script_fields": self.scripts if self.scripts else None,
            }
        )

    def __data__(self):
        return self.to_es()


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
        es_query = EsNestedOp(Variable(p), query=es_where, select=select)
    return es_query.partial_eval().to_esfilter(schema)


expsected = {
    "_source": False,
    "from": 0,
    "query": {"bool": {"should": [
        {"bool": {"should": [{"exists": {"field": "a._a.v.~s~"}}]}},
        {"nested": {
            "inner_hits": {
                "_source": False,
                "size": 100000,
                "stored_fields": ["a._a.~N~.v.~s~"]
            },
            "path": "a._a.~N~",
            "query": {"match_all": {}}
        }}
    ]}},
    "size": 10,
    "sort": [],
    "stored_fields": ["o.~n~", "a._a.v.~s~"]
}
