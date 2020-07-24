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

from jx_base.expressions import (
    FALSE,
    Variable as Variable_,
    MissingOp,
    Variable,
    is_literal,
    TRUE,
    NULL,
    ConcatOp, IDENTITY, OuterJoinOp, NotOp, InnerJoinOp, Expression)
from jx_base.language import Language, is_op
from jx_elasticsearch.es52.painless import Painless
from jx_elasticsearch.es52.painless.es_script import es_script
from mo_dots import Null, to_data, join_field, split_field, coalesce
from mo_future import first
from mo_imports import expect
from mo_json import EXISTS
from mo_json.typed_encoder import EXISTS_TYPE, NESTED_TYPE
from mo_logs import Log
from mo_math import MAX

MATCH_NONE, MATCH_ALL, AndOp, OrOp, NestedOp, = expect(
    "MATCH_NONE", "MATCH_ALL", "AndOp", "OrOp", "NestedOp",
)


def _inequality_to_esfilter(self, schema):
    if is_op(self.lhs, Variable_) and is_literal(self.rhs):
        cols = schema.leaves(self.lhs.var)
        if not cols:
            lhs = self.lhs.var  # HAPPENS DURING DEBUGGING, AND MAYBE IN REAL LIFE TOO
        elif len(cols) == 1:
            lhs = first(cols).es_column
        else:
            raise Log.error(
                "operator {{op|quote}} does not work on objects", op=self.op
            )
        return {"range": {lhs: {self.op: self.rhs.value}}}
    else:
        script = Painless[self].to_es_script(schema)
        if script.miss is not FALSE:
            Log.error("inequality must be decisive")
        return {"script": es_script(script.expr)}


def split_expression_by_depth(where, schema, output=None, var_to_depth=None):
    """
    :param where: EXPRESSION TO INSPECT
    :param schema: THE SCHEMA
    :param output:
    :param var_to_depth: MAP FROM EACH VARIABLE NAME TO THE DEPTH
    :return:
    """
    """
    It is unfortunate that ES can not handle expressions that
    span nested indexes.  This will split your where clause
    returning {"and": [filter_depth0, filter_depth1, ...]}
    """
    vars_ = where.vars()

    if var_to_depth is None:
        if not vars_:
            return Null
        # MAP VARIABLE NAMES TO HOW DEEP THEY ARE
        var_to_depth = {
            v.var: max(len(c.nested_path) - 1, 0) for v in vars_ for c in schema[v.var]
        }
        all_depths = set(var_to_depth.values())
        if len(all_depths) == 0:
            all_depths = {0}
        output = to_data([[] for _ in range(MAX(all_depths) + 1)])
    else:
        all_depths = set(var_to_depth[v.var] for v in vars_)

    if len(all_depths) == 1:
        output[first(all_depths)] += [where]
    elif is_op(where, AndOp):
        for a in where.terms:
            split_expression_by_depth(a, schema, output, var_to_depth)
    else:
        Log.error("Can not handle complex where clause")

    return output


def exists_variable(path):
    """
    RETURN THE VARIABLE THAT WILL INDICATE OBJECT (OR ARRAY) EXISTS (~e~)
    """
    steps = split_field(path)
    if not steps:
        return EXISTS_TYPE
    if steps[-1] == NESTED_TYPE:
        steps = steps[:-1]
    return join_field(steps + [EXISTS_TYPE])


def setop_to_es_queries(query, split_select):
    frum = query.frum
    schema = frum.schema
    where = query.where

    # MAP TO es_columns, INCLUDE NESTED EXISTENCE IN EACH VARIABLE
    expr_vars = where.vars()
    var_to_columns = {v.var: schema.values(v.var) for v in expr_vars}


    # FROM DEEPEST TO SHALLOWEST
    all_paths = list(
        reversed(
            sorted(
                set(c.nested_path[0] for v in expr_vars for c in var_to_columns[v.var])
                | {"."}
                | set(schema.query_path)
                | set(split_select.keys())
            )
        )
    )

    wheres = [where]

    # WE DO THIS EXPANSION TO CAPTURE A VARIABLE OVER DIFFERENT NESTED LEVELS
    # EXPAND VARS TO COLUMNS, MULTIPLY THE EXPRESSIONS
    for v, cols in var_to_columns.items():
        more_exprs = []
        if not cols:
            for e in wheres:
                more_exprs.append(e.map({v: NULL}))
        else:
            for c in cols:
                deepest = c.nested_path[0]
                for e in wheres:
                    if deepest == ".":
                        more_exprs.append(e.map({v: Variable(c.es_column)}))
                    else:
                        more_exprs.append(
                            e.map(
                                {
                                    v: NestedOp(
                                        path=Variable(deepest),
                                        select=Variable(c.es_column),
                                        where=Variable(c.es_column).exists()
                                    )
                                }
                            )
                        )
        wheres = more_exprs
        var_to_columns = {
            c.es_column: [c]
            for cs in var_to_columns.values()
            for c in cs
        }


    concat_outer = query_to_outer_joins(frum, OrOp(wheres), all_paths, var_to_columns, split_select)

    # SPLIT COLUMNS BY DEPTH
    paths_to_cols = OrderedDict((n, []) for n in all_paths)
    for v, cs in var_to_columns.items():
        for c in cs:
            paths_to_cols[c.nested_path[0]].append(c)

    # JSON QUERY EXPRESSIONS ASSUME OUTER JOIN
    # ES ONLY HAS INNER JOIN
    # ACCOUNT FOR WHEN NESTED RECORDS ARE MISSING
    def outer_to_inner(expr):
        if is_op(expr, ConcatOp):
            output = []
            for outer in expr.terms:
                for inner in outer_to_inner(outer).terms:
                    output.append(inner)
            return ConcatOp(output)
        elif is_op(expr, OuterJoinOp):
            # THE MAIN INNER JOIN
            output = [InnerJoinOp(expr.frum, expr.nests)]
            # ALL THE OUTER JOIN RESIDUES
            for deepest in expr.nests[:-1]:  # LAST '.' NOT NEEDED
                deepest_path = deepest.path.var
                inner_join = InnerJoinOp(expr.frum, [])
                deeper_conditions = TRUE
                for nest in expr.nests:
                    nest_path = nest.path.var
                    if len(nest_path) < len(deepest_path):
                        new_nest = NestedOp(
                            path=nest.path,
                            select=nest.select,
                            where=AndOp([deeper_conditions, nest.where]),
                            sort=nest.sort,
                            limit=nest.limit
                        )
                        inner_join.nests.append(new_nest)
                        deeper_conditions = TRUE
                    elif nest_path == deepest_path:
                        # assume the deeper is null
                        set_null = {d.es_column: NULL for d in paths_to_cols[deepest_path]}
                        set_null[deepest_path] = NULL
                        deeper_exists = nest.where.map(set_null).partial_eval()

                        if deeper_exists is FALSE:
                            # WHERE CAN NOT BE SATISFIED IF NESTED IS NULL
                            deeper_conditions = FALSE
                        else:
                            # ENSURE THIS IS NOT "OPTIMIZED" TO FALSE
                            deeper_conditions = NotOp(NestedOp(path=Variable(nest_path), where=TRUE))
                            deeper_conditions.simplified = True

                inner_join = inner_join.partial_eval()
                if inner_join.missing() is not TRUE:
                    output.append(inner_join)
            return ConcatOp(output)
        else:
            Log.error("do not know what to do yet")
    concat_inner = outer_to_inner(concat_outer)

    es_query = [ES52[t.partial_eval()].to_es(schema) for t in concat_inner.terms]

    return es_query


def query_to_outer_joins(frum, expr, all_paths, var_to_columns, split_select):
    """
    CONVERT FROM JSON QUERY EXPRESSION TO A NUMBER OF OUTER JOINS
    :param frum:
    :param expr:
    :param all_paths:
    :param var_to_columns:
    :return:
    """

    def split(expr):
        """
        :param expr: JSON EXPRESSION
        :return: ARRAY INDEX BY (CONCAT, OUTER JOIN, AND)
        """
        expr = expr.partial_eval()

        if is_op(expr, AndOp):
            acc = [tuple([] for _ in all_paths)]
            for t in expr.terms:
                next = []
                for c in split(t):
                    for a in acc:
                        next.append(tuple(n + an for n, an in zip(c, a)))
                acc = next
            return acc
        elif is_op(expr, OrOp):
            output = []
            exclude = []
            for t in expr.terms:
                for c in split(AndOp([AndOp(exclude), t])):
                    output.append(c)
                exclude.append(NotOp(t))
            return output

        all_nests = list(
            set(
                c.nested_path[0]
                for v in expr.vars()
                for c in frum.schema.values(v.var)
            )
        )

        if len(all_nests) > 1:
            Log.error("do not know how to handle")
        elif not all_nests:
            return [tuple([expr] if p == '.' else [] for p in all_paths)]
        else:
            return [tuple([expr] if p == all_nests[0] else [] for p in all_paths)]

    concat_outer_and = split(expr)

    # ATTACH SELECTS
    output = ConcatOp([])
    for concat in concat_outer_and:
        outer = OuterJoinOp(frum, [])
        for p, nest in zip(all_paths, concat):
            select = coalesce(split_select.get(p), NULL)
            outer.nests.append(NestedOp(Variable(p), select=select, where = AndOp(nest)))
        output.terms.append(outer)

    return output


def split_expression_by_path(expr, schema, lang=Language):
    """
    :param expr: EXPRESSION TO INSPECT
    :param schema: THE SCHEMA
    :param output: THE MAP FROM PATH TO EXPRESSION WE WANT UPDATED
    :param var_to_columns: MAP FROM EACH VARIABLE NAME TO THE DEPTH
    :return: type, output: (OP, MAP) PAIR WHERE OP IS OPERATOR TO APPLY ON MAP ITEMS, AND MAP FROM PATH TO EXPRESSION
    """
    if is_op(expr, AndOp):
        if not expr.terms:
            return AndOp, {".": TRUE}
        elif len(expr.terms) == 1:
            return split_expression_by_path(expr.terms[0], schema, lang=lang)

        output = {}
        curr_op = AndOp
        for w in expr.terms:
            op, split = split_expression_by_path(w, schema, lang=lang)
            if op == AndOp:
                for v, es in split.items():
                    ae = output.get(v)
                    if not ae:
                        output[v] = ae = AndOp([])
                    ae.terms.append(es)
            elif len(output) == 1 and all(
                c.jx_type == EXISTS
                for v in split["."].vars()
                for c in schema.values(v.var)
            ):
                for v, es in split.items():
                    if v == ".":
                        continue
                    ae = output.get(v)
                    if not ae:
                        output[v] = ae = AndOp([])
                    ae.terms.append(es)
            else:
                Log.error("confused")
        return curr_op, output

    expr_vars = expr.vars()
    var_to_columns = {v.var: schema.values(v.var) for v in expr_vars}
    all_paths = set(c.nested_path[0] for v in expr_vars for c in var_to_columns[v.var])

    def add(v, c):
        cols = var_to_columns.get(v)
        if not cols:
            var_to_columns[v] = cols = []
        if c not in cols:
            cols.append(c)

    # all_paths MAY BE MISSING SHALLOW PATHS
    exprs = [expr]
    undo = {}
    for p in schema.query_path:
        # CALCULATE THE RESIDUAL EXPRESSION
        # REPLACE EACH DEEPER VAR WITH null
        # TODO: NOT ACCOUNTING FOR DEEP QUERIES ON SHALLOW TABLE
        mapping = {
            v: c
            for v, cols in var_to_columns.items()
            for c in cols
            if len(c.nested_path[0]) > len(p)
        }
        if mapping:
            acc = []
            for v, col in mapping.items():
                nested_exists = join_field(
                    split_field(col.nested_path[0])[:-1] + [EXISTS_TYPE]
                )
                e = schema.values(nested_exists)
                if not e:
                    Log.error("do not know how to handle")
                add(nested_exists, first(e))  # REGISTER THE EXISTENCE VARIABLE
                acc.append(MissingOp(Variable(nested_exists)))
            acc.append(expr.map({v: NULL for v in mapping.keys()}))
            with_nulls = AndOp(acc).partial_eval()
            if with_nulls is not FALSE:
                all_paths.add(p)
                exprs.append(with_nulls)

    if len(all_paths) == 0:
        return AndOp, {".": expr}  # CONSTANTS
    elif len(all_paths) == 1:
        return AndOp, {first(all_paths): expr}

    # EXPAND EXPRESSION TO ALL REALIZED COLUMNS
    for v, cols in list(var_to_columns.items()):
        for col in cols:
            add(col.es_column, col)
        if len(cols) <= 1:
            continue

        more_expr = []
        for e in exprs:
            for col in cols:
                more_expr.append(e.map({v: col.es_column}))
        exprs = more_expr

    acc = {}
    for e in exprs:
        nestings = list(
            set(c.nested_path[0] for v in e.vars() for c in var_to_columns[v])
        )
        if not nestings:
            a = acc.get(".")
            if not a:
                acc["."] = a = OrOp([])
            a.terms.append(e)
        elif len(nestings) == 1:
            a = acc.get(nestings[0])
            if not a:
                acc[nestings[0]] = a = OrOp([])
            a.terms.append(e)
        else:
            Log.error("Expression is too complex")

    if undo:
        return OrOp, {k: v.map(undo) for k, v in acc.items()}
    else:
        return OrOp, acc


def get_type(var_name):
    type_ = var_name.split(".$")[1:]
    if not type_:
        return "j"
    return json_type_to_es_script_type.get(type_[0], "j")


json_type_to_es_script_type = {"string": "s", "boolean": "b", "number": "n"}


ES52 = Language("ES52")
