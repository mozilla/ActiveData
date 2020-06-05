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
    Variable as Variable_, MissingOp, Variable, ExistsOp)
from jx_base.expressions.literal import is_literal, TRUE, NULL, Literal, ZERO
from jx_base.language import Language, is_op
from jx_elasticsearch.es52.painless import Painless
from jx_elasticsearch.es52.painless.es_script import es_script
from mo_dots import Null, to_data, join_field, split_field
from mo_future import first
from mo_json import EXISTS
from mo_json.typed_encoder import EXISTS_TYPE, NESTED_TYPE
from mo_logs import Log
from mo_math import MAX

MATCH_NONE, MATCH_ALL, Painlesss, AndOp, OrOp = [Null] * 5  # IMPORTS


def _inequality_to_esfilter(self, schema):
    if is_op(self.lhs, Variable_) and is_literal(self.rhs):
        cols = schema.leaves(self.lhs.var)
        if not cols:
            lhs = self.lhs.var  # HAPPENS DURING DEBUGGING, AND MAYBE IN REAL LIFE TOO
        elif len(cols) == 1:
            lhs = first(cols).es_column
        else:
            raise Log.error("operator {{op|quote}} does not work on objects", op=self.op)
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


def get_exists(path):
    steps = split_field(path)
    if not steps:
        return EXISTS_TYPE
    if steps[-1] == NESTED_TYPE:
        steps = steps[:-1]
    return join_field(steps + [EXISTS_TYPE])


def split_expression_by_path_for_setop(expr, schema, more_path=tuple()):

    # MAP TO es_columns, INCLUDE NESTED EXISTENCE IN EACH VARIABLE
    expr_vars = expr.vars()
    var_to_columns = {v.var: schema.values(v.var) for v in expr_vars}

    all_paths = list(reversed(sorted(
        set(c.nested_path[0] for v in expr_vars for c in var_to_columns[v.var]) |
        {'.'} |
        set(schema.query_path) |
        set(more_path)
    )))

    exprs = [expr]

    # CONSTANTS
    more_exprs = []
    for e in exprs:
        for i, np in enumerate(all_paths):
            existence = Variable(get_exists(np))
            if not i:
                more_exprs.append(AndOp([ExistsOp(existence), e]))
            more_exprs.append(AndOp([MissingOp(existence), e]))
    exprs = more_exprs

    # EXPAND VARIABLES
    for v, cols in var_to_columns.items():
        more_exprs = []
        for e in exprs:
            for col in cols:
                for i, np in enumerate(col.nested_path):
                    existence = Variable(get_exists(np))
                    if not i:
                        more_exprs.append(AndOp([ExistsOp(existence), e.map({v: col.es_column})]))
                    more_exprs.append(AndOp([MissingOp(existence), e.map({v: NULL})]))
        exprs = more_exprs

    # SIMPLIFY
    simpler = OrOp(exprs).partial_eval()
    if is_op(simpler, OrOp):
        remain = simpler.terms
    else:
        remain = [simpler]

    # FACTOR OUT THE existence, DEEP FIRST
    depths = OrderedDict()
    for p in all_paths[:-1]:
        exists = depths[p] = []
        missing = []
        existence = get_exists(p)
        miss = MissingOp(Variable(existence))
        for e in remain:
            if AndOp([miss, e]).partial_eval() is FALSE:
                exists.append(e.map({existence: ZERO}))
            else:
                missing.append(e)
        remain = missing
    depths['.'] = [r.map({get_exists('.'): ZERO}) for r in remain]

    output = OrderedDict((k, OrOp(v).partial_eval()) for k, v in depths.items())
    Log.note("{{expr|json}}", expr=output)
    return OrOp, output


def split_expression_by_path(
    expr, schema, lang=Language
):
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
            elif len(output) == 1 and all(c.jx_type == EXISTS for v in split['.'].vars() for c in schema.values(v.var)):
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
                nested_exists = join_field(split_field(col.nested_path[0])[:-1] + [EXISTS_TYPE])
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
        return AndOp, {'.': expr}  # CONSTANTS
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
        nestings = list(set(c.nested_path[0] for v in e.vars() for c in var_to_columns[v]))
        if not nestings:
            a = acc.get('.')
            if not a:
                acc['.'] = a = OrOp([])
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
