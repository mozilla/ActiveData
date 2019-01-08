# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from mo_future import is_text, is_binary
from jx_base.expressions import AddOp, AndOp, BaseBinaryOp, BaseInequalityOp, BasicIndexOfOp, BasicMultiOp, BasicSubstringOp, BetweenOp, BooleanOp, CaseOp, CoalesceOp, ConcatOp, CountOp, DateOp, DivOp, EqOp, ExistsOp, FALSE, FalseOp, FindOp, FloorOp, FromUnixOp, InOp, IntegerOp, LeavesOp, LeftOp, LengthOp, Literal, MaxOp, MinOp, MissingOp, NULL, NeOp, NotLeftOp, NotOp, NotRightOp, NullOp, NumberOp, ONE, OrOp, PrefixOp, RangeOp, RegExpOp, RightOp, SqlEqOp, SqlInstrOp, SqlSubstrOp, StringOp, SubOp, SuffixOp, TRUE, TrueOp, TupleOp, UnixOp, Variable, WhenOp, ZERO, builtin_ops, extend, simplified
from jx_base.queries import get_property_name
from jx_base.utils import is_op
from jx_sqlite import GUID, quoted_GUID
from mo_dots import Null, ROOT_PATH, coalesce, join_field, listwrap, relative_field, split_field, startswith_field, wrap
from mo_future import text_type, is_text
from mo_json import json2value, BOOLEAN, EXISTS, NESTED, OBJECT
from mo_logs import Log
from mo_math import is_number
from pyLibrary import convert
from pyLibrary.sql import SQL, SQL_AND, SQL_CASE, SQL_ELSE, SQL_EMPTY_STRING, SQL_END, SQL_FALSE, SQL_IS_NOT_NULL, SQL_IS_NULL, SQL_NULL, SQL_ONE, SQL_OR, SQL_THEN, SQL_TRUE, SQL_WHEN, SQL_ZERO, sql_coalesce, sql_concat, sql_iso, sql_list
from pyLibrary.sql.sqlite import quote_column, quote_value


@extend(Variable)
def to_sql(self, schema, not_null=False, boolean=False):
    if self.var == GUID:
        return wrap([{"name": ".", "sql": {"s": quoted_GUID}, "nested_path": ROOT_PATH}])
    vars = schema[self.var]
    if not vars:
        # DOES NOT EXIST
        return wrap([{"name": ".", "sql": {"0": SQL_NULL}, "nested_path": ROOT_PATH}])
    var_name = list(set(listwrap(vars).name))
    if len(var_name) > 1:
        Log.error("do not know how to handle")
    var_name = var_name[0]
    cols = schema.leaves(self.var)
    acc = {}
    if boolean:
        for col in cols:
            cname = relative_field(col.name, var_name)
            nested_path = col.nested_path[0]
            if col.type == OBJECT:
                value = SQL_TRUE
            elif col.type == BOOLEAN:
                value = quote_column(col.es_column)
            else:
                value = quote_column(col.es_column) + SQL_IS_NOT_NULL
            tempa = acc.setdefault(nested_path, {})
            tempb = tempa.setdefault(get_property_name(cname), {})
            tempb['b'] = value
    else:
        for col in cols:
            cname = relative_field(col.name, var_name)
            if col.type == OBJECT:
                prefix = self.var + "."
                for cn, cs in schema.items():
                    if cn.startswith(prefix):
                        for child_col in cs:
                            tempa = acc.setdefault(child_col.nested_path[0], {})
                            tempb = tempa.setdefault(get_property_name(cname), {})
                            tempb[json_type_to_sql_type[col.type]] = quote_column(child_col.es_column)
            else:
                nested_path = col.nested_path[0]
                tempa = acc.setdefault(nested_path, {})
                tempb = tempa.setdefault(get_property_name(cname), {})
                tempb[json_type_to_sql_type[col.type]] = quote_column(col.es_column)

    return wrap([
        {"name": cname, "sql": types, "nested_path": nested_path}
        for nested_path, pairs in acc.items() for cname, types in pairs.items()
    ])


@extend(Literal)
def to_sql(self, schema, not_null=False, boolean=False):
    value = self.value
    v = quote_value(value)
    if v == None:
        return wrap([{"name": "."}])
    elif is_text(value):
        return wrap([{"name": ".", "sql": {"s": quote_value(value)}}])
    elif is_number(v):
        return wrap([{"name": ".", "sql": {"n": quote_value(value)}}])
    elif v in [True, False]:
        return wrap([{"name": ".", "sql": {"b": quote_value(value)}}])
    else:
        return wrap([{"name": ".", "sql": {"j": quote_value(self.json)}}])


@extend(NullOp)
def to_sql(self, schema, not_null=False, boolean=False):
    return Null


@extend(TrueOp)
def to_sql(self, schema, not_null=False, boolean=False):
    return wrap([{"name": ".", "sql": {"b": SQL_TRUE}}])


@extend(FalseOp)
def to_sql(self, schema, not_null=False, boolean=False):
    return wrap([{"name": ".", "sql": {"b": SQL_FALSE}}])


@extend(DateOp)
def to_sql(self, schema, not_null=False, boolean=False):
    return wrap([{"name": ".", "sql": {"n": quote_value(self.value)}}])


@extend(TupleOp)
def to_sql(self, schema, not_null=False, boolean=False):
    return wrap([{"name": ".", "sql": t.to_sql(schema)[0].sql} for t in self.terms])


@extend(LeavesOp)
def to_sql(self, schema, not_null=False, boolean=False):
    if not is_op(self.term, Variable):
        Log.error("Can only handle Variable")
    term = self.term.var
    prefix_length = len(split_field(term))
    output = wrap([
        {
            "name": join_field(split_field(schema.get_column_name(c))[prefix_length:]),
            "sql": Variable(schema.get_column_name(c)).to_sql(schema)[0].sql
        }
        for c in schema.columns
        if startswith_field(c.name, term) and (
            (c.jx_type not in (EXISTS, OBJECT, NESTED) and startswith_field(schema.nested_path[0], c.nested_path[0])) or
            (c.jx_type not in (EXISTS, OBJECT) and schema.nested_path[0] == c.nested_path[0])
        )
    ])
    return output


@extend(EqOp)
def to_sql(self, schema, not_null=False, boolean=False):
    lhs = self.lhs.to_sql(schema)
    rhs = self.rhs.to_sql(schema)
    acc = []
    if len(lhs) != len(rhs):
        Log.error("lhs and rhs have different dimensionality!?")

    for l, r in zip(lhs, rhs):
        for t in "bsnj":
            if l.sql[t] == None:
                if r.sql[t] == None:
                    pass
                else:
                    acc.append(sql_iso(r.sql[t]) + SQL_IS_NULL)
            else:
                if r.sql[t] == None:
                    acc.append(sql_iso(l.sql[t]) + SQL_IS_NULL)
                else:
                    acc.append(sql_iso(l.sql[t]) + " = " + sql_iso(r.sql[t]))
    if not acc:
        return FALSE.to_sql(schema)
    else:
        return wrap([{"name": ".", "sql": {"b": SQL_OR.join(acc)}}])


@extend(EqOp)
@simplified
def partial_eval(self):
    lhs = self.lhs.partial_eval()
    rhs = self.rhs.partial_eval()

    if is_literal(lhs) and is_literal(rhs):
        return TRUE if builtin_ops["eq"](lhs.value, rhs.value) else FALSE
    else:
        rhs_missing = rhs.missing().partial_eval()
        return CaseOp(
            [
                WhenOp(lhs.missing(), **{"then": rhs_missing}),
                WhenOp(rhs_missing, **{"then": FALSE}),
                SqlEqOp([lhs, rhs])
            ]
        ).partial_eval()


@extend(NeOp)
def to_sql(self, schema, not_null=False, boolean=False):
    return NotOp(EqOp([self.lhs, self.rhs]).partial_eval()).partial_eval().to_sql(schema)


@extend(BasicIndexOfOp)
def to_sql(self, schema, not_null=False, boolean=False):
    value = self.value.to_sql(schema)[0].sql.s
    find = self.find.to_sql(schema)[0].sql.s
    start = self.start

    if is_literal(start) and start.value == 0:
        return wrap([{"name": ".", "sql": {"n": "INSTR" + sql_iso(value + "," + find) + "-1"}}])
    else:
        start_index = start.to_sql(schema)[0].sql.n
        found = "INSTR(SUBSTR" + sql_iso(value + "," + start_index + "+1)," + find)
        return wrap([{"name": ".", "sql": {"n": (
            SQL_CASE +
            SQL_WHEN + found +
            SQL_THEN + found + "+" + start_index + "-1" +
            SQL_ELSE + "-1" +
            SQL_END
        )}}])


@extend(BasicSubstringOp)
def to_sql(self, schema, not_null=False, boolean=False):
    value = self.value.to_sql(schema)[0].sql.s
    start = AddOp([self.start, Literal(1)]).partial_eval().to_sql(schema)[0].sql.n
    length = SubOp([self.end, self.start]).partial_eval().to_sql(schema)[0].sql.n

    return wrap([{"name": ".", "sql": {"s": "SUBSTR" + sql_iso(value + "," + start + ", " + length)}}])


@extend(BaseBinaryOp)
def to_sql(self, schema, not_null=False, boolean=False):
    op, zero = _sql_operators[self.op]
    lhs = self.lhs.to_sql(schema)[0].sql.n
    rhs = self.rhs.to_sql(schema)[0].sql.n

    return wrap([{"name": ".", "sql": {"n": sql_iso(lhs) + " " + op + " " + sql_iso(rhs)}}])


@extend(MinOp)
def to_sql(self, schema, not_null=False, boolean=False):
    terms = [t.partial_eval().to_sql(schema)[0].sql.n for t in self.terms]
    return wrap([{"name": ".", "sql": {"n": "min" + sql_iso((sql_list(terms)))}}])


@extend(MaxOp)
def to_sql(self, schema, not_null=False, boolean=False):
    terms = [t.partial_eval().to_sql(schema)[0].sql.n for t in self.terms]
    return wrap([{"name": ".", "sql": {"n": "max" + sql_iso((sql_list(terms)))}}])


@extend(BaseInequalityOp)
def to_sql(self, schema, not_null=False, boolean=False):
    op, zero = _sql_operators[self.op]
    lhs = self.lhs.to_sql(schema, not_null=True)[0].sql
    rhs = self.rhs.to_sql(schema, not_null=True)[0].sql
    lhs_exists = self.lhs.exists().to_sql(schema)[0].sql
    rhs_exists = self.rhs.exists().to_sql(schema)[0].sql

    if len(lhs) == 1 and len(rhs) == 1:
        return wrap([{"name": ".", "sql": {
            "b": sql_iso(lhs.values()[0]) + " " + op + " " + sql_iso(rhs.values()[0])
        }}])

    ors = []
    for l in "bns":
        ll = lhs[l]
        if not ll:
            continue
        for r in "bns":
            rr = rhs[r]
            if not rr:
                continue
            elif r == l:
                ors.append(
                    sql_iso(lhs_exists[l]) + SQL_AND + sql_iso(rhs_exists[r]) + SQL_AND + sql_iso(lhs[l]) + " " + op + " " + sql_iso(rhs[r])
                )
            elif (l > r and self.op in ["gte", "gt"]) or (l < r and self.op in ["lte", "lt"]):
                ors.append(
                    sql_iso(lhs_exists[l]) + SQL_AND + sql_iso(rhs_exists[r])
                )
    sql = sql_iso(SQL_OR.join(sql_iso(o) for o in ors))

    return wrap([{"name": ".", "sql": {"b": sql}}])


@extend(DivOp)
def to_sql(self, schema, not_null=False, boolean=False):
    lhs = self.lhs.to_sql(schema)[0].sql.n
    rhs = self.rhs.to_sql(schema)[0].sql.n
    d = self.default.to_sql(schema)[0].sql.n

    if lhs and rhs:
        if d == None:
            return wrap([{
                "name": ".",
                "sql": {"n": sql_iso(lhs) + " / " + sql_iso(rhs)}
            }])
        else:
            return wrap([{
                "name": ".",
                "sql": {"n": sql_coalesce([sql_iso(lhs) + " / " + sql_iso(rhs), d])}
            }])
    else:
        return Null


@extend(FloorOp)
def to_sql(self, schema, not_null=False, boolean=False):
    lhs = self.lhs.to_sql(schema)
    rhs = self.rhs.to_sql(schema)
    acc = []
    if len(lhs) != len(rhs):
        Log.error("lhs and rhs have different dimensionality!?")
    for l, r in zip(lhs, rhs):
        for t in "bsnj":
            if l.sql[t] == None:
                if r.sql[t] == None:
                    pass
                else:
                    acc.append(sql_iso(r.sql[t]) + " IS " + SQL_NULL)
            else:
                if r.sql[t] == None:
                    acc.append(sql_iso(l.sql[t]) + " IS " + SQL_NULL)
                else:
                    acc.append("(" + sql_iso(l.sql[t]) + " = " + sql_iso(r.sql[t]) + " OR (" + sql_iso(l.sql[t]) + " IS" + SQL_NULL + SQL_AND + "(" + r.sql[
                        t] + ") IS NULL))")
    if not acc:
        return FALSE.to_sql(schema)
    else:
        return wrap([{"name": ".", "sql": {"b": SQL_OR.join(acc)}}])


# @extend(NeOp)
# def to_sql(self, schema, not_null=False, boolean=False):
#     return NotOp(EqOp([self.lhs, self.rhs])).to_sql(schema, not_null, boolean)


@extend(NotOp)
def to_sql(self, schema, not_null=False, boolean=False):
    not_expr = NotOp(BooleanOp(self.term)).partial_eval()
    if is_op(not_expr, Variable):
        return wrap([{"name": ".", "sql": {"b": "NOT " + sql_iso(not_expr.term.to_sql(schema)[0].sql.b)}}])
    else:
        return not_expr.to_sql(schema)


@extend(BooleanOp)
def to_sql(self, schema, not_null=False, boolean=False):
    term = self.term.partial_eval()
    if term.type == "boolean":
        sql = term.to_sql(schema)
        return sql
    else:
        sql = term.exists().partial_eval().to_sql(schema)
        return sql


@extend(AndOp)
def to_sql(self, schema, not_null=False, boolean=False):
    if not self.terms:
        return wrap([{"name": ".", "sql": {"b": SQL_TRUE}}])
    elif all(self.terms):
        return wrap([{"name": ".", "sql": {
            "b": SQL_AND.join([sql_iso(t.to_sql(schema, boolean=True)[0].sql.b) for t in self.terms])
        }}])
    else:
        return wrap([{"name": ".", "sql": {"b": SQL_FALSE}}])


@extend(OrOp)
def to_sql(self, schema, not_null=False, boolean=False):
    return wrap([{
        "name": ".",
        "sql": {"b": SQL_OR.join(
            sql_iso(t.to_sql(schema, boolean=True)[0].sql.b)
            for t in self.terms
        )}
    }])


@extend(LengthOp)
def to_sql(self, schema, not_null=False, boolean=False):
    term = self.term.partial_eval()
    if is_literal(term):
        val = term.value
        if is_text(val):
            return wrap([{"name": ".", "sql": {"n": convert.value2json(len(val))}}])
        elif isinstance(val, (float, int)):
            return wrap([{"name": ".", "sql": {"n": convert.value2json(len(convert.value2json(val)))}}])
        else:
            return Null
    value = term.to_sql(schema)[0].sql.s
    return wrap([{"name": ".", "sql": {"n": "LENGTH" + sql_iso(value)}}])


@extend(IntegerOp)
def to_sql(self, schema, not_null=False, boolean=False):
    value = self.term.to_sql(schema, not_null=True)
    acc = []
    for c in value:
        for t, v in c.sql.items():
            if t == "s":
                acc.append("CAST(" + v + " as INTEGER)")
            else:
                acc.append(v)

    if not acc:
        return wrap([])
    elif len(acc) == 1:
        return wrap([{"name": ".", "sql": {"n": acc[0]}}])
    else:
        return wrap([{"name": ".", "sql": {"n": sql_coalesce(acc)}}])


@extend(NumberOp)
def to_sql(self, schema, not_null=False, boolean=False):
    value = self.term.to_sql(schema, not_null=True)
    acc = []
    for c in value:
        for t, v in c.sql.items():
            if t == "s":
                acc.append("CAST(" + v + " as FLOAT)")
            else:
                acc.append(v)

    if not acc:
        return wrap([])
    elif len(acc) == 1:
        return wrap([{"name": ".", "sql": {"n": acc}}])
    else:
        return wrap([{"name": ".", "sql": {"n": sql_coalesce(acc)}}])


@extend(StringOp)
def to_sql(self, schema, not_null=False, boolean=False):
    test = self.term.missing().to_sql(schema, boolean=True)[0].sql.b
    value = self.term.to_sql(schema, not_null=True)[0].sql
    acc = []
    for t, v in value.items():
        if t == "b":
            acc.append(SQL_CASE+SQL_WHEN + sql_iso(test) + SQL_THEN + SQL_NULL + SQL_WHEN + sql_iso(v) + SQL_THEN+"'true'"+SQL_ELSE+"'false'"+SQL_END)
        elif t == "s":
            acc.append(v)
        else:
            acc.append("RTRIM(RTRIM(CAST" + sql_iso(v + " as TEXT), " + quote_value('0')) + ", " + quote_value(".") + ")")
    if not acc:
        return wrap([{}])
    elif len(acc) == 1:
        return wrap([{"name": ".", "sql": {"s": acc[0]}}])
    else:
        return wrap([{"name": ".", "sql": {"s": sql_coalesce(acc)}}])


@extend(CountOp)
def to_sql(self, schema, not_null=False, boolean=False):
    acc = []
    for term in self.terms:
        sqls = term.to_sql(schema)
        if len(sqls) > 1:
            acc.append(SQL_TRUE)
        else:
            for t, v in sqls[0].sql.items():
                if t in ["b", "s", "n"]:
                    acc.append(SQL_CASE+SQL_WHEN + sql_iso(v) + SQL_IS_NULL + SQL_THEN+"0"+SQL_ELSE+"1"+SQL_END)
                else:
                    acc.append(SQL_TRUE)

    if not acc:
        return wrap([{}])
    else:
        return wrap([{"nanme": ".", "sql": {"n": SQL("+").join(acc)}}])


_sql_operators = {
    "add": (SQL(" + "), SQL_ZERO),  # (operator, zero-array default value) PAIR
    "basic.add": (SQL(" + "), SQL_ZERO),  # (operator, zero-array default value) PAIR
    "sum": (SQL(" + "), SQL_ZERO),
    "mul": (SQL(" * "), SQL_ONE),
    "multiply": (SQL(" * "), SQL_ONE),
    "basic.mul": (SQL(" * "), SQL_ONE)
}


@extend(BasicMultiOp)
def to_sql(self, schema, not_null=False, boolean=False):
    op, identity = _sql_operators[self.op]
    sql = op.join(sql_iso(t.to_sql(schema)[0].sql.n) for t in self.terms)
    return wrap([{"name": ".", "sql": {"n": sql}}])


@extend(RegExpOp)
def to_sql(self, schema, not_null=False, boolean=False):
    pattern = quote_value(json2value(self.pattern.json))
    value = self.var.to_sql(schema)[0].sql.s
    return wrap([
        {"name": ".", "sql": {"b": value + " REGEXP " + pattern}}
    ])


@extend(CoalesceOp)
def to_sql(self, schema, not_null=False, boolean=False):
    acc = {
        "b": [],
        "s": [],
        "n": []
    }

    for term in self.terms:
        for t, v in term.to_sql(schema)[0].sql.items():
            acc[t].append(v)

    output = {}
    for t, terms in acc.items():
        if not terms:
            continue
        elif len(terms) == 1:
            output[t] = terms[0]
        else:
            output[t] = sql_coalesce(terms)
    return wrap([{"name": ".", "sql": output}])


@extend(MissingOp)
def to_sql(self, schema, not_null=False, boolean=False):
    value = self.expr.partial_eval()
    missing_value = value.missing().partial_eval()

    if not is_op(missing_value, Variable):
        return missing_value.to_sql(schema)

    value_sql = value.to_sql(schema)

    if len(value_sql) > 1:
        return wrap([{"name": ".", "sql": {"b": SQL_FALSE}}])

    acc = []
    for c in value_sql:
        for t, v in c.sql.items():
            if t == "b":
                acc.append(sql_iso(v) + SQL_IS_NULL)
            if t == "s":
                acc.append(sql_iso(sql_iso(v) + SQL_IS_NULL) + SQL_OR + sql_iso(sql_iso(v) + "=" + SQL_EMPTY_STRING))
            if t == "n":
                acc.append(sql_iso(v) + SQL_IS_NULL)

    if not acc:
        return wrap([{"name": ".", "sql": {"b": SQL_TRUE}}])
    else:
        return wrap([{"name": ".", "sql": {"b": SQL_AND.join(acc)}}])




@extend(WhenOp)
def to_sql(self, schema, not_null=False, boolean=False):
    when = self.when.partial_eval().to_sql(schema, boolean=True)[0].sql
    then = self.then.partial_eval().to_sql(schema, not_null=not_null)[0].sql
    els_ = self.els_.partial_eval().to_sql(schema, not_null=not_null)[0].sql
    output = {}
    for t in "bsn":
        if then[t] == None:
            if els_[t] == None:
                pass
            else:
                output[t] = SQL_CASE+SQL_WHEN + when.b + SQL_THEN + SQL_NULL + SQL_ELSE + els_[t] + SQL_END
        else:
            if els_[t] == None:
                output[t] = SQL_CASE+SQL_WHEN + when.b + SQL_THEN + then[t] + SQL_END
            else:
                output[t] = SQL_CASE+SQL_WHEN + when.b + SQL_THEN + then[t] + SQL_ELSE + els_[t] + SQL_END
    if not output:
        return wrap([{"name": ".", "sql": {"0": SQL_NULL}}])
    else:
        return wrap([{"name": ".", "sql": output}])


@extend(ExistsOp)
def to_sql(self, schema, not_null=False, boolean=False):
    field = self.field.to_sql(schema)[0].sql
    acc = []
    for t, v in field.items():
        if t in "bns":
            acc.append(sql_iso(v + SQL_IS_NOT_NULL))

    if not acc:
        return wrap([{"name": ".", "sql": {"b": SQL_FALSE}}])
    else:
        return wrap([{"name": ".", "sql": {"b": SQL_OR.join(acc)}}])


@extend(PrefixOp)
def to_sql(self, schema, not_null=False, boolean=False):
    if not self.expr:
        return wrap([{"name": ".", "sql": {"b": SQL_TRUE}}])
    else:
        return wrap([{"name": ".", "sql": {
            "b": "INSTR" + sql_iso(self.expr.to_sql(schema)[0].sql.s + ", " + self.prefix.to_sql(schema)[0].sql.s) + "==1"
        }}])


@extend(SuffixOp)
def to_sql(self, schema, not_null=False, boolean=False):
    if not self.expr:
        return wrap([{"name": ".", "sql": {"b": SQL_FALSE}}])
    elif is_literal(self.suffix) and not self.suffix.value:
        return wrap([{"name": ".", "sql": {"b": SQL_TRUE}}])
    else:
        return EqOp(
            [
                RightOp([self.expr, LengthOp(self.suffix)]),
                self.suffix
            ]
        ).partial_eval().to_sql(schema)


@extend(ConcatOp)
def to_sql(self, schema, not_null=False, boolean=False):
    defult = self.default.to_sql(schema)
    if len(self.terms) == 0:
        return defult
    defult = coalesce(defult[0].sql, SQL_NULL)
    sep = self.separator.to_sql(schema)[0].sql.s

    acc = []
    for t in self.terms:
        missing = t.missing().partial_eval()

        term = t.to_sql(schema, not_null=True)[0].sql
        if term.s:
            term_sql = term.s
        elif term.n:
            term_sql = "cast(" + term.n + " as text)"
        else:
            term_sql = SQL_CASE + SQL_WHEN + term.b + SQL_THEN + quote_value("true") + SQL_ELSE + quote_value("false") + SQL_END

        if is_op(missing, Variable):
            acc.append(SQL_EMPTY_STRING)
        elif missing:
            acc.append(
                SQL_CASE +
                SQL_WHEN + sql_iso(missing.to_sql(schema, boolean=True)[0].sql.b) +
                SQL_THEN + SQL_EMPTY_STRING +
                SQL_ELSE + sql_iso(sql_concat([sep, term_sql])) +
                SQL_END
            )
        else:
            acc.append(sql_concat([sep, term_sql]))

    expr_ = "substr(" + sql_concat(acc) + ", " + LengthOp(self.separator).to_sql(schema)[0].sql.n + "+1)"

    missing = self.missing()
    if not missing:
        return wrap([{"name": ".", "sql": {"s": expr_}}])
    else:
        return wrap([{
            "name": ".",
            "sql": {
                "s": SQL_CASE+SQL_WHEN+"(" + missing.to_sql(schema, boolean=True)[0].sql.b +
                     ")"+SQL_THEN+"(" + defult +
                     ")"+SQL_ELSE+"(" + expr_ +
                     ")"+SQL_END
            }
        }])


@extend(UnixOp)
def to_sql(self, schema, not_null=False, boolean=False):
    v = self.value.to_sql(schema)[0].sql
    return wrap([{
        "name": ".",
        "sql": {"n": "UNIX_TIMESTAMP" + sql_iso(v.n)}
    }])


@extend(FromUnixOp)
def to_sql(self, schema, not_null=False, boolean=False):
    v = self.value.to_sql(schema)[0].sql
    return wrap([{
        "name": ".",
        "sql": {"n": "FROM_UNIXTIME" + sql_iso(v.n)}
    }])


@extend(LeftOp)
def to_sql(self, schema, not_null=False, boolean=False):
    return SqlSubstrOp(
        [
            self.value,
            ONE,
            self.length
        ]
    ).partial_eval().to_sql(schema)


@extend(NotLeftOp)
def to_sql(self, schema, not_null=False, boolean=False):
    # test_v = self.value.missing().to_sql(boolean=True)[0].sql.b
    # test_l = self.length.missing().to_sql(boolean=True)[0].sql.b
    v = self.value.to_sql(schema, not_null=True)[0].sql.s
    l = "max(0, " + self.length.to_sql(schema, not_null=True)[0].sql.n + ")"

    expr = "substr(" + v + ", " + l + "+1)"
    return wrap([{"name": ".", "sql": {"s": expr}}])


@extend(RightOp)
def to_sql(self, schema, not_null=False, boolean=False):
    v = self.value.to_sql(schema, not_null=True)[0].sql.s
    r = self.length.to_sql(schema, not_null=True)[0].sql.n
    l = "max(0, length" + sql_iso(v) + "-max(0, " + r + "))"
    expr = "substr(" + v + ", " + l + "+1)"
    return wrap([{"name": ".", "sql": {"s": expr}}])


@extend(RightOp)
@simplified
def partial_eval(self):
    value = self.value.partial_eval()
    length = self.length.partial_eval()
    max_length = LengthOp(value)

    return BasicSubstringOp([
        value,
        MaxOp([ZERO, MinOp([max_length, BaseBinaryOp([max_length, length])])]),
        max_length
    ])


@extend(NotRightOp)
def to_sql(self, schema, not_null=False, boolean=False):
    v = self.value.to_sql(schema, not_null=True)[0].sql.s
    r = self.length.to_sql(schema, not_null=True)[0].sql.n
    l = "max(0, length" + sql_iso(v) + "-max(0, " + r + "))"
    expr = "substr" + sql_iso(v + ", 1, " + l)
    return wrap([{"name": ".", "sql": {"s": expr}}])


@extend(FindOp)
def to_sql(self, schema, not_null=False, boolean=False):
    test = SqlInstrOp([
        SqlSubstrOp([
            self.value,
            AddOp([self.start, ONE]),
            NULL
        ]),
        self.find
    ]).partial_eval()

    if boolean:
        return test.to_sql(schema)
    else:
        offset = SubOp([self.start, ONE]).partial_eval()
        index = AddOp([test, offset]).partial_eval()
        temp = index.to_sql(schema)
        return WhenOp(
            EqOp([test, ZERO]),
            **{
                "then": self.default,
                "else": index
            }
        ).partial_eval().to_sql(schema)


@extend(FindOp)
@simplified
def partial_eval(self):
    return FindOp(
        [
            self.value.partial_eval(),
            self.find.partial_eval()
        ],
        **{
            "start": self.start.partial_eval(),
            "default": self.default.partial_eval()
        }
    )


@extend(BetweenOp)
def to_sql(self, schema, not_null=False, boolean=False):
    return self.partial_eval().to_sql(schema)


@extend(InOp)
def to_sql(self, schema, not_null=False, boolean=False):
    if not is_literal(self.superset):
        Log.error("Not supported")
    j_value = json2value(self.superset.json)
    if j_value:
        var = self.value.to_sql(schema)
        return SQL_OR.join(sql_iso(var + "==" + quote_value(v)) for v in j_value)
    else:
        return wrap([{"name": ".", "sql": {"b": SQL_FALSE}}])


@extend(RangeOp)
def to_sql(self, schema, not_null=False, boolean=False):
    when = self.when.to_sql(schema, boolean=True)[0].sql
    then = self.then.to_sql(schema, not_null=not_null)[0].sql
    els_ = self.els_.to_sql(schema, not_null=not_null)[0].sql
    output = {}
    for t in "bsn":
        if then[t] == None:
            if els_[t] == None:
                pass
            else:
                output[t] = SQL_CASE+SQL_WHEN + when.b + SQL_THEN + SQL_NULL + SQL_ELSE + els_[t] + SQL_END
        else:
            if els_[t] == None:
                output[t] = SQL_CASE+SQL_WHEN + when.b + SQL_THEN + then[t] + SQL_END
            else:
                output[t] = SQL_CASE+SQL_WHEN + when.b + SQL_THEN + then[t] + SQL_ELSE + els_[t] + SQL_END
    if not output:
        return wrap([{"name": ".", "sql": {"0": SQL_NULL}}])
    else:
        return wrap([{"name": ".", "sql": output}])


@extend(CaseOp)
def to_sql(self, schema, not_null=False, boolean=False):
    if len(self.whens) == 1:
        return self.whens[-1].to_sql(schema)

    output = {}
    for t in "bsn":  # EXPENSIVE LOOP to_sql() RUN 3 TIMES
        els_ = coalesce(self.whens[-1].to_sql(schema)[0].sql[t], SQL_NULL)
        acc = SQL_ELSE + els_ + SQL_END
        for w in reversed(self.whens[0:-1]):
            acc = SQL_WHEN + w.when.to_sql(schema, boolean=True)[0].sql.b + SQL_THEN + coalesce(w.then.to_sql(schema)[0].sql[t], SQL_NULL) + acc
        output[t] = SQL_CASE + acc
    return wrap([{"name": ".", "sql": output}])


@extend(SqlEqOp)
def to_sql(self, schema, not_null=False, boolean=False):
    lhs = self.lhs.partial_eval().to_sql(schema)[0].sql.values()[0]
    rhs = self.rhs.partial_eval().to_sql(schema)[0].sql.values()[0]

    return wrap([{"name": ".", "sql": {
        "b": sql_iso(lhs) + "=" + sql_iso(rhs)
    }}])


@extend(SqlInstrOp)
def to_sql(self, schema, not_null=False, boolean=False):
    value = self.value.to_sql(schema)[0].sql.s
    find = self.find.to_sql(schema)[0].sql.s

    return wrap([{"name": ".", "sql": {
        "n": "INSTR" + sql_iso(sql_list([value, find]))
    }}])


@extend(SqlInstrOp)
@simplified
def partial_eval(self):
    value = self.value.partial_eval()
    find = self.find.partial_eval()
    return SqlInstrOp([value, find])


@extend(SqlSubstrOp)
def to_sql(self, schema, not_null=False, boolean=False):
    value = self.value.to_sql(schema)[0].sql.s
    start = self.start.to_sql(schema)[0].sql.n
    if self.length is NULL:
        return wrap([{"name": ".", "sql": {
            "s": "SUBSTR" + sql_iso(sql_list([value, start]))
        }}])
    else:
        length = self.length.to_sql(schema)[0].sql.n
        return wrap([{"name": ".", "sql": {
            "s": "SUBSTR" + sql_iso(sql_list([value, start, length]))
        }}])


@extend(SqlSubstrOp)
@simplified
def partial_eval(self):
    value = self.value.partial_eval()
    start = self.start.partial_eval()
    length = self.length.partial_eval()
    if is_literal(start) and start.value == 1:
        if length is NULL:
            return value
    return SqlSubstrOp([value, start, length])


json_type_to_sql_type = {
    "null": "0",
    "boolean": "b",
    "number": "n",
    "string": "s",
    "object": "j",
    "nested": "j"
}

sql_type_to_json_type = {
    "0": "null",
    "b": "boolean",
    "n": "number",
    "s": "string",
    "j": "object"
}
