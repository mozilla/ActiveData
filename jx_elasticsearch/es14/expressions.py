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

import itertools

from future.utils import text_type
from mo_dots import coalesce, wrap, Null, unwraplist, literal_field, set_default
from mo_json import json2value, quote
from mo_logs import Log, suppress_exception
from mo_math import OR, MAX
from pyLibrary import convert

from jx_base.expressions import Variable, DateOp, TupleOp, LeavesOp, BinaryOp, OrOp, ScriptOp, \
    WhenOp, InequalityOp, extend, RowsOp, Literal, NullOp, TrueOp, FalseOp, DivOp, FloorOp, \
    EqOp, NeOp, NotOp, LengthOp, NumberOp, StringOp, CountOp, MultiOp, RegExpOp, CoalesceOp, MissingOp, ExistsOp, \
    PrefixOp, UnixOp, NotLeftOp, RightOp, NotRightOp, FindOp, BetweenOp, InOp, RangeOp, CaseOp, AndOp, \
    ConcatOp, TRUE_FILTER, FALSE_FILTER, LeftOp


@extend(BetweenOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    if isinstance(self.prefix, Literal) and isinstance(json2value(self.prefix.json), int):
        value_is_missing = self.value.missing().to_ruby()
        value = self.value.to_ruby(not_null=True)
        start = "max(" + self.prefix.json + ", 0)"

        if isinstance(self.suffix, Literal) and isinstance(json2value(self.suffix.json), int):
            check = "(" + value_is_missing + ")"
            end = "min(" + self.suffix.to_ruby() + ", " + value + ".length())"
        else:
            end = value + ".indexOf(" + self.suffix.to_ruby() + ", " + start + ")"
            check = "((" + value_is_missing + ") || (" + end + "==-1))"

        expr = check + " ? " + self.default.to_ruby() + " : ((" + value + ").substring(" + start + ", " + end + "))"
        return expr

    else:
        # ((Runnable)(() -> {int a=2; int b=3; System.out.println(a+b);})).run();
        value_is_missing = self.value.missing().to_ruby()
        value = self.value.to_ruby(not_null=True)
        prefix = self.prefix.to_ruby()
        len_prefix = text_type(len(json2value(self.prefix.json))) if isinstance(self.prefix,
                                                                              Literal) else "(" + prefix + ").length()"
        suffix = self.suffix.to_ruby()
        start_index = self.start.to_ruby()
        if start_index == "null":
            if prefix == "null":
                start = "0"
            else:
                start = value + ".indexOf(" + prefix + ")"
        else:
            start = value + ".indexOf(" + prefix + ", " + start_index + ")"

        if suffix == "null":
            expr = "((" + value_is_missing + ") || (" + start + "==-1)) ? " + self.default.to_ruby() + " : ((" + value + ").substring(" + start + "+" + len_prefix + "))"
        else:
            end = value + ".indexOf(" + suffix + ", " + start + "+" + len_prefix + ")"
            expr = "((" + value_is_missing + ") || (" + start + "==-1) || (" + end + "==-1)) ? " + self.default.to_ruby() + " : ((" + value + ").substring(" + start + "+" + len_prefix + ", " + end + "))"

        return expr


@extend(BetweenOp)
def to_esfilter(self):
    if isinstance(self.value, Variable):
        return {"terms": {self.value.var: json2value(self.superset.json)}}
    else:
        return {"script": self.to_ruby()}


@extend(BinaryOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    lhs = self.lhs.to_ruby(not_null=True)
    rhs = self.rhs.to_ruby(not_null=True)
    script = "(" + lhs + ") " + BinaryOp.operators[self.op] + " (" + rhs + ")"
    missing = OrOp("or", [self.lhs.missing(), self.rhs.missing()])

    if self.op in BinaryOp.operators:
        script = "(" + script + ").doubleValue()"  # RETURN A NUMBER, NOT A STRING
        default = self.default
    if many:
        script = "[" + script + "]"
        default = ScriptOp("script", self.default.to_ruby(many=many))

    output = WhenOp(
        "when",
        missing,
        **{
            "then": default,
            "else":
                ScriptOp("script", script)
        }
    ).to_ruby()
    return output


@extend(BinaryOp)
def to_esfilter(self):
    if not isinstance(self.lhs, Variable) or not isinstance(self.rhs, Literal) or self.op in BinaryOp.algebra_ops:
        return {"script": {"script": self.to_ruby()}}

    if self.op in ["eq", "term"]:
        return {"term": {self.lhs.var: self.rhs.to_esfilter()}}
    elif self.op in ["ne", "neq"]:
        return {"not": {"term": {self.lhs.var: self.rhs.to_esfilter()}}}
    elif self.op in BinaryOp.ineq_ops:
        return {"range": {self.lhs.var: {self.op: json2value(self.rhs.json)}}}
    else:
        Log.error("Logic error")


@extend(CaseOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    acc = self.whens[-1].to_ruby()
    for w in reversed(self.whens[0:-1]):
        acc = "(" + w.when.to_ruby(boolean=True) + ") ? (" + w.then.to_ruby() + ") : (" + acc + ")"
    return acc


@extend(CaseOp)
def to_esfilter(self):
    return {"script": {"script": self.to_ruby()}}


@extend(ConcatOp)
def to_esfilter(self):
    if isinstance(self.value, Variable) and isinstance(self.find, Literal):
        return {"regexp": {self.value.var: ".*" + convert.string2regexp(json2value(self.find.json)) + ".*"}}
    else:
        return {"script": {"script": self.to_ruby()}}


@extend(ConcatOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    if len(self.terms) == 0:
        return self.default.to_ruby()

    acc = []
    for t in self.terms:
        acc.append("((" + t.missing().to_ruby(boolean=True) + ") ? \"\" : (" + self.separator.json + "+" + t.to_ruby(
            not_null=True) + "))")
    expr_ = "(" + "+".join(acc) + ").substring(" + text_type(len(json2value(self.separator.json))) + ")"

    return "(" + self.missing().to_ruby() + ") ? (" + self.default.to_ruby() + ") : (" + expr_ + ")"


@extend(Literal)
def to_ruby(self, not_null=False, boolean=False, many=False):
    def _convert(v):
        if v is None:
            return "null"
        if v is True:
            return "true"
        if v is False:
            return "false"
        if isinstance(v, basestring):
            return quote(v)
        if isinstance(v, (int, long, float)):
            return text_type(v)
        if isinstance(v, dict):
            return "[" + ", ".join(quote(k) + ": " + _convert(vv) for k, vv in v.items()) + "]"
        if isinstance(v, list):
            return "[" + ", ".join(_convert(vv) for vv in v) + "]"

    value = convert.json_decoder(self.json)
    if many:
        if value is None:
            return "[]"
        return "[" + _convert(value) + "]"
    else:
        return _convert(value)


@extend(CoalesceOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    if not self.terms:
        return "null"
    acc = self.terms[-1].to_ruby()
    for v in reversed(self.terms[:-1]):
        r = v.to_ruby()
        acc = "(((" + r + ") != null) ? (" + r + ") : (" + acc + "))"
    return acc


@extend(CoalesceOp)
def to_esfilter(self):
    return {"or": [{"exists": {"field": v}} for v in self.terms]}


@extend(ExistsOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    if isinstance(self.field, Variable):
        return "!doc[" + quote(self.field.var) + "].isEmpty()"
    elif isinstance(self.field, Literal):
        return self.field.exists().to_ruby()
    else:
        return self.field.to_ruby() + " != null"


@extend(ExistsOp)
def to_esfilter(self):
    if isinstance(self.field, Variable):
        return {"exists": {"field": self.field.var}}
    else:
        return {"script": {"script": self.to_ruby()}}


@extend(FindOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    missing = self.missing()
    v = self.value.to_ruby(not_null=True)
    find = self.find.to_ruby(not_null=True)
    start = self.start.to_ruby(not_null=True)
    index = v + ".indexOf(" + find + ", " + start + ")"

    if not_null:
        no_index = index + "==-1"
    else:
        no_index = missing.to_ruby(boolean=True)

    expr = "(" + no_index + ") ? " + self.default.to_ruby() + " : " + index
    return expr


@extend(FindOp)
def to_esfilter(self):
    if isinstance(self.value, Variable) and isinstance(self.find, Literal):
        return {"regexp": {self.value.var: ".*" + convert.string2regexp(json2value(self.find.json)) + ".*"}}
    else:
        return {"script": {"script": self.to_ruby()}}


@extend(Literal)
def to_esfilter(self):
    return json2value(self.json)


@extend(NullOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    if many:
        return "[]"
    return "null"


@extend(NullOp)
def to_esfilter(self):
    return {"not": {"match_all": {}}}


@extend(FalseOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    return "false"


@extend(FalseOp)
def to_esfilter(self):
    return {"not": {"match_all": {}}}


@extend(DateOp)
def to_esfilter(self):
    return json2value(self.json)


@extend(DateOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    Log.error("not supported")


@extend(TupleOp)
def to_esfilter(self):
    Log.error("not supported")


@extend(LeavesOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    Log.error("not supported")


@extend(LeavesOp)
def to_esfilter(self):
    Log.error("not supported")


@extend(InequalityOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    lhs = self.lhs.to_ruby(not_null=True)
    rhs = self.rhs.to_ruby(not_null=True)
    script = "(" + lhs + ") " + InequalityOp.operators[self.op] + " (" + rhs + ")"
    missing = OrOp("or", [self.lhs.missing(), self.rhs.missing()])

    output = WhenOp(
        "when",
        missing,
        **{
            "then": self.default,
            "else":
                ScriptOp("script", script)
        }
    ).to_ruby()
    return output


@extend(InequalityOp)
def to_esfilter(self):
    if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
        return {"range": {self.lhs.var: {self.op: json2value(self.rhs.json)}}}
    else:
        return {"script": {"script": self.to_ruby()}}


@extend(DivOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    lhs = self.lhs.to_ruby(not_null=True)
    rhs = self.rhs.to_ruby(not_null=True)
    script = "((double)(" + lhs + ") / (double)(" + rhs + ")).doubleValue()"

    output = WhenOp(
        "when",
        OrOp("or", [self.lhs.missing(), self.rhs.missing(), EqOp("eq", [self.rhs, Literal("literal", 0)])]),
        **{
            "then": self.default,
            "else":
                ScriptOp("script", script)
        }
    ).to_ruby()
    return output


@extend(DivOp)
def to_esfilter(self):
    if not isinstance(self.lhs, Variable) or not isinstance(self.rhs, Literal):
        return {"script": {"script": self.to_ruby()}}
    else:
        Log.error("Logic error")


@extend(FloorOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    lhs = self.lhs.to_ruby(not_null=True)
    rhs = self.rhs.to_ruby(not_null=True)
    script = "Math.floor(((double)(" + lhs + ") / (double)(" + rhs + ")).doubleValue())*(" + rhs + ")"

    output = WhenOp(
        "when",
        OrOp("or", [self.lhs.missing(), self.rhs.missing(), EqOp("eq", [self.rhs, Literal("literal", 0)])]),
        **{
            "then": self.default,
            "else":
                ScriptOp("script", script)
        }
    ).to_ruby()
    return output


@extend(FloorOp)
def to_esfilter(self):
    Log.error("Logic error")


@extend(EqOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    lhs_list = self.lhs.to_ruby(many=True)
    rhs_missing = self.rhs.missing().to_ruby()
    rhs = self.rhs.to_ruby(not_null=True)

    if boolean:
        return "(" + rhs_missing + ")?(" + lhs_list + ".size()==0):((" + lhs_list + ").contains(" + rhs + "))"
    else:
        return "((" + rhs_missing + ")|(" + lhs_list + ".size()==0))?null:((" + lhs_list + ").contains(" + rhs + "))"


@extend(EqOp)
def to_esfilter(self):
    if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
        rhs = json2value(self.rhs.json)
        if isinstance(rhs, list):
            if len(rhs) == 1:
                return {"term": {self.lhs.var: rhs[0]}}
            else:
                return {"terms": {self.lhs.var: rhs}}
        else:
            return {"term": {self.lhs.var: rhs}}
    else:
        return {"script": {"script": self.to_ruby(boolean=True)}}


@extend(MissingOp)
def to_ruby(self, not_null=False, boolean=True, many=False):
    if isinstance(self.expr, Variable):
        if self.expr.var == "_id":
            return "false"
        else:
            return "doc[" + quote(self.expr.var) + "].isEmpty()"
    elif isinstance(self.expr, Literal):
        return self.expr.missing().to_ruby()
    else:
        return self.expr.missing().to_ruby()


@extend(MissingOp)
def to_esfilter(self):
    if isinstance(self.expr, Variable):
        return {"missing": {"field": self.expr.var}}
    else:
        return {"script": {"script": self.to_ruby()}}


@extend(NotLeftOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    test_v = self.value.missing().to_ruby(boolean=True)
    test_l = self.length.missing().to_ruby(boolean=True)
    v = self.value.to_ruby(not_null=True)
    l = self.length.to_ruby(not_null=True)

    expr = "((" + test_v + ") || (" + test_l + ")) ? null : (" + v + ".substring(max(0, min(" + v + ".length(), " + l + ")).intValue()))"
    return expr


@extend(NeOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    lhs = self.lhs.to_ruby()
    rhs = self.rhs.to_ruby()
    return "((" + lhs + ")!=null) && ((" + rhs + ")!=null) && ((" + lhs + ")!=(" + rhs + "))"


@extend(NeOp)
def to_esfilter(self):
    if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
        return {"not": {"term": {self.lhs.var: self.rhs.to_esfilter()}}}
    else:
        return {"and": [
            {"and": [{"exists": {"field": v}} for v in self.vars()]},
            {"script": {"script": self.to_ruby()}}
        ]}


@extend(NotOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    return "!(" + self.term.to_ruby(boolean=True) + ")"


@extend(NotOp)
def to_esfilter(self):
    operand = self.term.to_esfilter()
    if operand.get("script"):
        return {"script": {"script": "!(" + operand.get("script", {}).get("script") + ")"}}
    else:
        return {"not": operand}


@extend(AndOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    if not self.terms:
        return "true"
    else:
        return " && ".join("(" + t.to_ruby() + ")" for t in self.terms)


@extend(AndOp)
def to_esfilter(self):
    if not len(self.terms):
        return {"match_all": {}}
    else:
        return {"bool": {"must": [t.to_esfilter() for t in self.terms]}}


@extend(OrOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    return " || ".join("(" + t.to_ruby(boolean=True) + ")" for t in self.terms if t)


@extend(OrOp)
def to_esfilter(self):
    return {"or": [t.to_esfilter() for t in self.terms]}


@extend(LengthOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    value = self.term.to_ruby(not_null=True)
    if not_null:
        return "(" + value + ").length()"

    missing = self.missing().to_ruby()
    if many:
        return "(" + missing + " ) ? [] : [(" + value + ").length()]"
    else:
        return "(" + missing + " ) ? null : (" + value + ").length()"


@extend(NumberOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    test = self.term.missing().to_ruby(boolean=True)
    value = self.term.to_ruby(not_null=True)
    return "(" + test + ") ? null : (((" + value + ") instanceof String) ? Double.parseDouble(" + value + ") : (" + value + "))"


@extend(CountOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    return "+".join("((" + t.missing().to_ruby(boolean=True) + ") ? 0 : 1)" for t in self.terms)


@extend(LengthOp)
def to_esfilter(self):
    return {"regexp": {self.var.var: json2value(self.pattern.json)}}


@extend(MultiOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    if self.nulls:
        op, unit = MultiOp.operators[self.op]
        null_test = CoalesceOp("coalesce", self.terms).missing().to_ruby(boolean=True)
        acc = op.join(
            "((" + t.missing().to_ruby(boolean=True) + ") ? " + unit + " : (" + t.to_ruby(not_null=True) + "))" for
            t in self.terms
        )
        if many:
            acc = "[" + acc + "]"
        return "((" + null_test + ") ? (" + self.default.to_ruby(many=many) + ") : (" + acc + "))"
    else:
        op, unit = MultiOp.operators[self.op]
        null_test = OrOp("or", [t.missing() for t in self.terms]).to_ruby()
        acc = op.join("(" + t.to_ruby(not_null=True) + ")" for t in self.terms)
        if many:
            acc = "[" + acc + "]"
        return "((" + null_test + ") ? (" + self.default.to_ruby(many=many) + ") : (" + acc + "))"


@extend(RegExpOp)
def to_esfilter(self):
    return {"regexp": {self.var.var: json2value(self.pattern.json)}}


@extend(StringOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    value = self.term.to_ruby(not_null=True)
    missing = self.term.missing().to_ruby()
    return "(" + missing + ") ? null : (((" + value + ") instanceof java.lang.Double) ? String.valueOf(" + value + ").replaceAll('\\\\.0$', '') : String.valueOf(" + value + "))"  # "\\.0$"


@extend(TrueOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    return "true"


@extend(TrueOp)
def to_esfilter(self):
    return {"match_all": {}}


@extend(PrefixOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    return "(" + self.field.to_ruby() + ").startsWith(" + self.prefix.to_ruby() + ")"


@extend(PrefixOp)
def to_esfilter(self):
    if isinstance(self.field, Variable) and isinstance(self.prefix, Literal):
        return {"prefix": {self.field.var: json2value(self.prefix.json)}}
    else:
        return {"script": {"script": self.to_ruby()}}


@extend(RightOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    test_v = self.value.missing().to_ruby(boolean=True)
    test_l = self.length.missing().to_ruby(boolean=True)
    v = self.value.to_ruby(not_null=True)
    l = self.length.to_ruby(not_null=True)

    expr = "((" + test_v + ") || (" + test_l + ")) ? null : (" + v + ".substring(min(" + v + ".length(), max(0, (" + v + ").length() - (" + l + "))).intValue()))"
    return expr


@extend(NotRightOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    test_v = self.value.missing().to_ruby(boolean=True)
    test_l = self.length.missing().to_ruby(boolean=True)
    v = self.value.to_ruby(not_null=True)
    l = self.length.to_ruby(not_null=True)

    expr = "((" + test_v + ") || (" + test_l + ")) ? null : (" + v + ".substring(0, min(" + v + ".length(), max(0, (" + v + ").length() - (" + l + "))).intValue()))"
    return expr


@extend(InOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    return self.superset.to_ruby(many=True) + ".contains(" + self.value.to_ruby() + ")"


@extend(InOp)
def to_esfilter(self):
    if isinstance(self.value, Variable):
        return {"terms": {self.value.var: json2value(self.superset.json)}}
    else:
        return {"script": self.to_ruby()}



@extend(RangeOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    return "(" + self.when.to_ruby(boolean=True) + ") ? (" + self.then.to_ruby(
        not_null=not_null) + ") : (" + self.els_.to_ruby(not_null=not_null) + ")"


@extend(RangeOp)
def to_esfilter(self):
    return {"or": [
        {"and": [
            self.when.to_esfilter(),
            self.then.to_esfilter()
        ]},
        {"and": [
            {"not": self.when.to_esfilter()},
            self.els_.to_esfilter()
        ]}
    ]}
    # return {"script": {"script": self.to_ruby()}}


@extend(LeftOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    test_v = self.value.missing()
    test_l = self.length.missing()
    v = self.value.to_ruby(not_null=True)
    l = self.length.to_ruby(not_null=True)

    if (not test_v or test_v.to_ruby(boolean=True) == "false") and not test_l:
        expr = v + ".substring(0, max(0, min(" + v + ".length(), " + l + ")).intValue())"
    else:
        expr = "((" + test_v.to_ruby(boolean=True) + ") || (" + test_l.to_ruby(boolean=True) + ")) ? null : (" + v + ".substring(0, max(0, min(" + v + ".length(), " + l + ")).intValue()))"
    return expr


@extend(RowsOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    return self.script


@extend(RowsOp)
def to_esfilter(self):
    return {"script": {"script": self.script}}


@extend(ScriptOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    return self.script


@extend(ScriptOp)
def to_esfilter(self):
    return {"script": {"script": self.script}}


@extend(Variable)
def to_ruby(self, not_null=False, boolean=False, many=False):
    if self.var == ".":
        return "_source"
    else:
        if self.var == "_id":
            return 'doc["_uid"].value.substring(doc["_uid"].value.indexOf(\'#\')+1)'
        q = quote(self.var)
        if many:
            return "doc[" + q + "].values"
        if not_null:
            if boolean:
                return "doc[" + q + "].value==\"T\""
            else:
                return "doc[" + q + "].value"
        else:
            if boolean:
                return "doc[" + q + "].isEmpty() ? null : (doc[" + q + "].value==\"T\")"
            else:
                return "doc[" + q + "].isEmpty() ? null : doc[" + q + "].value"


@extend(WhenOp)
def to_ruby(self, not_null=False, boolean=False, many=False):
    return "(" + self.when.to_ruby(boolean=True) + ") ? (" + self.then.to_ruby(not_null=not_null) + ") : (" + self.els_.to_ruby(not_null=not_null) + ")"


@extend(WhenOp)
def to_esfilter(self):
    return {"or": [
        {"and": [
            self.when.to_esfilter(),
            self.then.to_esfilter()
        ]},
        {"and": [
            {"not": self.when.to_esfilter()},
            self.els_.to_esfilter()
        ]}
    ]}


USE_BOOL_MUST = True


def simplify_esfilter(esfilter):
    try:
        output = normalize_esfilter(esfilter)
        if output is TRUE_FILTER:
            return {"match_all": {}}
        elif output is FALSE_FILTER:
            return {"not": {"match_all": {}}}

        output.isNormal = None
        return output
    except Exception as e:
        from mo_logs import Log

        Log.unexpected("programmer error", cause=e)


def removeOr(esfilter):
    if esfilter["not"]:
        return {"not": removeOr(esfilter["not"])}

    if esfilter["and"]:
        return {"and": [removeOr(v) for v in esfilter["and"]]}

    if esfilter["or"]:  # CONVERT OR TO NOT.AND.NOT
        return {"not": {"and": [{"not": removeOr(v)} for v in esfilter["or"]]}}

    return esfilter


def normalize_esfilter(esfilter):
    """
    SIMPLFY THE LOGIC EXPRESSION
    """
    return wrap(_normalize(wrap(esfilter)))


def _normalize(esfilter):
    """
    TODO: DO NOT USE Data, WE ARE SPENDING TOO MUCH TIME WRAPPING/UNWRAPPING
    REALLY, WE JUST COLLAPSE CASCADING `and` AND `or` FILTERS
    """
    if esfilter is TRUE_FILTER or esfilter is FALSE_FILTER or esfilter.isNormal:
        return esfilter

    # Log.note("from: " + convert.value2json(esfilter))
    isDiff = True

    while isDiff:
        isDiff = False

        if coalesce(esfilter["and"], esfilter.bool.must):
            terms = coalesce(esfilter["and"], esfilter.bool.must)
            # MERGE range FILTER WITH SAME FIELD
            for (i0, t0), (i1, t1) in itertools.product(enumerate(terms), enumerate(terms)):
                if i0 >= i1:
                    continue  # SAME, IGNORE
                with suppress_exception:
                    f0, tt0 = t0.range.items()[0]
                    f1, tt1 = t1.range.items()[0]
                    if f0 == f1:
                        set_default(terms[i0].range[literal_field(f1)], tt1)
                        terms[i1] = True

            output = []
            for a in terms:
                if isinstance(a, (list, set)):
                    from mo_logs import Log

                    Log.error("and clause is not allowed a list inside a list")
                a_ = normalize_esfilter(a)
                if a_ is not a:
                    isDiff = True
                a = a_
                if a == TRUE_FILTER:
                    isDiff = True
                    continue
                if a == FALSE_FILTER:
                    return FALSE_FILTER
                if coalesce(a.get("and"), a.bool.must):
                    isDiff = True
                    a.isNormal = None
                    output.extend(coalesce(a.get("and"), a.bool.must))
                else:
                    a.isNormal = None
                    output.append(a)
            if not output:
                return TRUE_FILTER
            elif len(output) == 1:
                # output[0].isNormal = True
                esfilter = output[0]
                break
            elif isDiff:
                if USE_BOOL_MUST:
                    esfilter = wrap({"bool": {"must": output}})
                else:
                    esfilter = wrap({"and": output})
            continue

        if esfilter["or"] != None:
            output = []
            for a in esfilter["or"]:
                a_ = _normalize(a)
                if a_ is not a:
                    isDiff = True
                a = a_

                if a == TRUE_FILTER:
                    return TRUE_FILTER
                if a == FALSE_FILTER:
                    isDiff = True
                    continue
                if a.get("or"):
                    a.isNormal = None
                    isDiff = True
                    output.extend(a["or"])
                else:
                    a.isNormal = None
                    output.append(a)
            if not output:
                return FALSE_FILTER
            elif len(output) == 1:
                esfilter = output[0]
                break
            elif isDiff:
                esfilter = wrap({"or": output})
            continue

        if esfilter.term != None:
            if esfilter.term.keys():
                esfilter.isNormal = True
                return esfilter
            else:
                return TRUE_FILTER

        if esfilter.terms != None:
            for k, v in esfilter.terms.items():
                if len(v) > 0:
                    if OR(vv == None for vv in v):
                        rest = [vv for vv in v if vv != None]
                        if len(rest) > 0:
                            return {
                                "or": [
                                    {"missing": {"field": k}},
                                    {"terms": {k: rest}}
                                ],
                                "isNormal": True
                            }
                        else:
                            return {
                                "missing": {"field": k},
                                "isNormal": True
                            }
                    else:
                        esfilter.isNormal = True
                        return esfilter
            return FALSE_FILTER

        if esfilter["not"] != None:
            _sub = esfilter["not"]
            sub = _normalize(_sub)
            if sub is FALSE_FILTER:
                return TRUE_FILTER
            elif sub is TRUE_FILTER:
                return FALSE_FILTER
            elif sub is not _sub:
                sub.isNormal = None
                return wrap({"not": sub, "isNormal": True})
            else:
                sub.isNormal = None

    esfilter.isNormal = True
    return esfilter


def split_expression_by_depth(where, schema, map_=None, output=None, var_to_depth=None):
    """
    :param where: EXPRESSION TO INSPECT
    :param schema: THE SCHEMA
    :param map_: THE VARIABLE NAME MAPPING TO PERFORM ON where
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
    if not map_:
        map_ = {v: schema[v][0].es_column for v in vars_}

    if var_to_depth is None:
        if not vars_:
            return Null
        # MAP VARIABLE NAMES TO HOW DEEP THEY ARE
        var_to_depth = {v: len(c.nested_path) - 1 for v in vars_ for c in schema[v]}
        all_depths = set(var_to_depth.values())
        if -1 in all_depths:
            Log.error(
                "Can not find column with name {{column|quote}}",
                column=unwraplist([k for k, v in var_to_depth.items() if v == -1])
            )
        if len(all_depths) == 0:
            all_depths = {0}
        output = wrap([[] for _ in range(MAX(all_depths) + 1)])
    else:
        all_depths = set(var_to_depth[v] for v in vars_)

    if len(all_depths) == 1:
        output[list(all_depths)[0]] += [where.map(map_)]
    elif isinstance(where, AndOp):
        for a in where.terms:
            split_expression_by_depth(a, schema, map_, output, var_to_depth)
    else:
        Log.error("Can not handle complex where clause")

    return output
