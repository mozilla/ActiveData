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
def to_painless(self, not_null=False, boolean=False, many=False):
    if isinstance(self.prefix, Literal) and isinstance(json2value(self.prefix.json), int):
        value_is_missing = self.value.missing().to_painless()
        value = self.value.to_painless(not_null=True)
        start = "(int)Math.max(" + self.prefix.json + ", 0)"

        if isinstance(self.suffix, Literal) and isinstance(json2value(self.suffix.json), int):
            check = "(" + value_is_missing + ")"
            end = "(int)Math.min(" + self.suffix.to_painless() + ", " + value + ".length())"
        else:
            end = value + ".indexOf(" + self.suffix.to_painless() + ", " + start + ")"
            check = "((" + value_is_missing + ") || (" + end + "==-1))"

        expr = check + " ? " + self.default.to_painless() + " : ((" + value + ").substring(" + start + ", " + end + "))"
        return expr

    else:
        # ((Runnable)(() -> {int a=2; int b=3; System.out.println(a+b);})).run();
        value_is_missing = self.value.missing().to_painless()
        value = self.value.to_painless(not_null=True)
        prefix = self.prefix.to_painless()
        len_prefix = unicode(len(json2value(self.prefix.json))) if isinstance(self.prefix,
                                                                              Literal) else "(" + prefix + ").length()"
        suffix = self.suffix.to_painless()
        start_index = self.start.to_painless()
        if start_index == "null":
            if prefix == "null":
                start = "0"
            else:
                start = value + ".indexOf(" + prefix + ")"
        else:
            start = value + ".indexOf(" + prefix + ", " + start_index + ")"

        if suffix == "null":
            expr = "((" + value_is_missing + ") || (" + start + "==-1)) ? " + self.default.to_painless() + " : ((" + value + ").substring(" + start + "+" + len_prefix + "))"
        else:
            end = value + ".indexOf(" + suffix + ", " + start + "+" + len_prefix + ")"
            expr = "((" + value_is_missing + ") || (" + start + "==-1) || (" + end + "==-1)) ? " + self.default.to_painless() + " : ((" + value + ").substring(" + start + "+" + len_prefix + ", " + end + "))"

        return expr


@extend(BetweenOp)
def to_esfilter(self):
    if isinstance(self.value, Variable):
        return {"terms": {self.value.var: json2value(self.superset.json)}}
    else:
        return {"script": self.to_painless()}


@extend(BinaryOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    lhs = self.lhs.to_painless(not_null=True)
    rhs = self.rhs.to_painless(not_null=True)
    script = "(" + lhs + ") " + BinaryOp.operators[self.op] + " (" + rhs + ")"
    missing = OrOp("or", [self.lhs.missing(), self.rhs.missing()])

    if self.op in BinaryOp.operators:
        script = "(" + script + ").doubleValue()"  # RETURN A NUMBER, NOT A STRING
        default = self.default
    if many:
        script = "[" + script + "]"
        default = ScriptOp("script", self.default.to_painless(many=many))

    output = WhenOp(
        "when",
        missing,
        **{
            "then": default,
            "else":
                ScriptOp("script", script)
        }
    ).to_painless()
    return output


@extend(BinaryOp)
def to_esfilter(self):
    if not isinstance(self.lhs, Variable) or not isinstance(self.rhs, Literal) or self.op in BinaryOp.algebra_ops:
        return ScriptOp("script",  self.to_painless()).to_esfilter()

    if self.op in ["eq", "term"]:
        return {"term": {self.lhs.var: self.rhs.to_esfilter()}}
    elif self.op in ["ne", "neq"]:
        return {"bool": {"must_not":{"term": {self.lhs.var: self.rhs.to_esfilter()}}}}
    elif self.op in BinaryOp.ineq_ops:
        return {"range": {self.lhs.var: {self.op: json2value(self.rhs.json)}}}
    else:
        Log.error("Logic error")


@extend(CaseOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    acc = self.whens[-1].to_painless()
    for w in reversed(self.whens[0:-1]):
        acc = "(" + w.when.to_painless(boolean=True) + ") ? (" + w.then.to_painless() + ") : (" + acc + ")"
    return acc


@extend(CaseOp)
def to_esfilter(self):
    return ScriptOp("script",  self.to_painless()).to_esfilter()


@extend(ConcatOp)
def to_esfilter(self):
    if isinstance(self.value, Variable) and isinstance(self.find, Literal):
        return {"regexp": {self.value.var: ".*" + convert.string2regexp(json2value(self.find.json)) + ".*"}}
    else:
        return ScriptOp("script",  self.to_painless()).to_esfilter()


@extend(ConcatOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    if len(self.terms) == 0:
        return self.default.to_painless()

    acc = []
    for t in self.terms:
        acc.append("((" + t.missing().to_painless(boolean=True) + ") ? \"\" : (" + self.separator.json + "+" + t.to_painless(
            not_null=True) + "))")
    expr_ = "(" + "+".join(acc) + ").substring(" + unicode(len(json2value(self.separator.json))) + ")"

    return "(" + self.missing().to_painless() + ") ? (" + self.default.to_painless() + ") : (" + expr_ + ")"


@extend(Literal)
def to_painless(self, not_null=False, boolean=False, many=False):
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
            return unicode(v)
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
def to_painless(self, not_null=False, boolean=False, many=False):
    if not self.terms:
        return "null"
    acc = self.terms[-1].to_painless()
    for v in reversed(self.terms[:-1]):
        r = v.to_painless()
        acc = "(((" + r + ") != null) ? (" + r + ") : (" + acc + "))"
    return acc


@extend(CoalesceOp)
def to_esfilter(self):
    return {"bool": {"should": [{"exists": {"field": v}} for v in self.terms]}}


@extend(ExistsOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    if isinstance(self.field, Variable):
        return "!doc[" + quote(self.field.var) + "].empty"
    elif isinstance(self.field, Literal):
        return self.field.exists().to_painless()
    else:
        return self.field.to_painless() + " != null"


@extend(ExistsOp)
def to_esfilter(self):
    if isinstance(self.field, Variable):
        return {"exists": {"field": self.field.var}}
    else:
        return ScriptOp("script",  self.to_painless()).to_esfilter()


@extend(FindOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    missing = self.missing()
    v = self.value.to_painless(not_null=True)
    find = self.find.to_painless(not_null=True)
    start = self.start.to_painless(not_null=True)
    index = v + ".indexOf(" + find + ", " + start + ")"

    if not_null:
        no_index = index + "==-1"
    else:
        no_index = missing.to_painless(boolean=True)

    expr = "(" + no_index + ") ? " + self.default.to_painless() + " : " + index
    return expr


@extend(FindOp)
def to_esfilter(self):
    if isinstance(self.value, Variable) and isinstance(self.find, Literal):
        return {"regexp": {self.value.var: ".*" + convert.string2regexp(json2value(self.find.json)) + ".*"}}
    else:
        return ScriptOp("script",  self.to_painless()).to_esfilter()


@extend(Literal)
def to_esfilter(self):
    return json2value(self.json)


@extend(NullOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    if many:
        return "[]"
    return "null"


@extend(NullOp)
def to_esfilter(self):
    return {"bool": {"must_not": {"match_all": {}}}}


@extend(FalseOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    return "false"


@extend(FalseOp)
def to_esfilter(self):
    return {"bool": {"must_not": {"match_all": {}}}}


@extend(DateOp)
def to_esfilter(self):
    return json2value(self.json)


@extend(DateOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    Log.error("not supported")


@extend(TupleOp)
def to_esfilter(self):
    Log.error("not supported")


@extend(LeavesOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    Log.error("not supported")


@extend(LeavesOp)
def to_esfilter(self):
    Log.error("not supported")


@extend(InequalityOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    lhs = self.lhs.to_painless(not_null=True)
    rhs = self.rhs.to_painless(not_null=True)
    script = "(" + lhs + ") " + InequalityOp.operators[self.op] + " (" + rhs + ")"
    missing = OrOp("or", [self.lhs.missing(), self.rhs.missing()])
    default = self.default
    if boolean:
        default = FalseOp()

    output = WhenOp(
        "when",
        missing,
        **{
            "then": default,
            "else":
                ScriptOp("script", script)
        }
    ).to_painless()
    return output


@extend(InequalityOp)
def to_esfilter(self):
    if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
        return {"range": {self.lhs.var: {self.op: json2value(self.rhs.json)}}}
    else:
        return ScriptOp("script", self.to_painless(boolean=True)).to_esfilter()


@extend(DivOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    lhs = self.lhs.to_painless(not_null=True)
    rhs = self.rhs.to_painless(not_null=True)
    script = "((double)(" + lhs + ") / (double)(" + rhs + ")).doubleValue()"

    output = WhenOp(
        "when",
        OrOp("or", [self.lhs.missing(), self.rhs.missing(), EqOp("eq", [self.rhs, Literal("literal", 0)])]),
        **{
            "then": self.default,
            "else":
                ScriptOp("script", script)
        }
    ).to_painless()
    return output


@extend(DivOp)
def to_esfilter(self):
    if not isinstance(self.lhs, Variable) or not isinstance(self.rhs, Literal):
        return ScriptOp("script", self.to_painless()).to_esfilter()
    else:
        Log.error("Logic error")


@extend(FloorOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    lhs = self.lhs.to_painless(not_null=True)
    rhs = self.rhs.to_painless(not_null=True)
    script = "(int)Math.floor(((double)(" + lhs + ") / (double)(" + rhs + ")).doubleValue())*(" + rhs + ")"

    output = WhenOp(
        "when",
        OrOp("or", [self.lhs.missing(), self.rhs.missing(), EqOp("eq", [self.rhs, Literal("literal", 0)])]),
        **{
            "then": self.default,
            "else":
                ScriptOp("script", script)
        }
    ).to_painless()
    return output


@extend(FloorOp)
def to_esfilter(self):
    Log.error("Logic error")


@extend(EqOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    lhs_list = self.lhs.to_painless(many=True)
    rhs_missing = self.rhs.missing().to_painless()
    rhs = self.rhs.to_painless(not_null=True)

    return WhenOp(
        "when",
        self.rhs.missing(),
        **{"then": self.lhs.missing(), "else": InOp("in", [self.rhs, self.lhs])}
    ).partial_eval().to_painless(boolean=True)


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
        return ScriptOp("script", self.to_painless(boolean=True)).to_esfilter()


@extend(MissingOp)
def to_painless(self, not_null=False, boolean=True, many=False):
    if isinstance(self.expr, Variable):
        if self.expr.var == "_id":
            return "false"
        else:
            return "doc[" + quote(self.expr.var) + "].empty"
    elif isinstance(self.expr, Literal):
        return self.expr.missing().to_painless()
    else:
        return self.expr.missing().to_painless()


@extend(MissingOp)
def to_esfilter(self):
    if isinstance(self.expr, Variable):
        return {"bool": {"must_not": {"exists": {"field": self.expr.var}}}}
    else:
        return ScriptOp("script", self.to_painless()).to_esfilter()


@extend(NotLeftOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    test_v = self.value.missing().to_painless(boolean=True)
    test_l = self.length.missing().to_painless(boolean=True)
    v = self.value.to_painless(not_null=True)
    l = self.length.to_painless(not_null=True)

    expr = "((" + test_v + ") || (" + test_l + ")) ? null : (" + v + ".substring((int)Math.max(0, (int)Math.min(" + v + ".length(), " + l + "))))"
    return expr


@extend(NeOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    lhs = self.lhs.to_painless()
    rhs = self.rhs.to_painless()
    return "((" + lhs + ")!=null) && ((" + rhs + ")!=null) && ((" + lhs + ")!=(" + rhs + "))"


@extend(NeOp)
def to_esfilter(self):
    if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
        return {"bool": {"must_not": {"term": {self.lhs.var: self.rhs.to_esfilter()}}}}
    else:
        return {"bool": {"must": [
            # TODO: MAKE TESTS TO SEE IF THIS LOGIC IS CORRECT
            {"bool": {"must": [{"exists": {"field": v}} for v in self.vars()]}},
            ScriptOp("script", self.to_painless()).to_esfilter()
        ]}}


@extend(NotOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    return "!(" + self.term.to_painless(boolean=True) + ")"


@extend(NotOp)
def to_esfilter(self):
    operand = self.term.to_esfilter()
    return {"bool": {"must_not": operand}}


@extend(AndOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    if not self.terms:
        return "true"
    else:
        return " && ".join("(" + t.to_painless() + ")" for t in self.terms)


@extend(AndOp)
def to_esfilter(self):
    if not len(self.terms):
        return {"match_all": {}}
    else:
        return {"bool": {"must": [t.to_esfilter() for t in self.terms]}}


@extend(OrOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    return " || ".join("(" + t.to_painless(boolean=True) + ")" for t in self.terms if t)


@extend(OrOp)
def to_esfilter(self):
    return {"bool": {"should": [t.to_esfilter() for t in self.terms]}}


@extend(LengthOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    value = self.term.to_painless(not_null=True)
    if not_null:
        return "(" + value + ").length()"

    missing = self.missing().to_painless()
    if many:
        return "(" + missing + " ) ? [] : [(" + value + ").length()]"
    else:
        return "(" + missing + " ) ? null : (" + value + ").length()"


@extend(NumberOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    test = self.term.missing().to_painless(boolean=True)
    value = self.term.to_painless(not_null=True)
    return "(" + test + ") ? null : (((" + value + ") instanceof String) ? Double.parseDouble(" + value + ") : (" + value + "))"


@extend(CountOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    return "+".join("((" + t.missing().to_painless(boolean=True) + ") ? 0 : 1)" for t in self.terms)


@extend(LengthOp)
def to_esfilter(self):
    return {"regexp": {self.var.var: json2value(self.pattern.json)}}


@extend(MultiOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    if self.nulls:
        op, unit = MultiOp.operators[self.op]
        null_test = CoalesceOp("coalesce", self.terms).missing().to_painless(boolean=True)
        acc = op.join(
            "((" + t.missing().to_painless(boolean=True) + ") ? " + unit + " : (" + t.to_painless(not_null=True) + "))" for
            t in self.terms
        )
        if many:
            acc = "[" + acc + "]"
        return "((" + null_test + ") ? (" + self.default.to_painless(many=many) + ") : (" + acc + "))"
    else:
        op, unit = MultiOp.operators[self.op]
        null_test = OrOp("or", [t.missing() for t in self.terms]).to_painless()
        acc = op.join("(" + t.to_painless(not_null=True) + ")" for t in self.terms)
        if many:
            acc = "[" + acc + "]"
        return "((" + null_test + ") ? (" + self.default.to_painless(many=many) + ") : (" + acc + "))"


@extend(RegExpOp)
def to_esfilter(self):
    return {"regexp": {self.var.var: json2value(self.pattern.json)}}


@extend(StringOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    value = self.term.to_painless(not_null=True)
    missing = self.term.missing().to_painless()
    return "(" + missing + ") ? null : (((" + value + ") instanceof java.lang.Double) ? String.valueOf(" + value + ").replaceAll('\\\\.0$', '') : String.valueOf(" + value + "))"  # "\\.0$"


@extend(TrueOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    return "true"


@extend(TrueOp)
def to_esfilter(self):
    return {"match_all": {}}


@extend(PrefixOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    return "(" + self.field.to_painless() + ").startsWith(" + self.prefix.to_painless() + ")"


@extend(PrefixOp)
def to_esfilter(self):
    if isinstance(self.field, Variable) and isinstance(self.prefix, Literal):
        return {"prefix": {self.field.var: json2value(self.prefix.json)}}
    else:
        return ScriptOp("script",  self.to_painless()).to_esfilter()


@extend(RightOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    test_v = self.value.missing().to_painless(boolean=True)
    test_l = self.length.missing().to_painless(boolean=True)
    v = self.value.to_painless(not_null=True)
    l = self.length.to_painless(not_null=True)

    expr = "((" + test_v + ") || (" + test_l + ")) ? null : (" + v + ".substring((int)Math.min(" + v + ".length(), (int)Math.max(0, (" + v + ").length() - (" + l + ")))))"
    return expr


@extend(NotRightOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    test_v = self.value.missing().to_painless(boolean=True)
    test_l = self.length.missing().to_painless(boolean=True)
    v = self.value.to_painless(not_null=True)
    l = self.length.to_painless(not_null=True)

    expr = "((" + test_v + ") || (" + test_l + ")) ? null : (" + v + ".substring(0, (int)Math.min(" + v + ".length(), (int)Math.max(0, (" + v + ").length() - (" + l + ")))))"
    return expr


@extend(InOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    return self.superset.to_painless(many=True) + ".contains(" + self.value.to_painless() + ")"


@extend(InOp)
def to_esfilter(self):
    if isinstance(self.value, Variable):
        return {"terms": {self.value.var: json2value(self.superset.json)}}
    else:
        return {"script": self.to_painless()}



@extend(RangeOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    return "(" + self.when.to_painless(boolean=True) + ") ? (" + self.then.to_painless(
        not_null=not_null) + ") : (" + self.els_.to_painless(not_null=not_null) + ")"


@extend(RangeOp)
def to_esfilter(self):
    return {"bool": {"should": [
        {"bool": {"must": [
            self.when.to_esfilter(),
            self.then.to_esfilter()
        ]}},
        {"bool": {"must": [
            {"bool": {"must_not": self.when.to_esfilter()}},
            self.els_.to_esfilter()
        ]}}
    ]}}


@extend(LeftOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    test_v = self.value.missing()
    test_l = self.length.missing()
    v = self.value.to_painless(not_null=True)
    l = self.length.to_painless(not_null=True)

    if (not test_v or test_v.to_painless(boolean=True) == "false") and not test_l:
        expr = v + ".substring(0, (int)Math.max(0, (int)Math.min(" + v + ".length(), " + l + ")))"
    else:
        expr = "((" + test_v.to_painless(boolean=True) + ") || (" + test_l.to_painless(boolean=True) + ")) ? null : (" + v + ".substring(0, (int)Math.max(0, (int)Math.min(" + v + ".length(), " + l + "))))"
    return expr


@extend(ScriptOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    return self.script


@extend(ScriptOp)
def to_esfilter(self):
    return {"script": {"script": {"lang": "painless", "inline": self.script}}}


@extend(Variable)
def to_painless(self, not_null=False, boolean=False, many=False):
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
                return "doc[" + q + "].empty ? null : (doc[" + q + "].value==\"T\")"
            else:
                return "doc[" + q + "].empty ? null : doc[" + q + "].value"


@extend(WhenOp)
def to_painless(self, not_null=False, boolean=False, many=False):
    if self.simplified:
        return "(" + self.when.to_painless(boolean=True) + ") ? (" + self.then.to_painless(not_null=not_null) + ") : (" + self.els_.to_painless(not_null=not_null) + ")"
    else:
        return self.partial_eval().to_painless()


@extend(WhenOp)
def to_esfilter(self):
    return {"bool": {"should": [
        {"bool": {"must": [
            self.when.to_esfilter(),
            self.then.to_esfilter()
        ]}},
        {"bool": {"must": [
            {"bool": {"must_not": self.when.to_esfilter()}},
            self.els_.to_esfilter()
        ]}}
    ]}}


USE_BOOL_MUST = True


def simplify_esfilter(esfilter):
    try:
        output = normalize_esfilter(esfilter)
        if output is TRUE_FILTER:
            return {"match_all": {}}
        elif output is FALSE_FILTER:
            return {"bool": {"must_not": {"match_all": {}}}}

        output.isNormal = None
        return output
    except Exception as e:
        from mo_logs import Log

        Log.unexpected("programmer error", cause=e)


def removeOr(esfilter):
    if esfilter["not"]:
        return {"bool": {"must_not": removeOr(esfilter["not"])}}

    if esfilter["and"]:
        return {"bool": {"must": [removeOr(v) for v in esfilter["and"]]}}

    if esfilter["or"]:  # CONVERT OR TO NOT.AND.NOT
        return {"bool": {"must_not": {"bool": {"must": [{"bool": {"must_not": removeOr(v)} for v in esfilter["or"]}]}}}}

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
                                "bool": {"should": [
                                    {"bool": {"must_not": {"exists": {"field": k}}}},
                                    {"terms": {k: rest}}
                                ]},
                                "isNormal": True
                            }
                        else:
                            return {
                                "bool": {"must_not": {"exists": {"field": k}}},
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
                return wrap({"or":  sub, "isNormal": True})
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
