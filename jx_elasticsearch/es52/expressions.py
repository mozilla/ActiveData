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

from future.utils import binary_type, text_type

from jx_base import JSON_TYPES, NUMBER, STRING, BOOLEAN, OBJECT, INTEGER
from mo_dots import coalesce, wrap, Null, unwraplist, literal_field, set_default, Data, listwrap
from mo_json import json2value, quote
from mo_logs import Log, suppress_exception
from mo_math import OR, MAX
from pyLibrary.convert import json_decoder, string2regexp

from jx_base.expressions import Variable, DateOp, TupleOp, LeavesOp, BinaryOp, OrOp, ScriptOp, \
    WhenOp, InequalityOp, extend, RowsOp, Literal, NullOp, TrueOp, FalseOp, DivOp, FloorOp, \
    EqOp, NeOp, NotOp, LengthOp, NumberOp, StringOp, CountOp, MultiOp, RegExpOp, CoalesceOp, MissingOp, ExistsOp, \
    PrefixOp, UnixOp, NotLeftOp, RightOp, NotRightOp, FindOp, BetweenOp, InOp, RangeOp, CaseOp, AndOp, \
    ConcatOp, IsNumberOp, TRUE_FILTER, FALSE_FILTER, LeftOp, Expression, BasicIndexOfOp, MaxOp, MinOp, BasicEqOp, BooleanOp, IntegerOp, BasicSubstringOp


class Painless(Expression):
    __slots__ = ("miss", "type", "expr", "many")

    def __init__(self, type, expr, frum, miss=None, many=False):
        self.miss = coalesce(miss, FalseOp())  # Expression that will return true/false to indicate missing result
        self.data_type = type
        self.expr = expr
        self.many = many  # True if script returns multi-value
        self.frum = frum

    @property
    def type(self):
        return self.data_type

    def script(self, schema):
        """
        RETURN A SCRIPT SUITABLE FOR CODE OUTSIDE THIS MODULE (NO KNOWLEDGE OF Painless)
        :param schema:
        :return:
        """
        missing = self.miss.partial_eval()
        if isinstance(missing, FalseOp):
            return self.partial_eval().to_painless(schema).expr
        elif isinstance(missing, TrueOp):
            return "null"

        return "(" + missing.to_painless(schema).expr + ")?null:(" + self.expr + ")"

    def to_esfilter(self, schema):
        return {"script": {"script": {"lang": "painless", "inline": self.script}}}

    def to_painless(self, schema):
        return self

    def missing(self):
        return self.miss

    def __data__(self):
        return {"script": self.script}


@extend(BinaryOp)
def to_painless(self, schema):
    lhs = NumberOp("number", self.lhs).partial_eval().to_painless(schema).expr
    rhs = NumberOp("number", self.rhs).partial_eval().to_painless(schema).expr
    script = "(" + lhs + ") " + BinaryOp.operators[self.op] + " (" + rhs + ")"
    missing = OrOp("or", [self.lhs.missing(), self.rhs.missing()])

    return WhenOp(
        "when",
        missing,
        **{
            "then": self.default,
            "else":
                Painless(type=NUMBER, expr=script)
        }
    ).partial_eval().to_painless(schema)


@extend(BinaryOp)
def to_esfilter(self, schema):
    if not isinstance(self.lhs, Variable) or not isinstance(self.rhs, Literal) or self.op in BinaryOp.algebra_ops:
        return ScriptOp("script",  self.to_painless(schema)).to_esfilter(schema)

    if self.op in ["eq", "term"]:
        return {"term": {self.lhs.var: self.rhs.to_esfilter(schema)}}
    elif self.op in ["ne", "neq"]:
        return {"bool": {"must_not":{"term": {self.lhs.var: self.rhs.to_esfilter(schema)}}}}
    elif self.op in BinaryOp.ineq_ops:
        return {"range": {self.lhs.var: {self.op: json2value(self.rhs.json)}}}
    else:
        Log.error("Logic error")


@extend(CaseOp)
def to_painless(self, schema):
    acc = self.whens[-1].partial_eval().to_painless(schema)
    for w in reversed(self.whens[0:-1]):
        acc = WhenOp(
            "when",
            w.when,
            **{"then": w.then, "else": acc}
        ).partial_eval().to_painless(schema)
    return acc


@extend(CaseOp)
def to_esfilter(self, schema):
    return ScriptOp("script",  self.to_painless(schema)).to_esfilter(schema)


@extend(ConcatOp)
def to_esfilter(self, schema):
    if isinstance(self.value, Variable) and isinstance(self.find, Literal):
        return {"regexp": {self.value.var: ".*" + string2regexp(json2value(self.find.json)) + ".*"}}
    else:
        return ScriptOp("script",  self.to_painless(schema)).to_esfilter(schema)


@extend(ConcatOp)
def to_painless(self, schema):
    if len(self.terms) == 0:
        return self.default.to_painless(schema)

    acc = []
    separator = StringOp("string", self.separator)
    sep = separator.to_painless(schema).expr
    for t in self.terms:
        val = WhenOp(
            "when",
            t.missing(),
            **{
                "then": Literal("literal", ""),
                "else": Painless(type=STRING, expr=sep + "+" + StringOp(None, t).to_painless(schema).expr)
            }
        )
        acc.append("(" + val.partial_eval().to_painless(schema).expr + ")")
    expr_ = "(" + "+".join(acc) + ").substring(" + LengthOp("length", separator).to_painless(schema).expr + ")"

    if isinstance(self.default, NullOp):
        return Painless(
            miss=self.missing(),
            type=STRING, expr=expr_
        )
    else:
        return Painless(
            miss=self.missing(),
            type=STRING, expr="((" + expr_ + ").length==0) ? (" + self.default.to_painless(schema).expr + ") : (" + expr_ + ")"
        )


@extend(Literal)
def to_painless(self, schema):
    def _convert(v):
        if v is None:
            return NullOp().to_painless(schema)
        if v is True:
            return Painless(
                type=BOOLEAN,
                expr="true",
                frum=self
            )
        if v is False:
            return Painless(
                type=BOOLEAN,
                expr="false",
                frum=self
            )
        if isinstance(v, (text_type, binary_type)):
            return Painless(
                type=STRING,
                expr=quote(v),
                frum=self
            )
        if isinstance(v, (int, long)):
            return Painless(
                type=INTEGER,
                expr=text_type(v),
                frum=self
            )
        if isinstance(v, (float)):
            return Painless(
                type=NUMBER,
                expr=text_type(v),
                frum=self
            )
        if isinstance(v, dict):
            return Painless(
                type=OBJECT,
                expr="[" + ", ".join(quote(k) + ": " + _convert(vv) for k, vv in v.items()) + "]",
                frum=self
            )
        if isinstance(v, list):
            return Painless(
                type=OBJECT,
                expr="[" + ", ".join(_convert(vv).expr for vv in v) + "]",
                frum=self
            )

    return _convert(self.value)


@extend(CoalesceOp)
def to_painless(self, schema):
    if not self.terms:
        return NullOp().to_painless(schema)

    acc = self.terms[-1].to_painless(schema)
    for v in reversed(self.terms[:-1]):
        m = v.missing()
        r = v.to_painless(schema)

        if acc.type == r.type:
            new_type = r.type
        elif acc.type == NUMBER and r.type == INTEGER:
            new_type = NUMBER
        elif acc.type == INTEGER and r.type == NUMBER:
            new_type = NUMBER
        else:
            new_type = OBJECT

        acc = Painless(
            miss=AndOp("and", [acc.miss, m]),
            type=new_type,
            expr="(!(" + m.expr + ")) ? (" + r.expr + ") : (" + acc.expr + ")",
            frum=self
        )
    return acc


@extend(CoalesceOp)
def to_esfilter(self, schema):
    return {"bool": {"should": [{"exists": {"field": v}} for v in self.terms]}}


@extend(ExistsOp)
def to_painless(self, schema):
    if isinstance(self.field, Variable):
        return "!doc[" + quote(self.field.var) + "].empty"
    elif isinstance(self.field, Literal):
        return self.field.exists().to_painless(schema)
    else:
        return self.field.to_painless(schema) + " != null"


@extend(ExistsOp)
def to_esfilter(self, schema):
    if isinstance(self.field, Variable):
        return {"exists": {"field": self.field.var}}
    else:
        return ScriptOp("script",  self.to_painless(schema)).to_esfilter(schema)


@extend(Literal)
def to_esfilter(self, schema):
    return json2value(self.json)


@extend(NullOp)
def to_painless(self, schema):
    return Painless(
        miss=TrueOp(),
        type=OBJECT,
        expr="null",
        frum=self
    )

@extend(NullOp)
def to_esfilter(self, schema):
    return {"bool": {"must_not": {"match_all": {}}}}


@extend(FalseOp)
def to_painless(self, schema):
    return Painless(type=BOOLEAN, expr="false")


@extend(FalseOp)
def to_esfilter(self, schema):
    return {"bool": {"must_not": {"match_all": {}}}}


@extend(DateOp)
def to_esfilter(self, schema):
    return json2value(self.json)


@extend(DateOp)
def to_painless(self, schema):
    Log.error("not supported")


@extend(TupleOp)
def to_esfilter(self, schema):
    Log.error("not supported")


@extend(LeavesOp)
def to_painless(self, schema):
    Log.error("not supported")


@extend(LeavesOp)
def to_esfilter(self, schema):
    Log.error("not supported")


@extend(InequalityOp)
def to_painless(self, schema):
    lhs = NumberOp("number", self.lhs).partial_eval().to_painless(schema).expr
    rhs = NumberOp("number", self.rhs).partial_eval().to_painless(schema).expr
    script = "(" + lhs + ") " + InequalityOp.operators[self.op] + " (" + rhs + ")"

    output = WhenOp(
        "when",
        OrOp("or", [self.lhs.missing(), self.rhs.missing()]),
        **{
            "then": FalseOp(),
            "else":
                Painless(type=BOOLEAN, expr=script)
        }
    ).partial_eval().to_painless(schema)
    return output


@extend(InequalityOp)
def to_esfilter(self, schema):
    if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
        return {"range": {self.lhs.var: {self.op: json2value(self.rhs.json)}}}
    else:
        return ScriptOp("script", self.to_painless(boolean=True).script).to_esfilter(schema)


@extend(DivOp)
def to_painless(self, schema):
    lhs = self.lhs.partial_eval()
    rhs = self.rhs.partial_eval()
    script = "Double.valueOf((double)(" + lhs.to_painless(schema).expr + ") / (double)(" + rhs.to_painless(schema).expr + "))"

    output = WhenOp(
        "when",
        OrOp("or", [lhs.missing(), rhs.missing(), EqOp("eq", [rhs, Literal("literal", 0)])]),
        **{
            "then": self.default,
            "else": Painless(type=NUMBER, expr=script)
        }
    ).partial_eval().to_painless(schema)

    return output


@extend(DivOp)
def to_esfilter(self, schema):
    if not isinstance(self.lhs, Variable) or not isinstance(self.rhs, Literal):
        return ScriptOp("script", self.to_painless(schema)).to_esfilter(schema)
    else:
        Log.error("Logic error")


@extend(FloorOp)
def to_painless(self, schema):
    lhs = self.lhs.to_painless(schema)
    rhs = self.rhs.to_painless(schema)
    script = "(int)Math.floor(((double)(" + lhs + ") / (double)(" + rhs + ")).doubleValue())*(" + rhs + ")"

    output = WhenOp(
        "when",
        OrOp("or", [self.lhs.missing(), self.rhs.missing(), EqOp("eq", [self.rhs, Literal("literal", 0)])]),
        **{
            "then": self.default,
            "else":
                ScriptOp("script", script)
        }
    ).to_painless(schema)
    return output


@extend(FloorOp)
def to_esfilter(self, schema):
    Log.error("Logic error")

@extend(EqOp)
def to_painless(self, schema):
    lhs = self.lhs.partial_eval().to_painless(schema)
    rhs = self.rhs.partial_eval().to_painless(schema)

    if lhs.many:
        if rhs.many:
            return Painless(
                boolean=WhenOp(
                    "when",
                    self.lhs.missing(),
                    **{
                        "then": self.rhs.missing(),
                        "else": AndOp("and", [
                            Painless(type=BOOLEAN, expr="(" + lhs.expr + ").size()==(" + rhs.expr + ").size()"),
                            Painless(type=BOOLEAN, expr="(" + rhs.expr + ").containsAll(" + lhs.expr + ")")
                        ])
                    }
                ).partial_eval().to_painless(boolean=True).expr
            )
        return Painless(
            boolean=WhenOp(
                "when",
                self.lhs.missing(),
                **{
                    "then": self.rhs.missing(),
                    "else": Painless(type=BOOLEAN, expr="(" + lhs.expr + ").contains(" + rhs.expr + ")")
                }
            ).partial_eval().to_painless(boolean=True).expr
        )
    elif rhs.many:
        return Painless(
            boolean=WhenOp(
                "when",
                self.lhs.missing(),
                **{
                    "then": self.rhs.missing(),
                    "else": Painless(type=BOOLEAN, expr="(" + rhs.expr + ").contains(" + lhs.expr + ")")
                }
            ).partial_eval().to_painless(boolean=True).expr
        )
    else:
        return Painless(type=BOOLEAN, expr="(" + lhs.expr + "==" + rhs.expr + ")")


@extend(EqOp)
def to_esfilter(self, schema):
    if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
        rhs = self.rhs.value
        if isinstance(rhs, list):
            if len(rhs) == 1:
                return {"term": {self.lhs.var: rhs[0]}}
            else:
                return {"terms": {self.lhs.var: rhs}}
        else:
            return {"term": {self.lhs.var: rhs}}
    else:
        return self.to_painless(schema).to_esfilter(schema)


@extend(BasicEqOp)
def to_painless(self, schema):
    lhs = self.lhs.partial_eval().to_painless(schema)
    rhs = self.rhs.partial_eval().to_painless(schema)

    if lhs.many:
        if rhs.many:
            return AndOp("and", [
                Painless(type=BOOLEAN, expr="(" + lhs.expr + ").size()==(" + rhs.expr + ").size()", frum=self),
                Painless(type=BOOLEAN, expr="(" + rhs.expr + ").containsAll(" + lhs.expr + ")", frum=self)
            ]).to_painless()
        else:
            return Painless(type=BOOLEAN, expr="(" + lhs.expr + ").contains(" + rhs.expr + ")",frum=self)
    elif rhs.many:
        return Painless(
            type=BOOLEAN,
            expr="(" + rhs.expr + ").contains(" + lhs.expr + ")",
            frum=self
        )
    else:
        return Painless(
            type=BOOLEAN,
            expr="(" + lhs.expr + "==" + rhs.expr + ")",
            frum=self
        )


@extend(BasicEqOp)
def to_esfilter(self, schema):
    if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
        rhs = self.rhs.value
        if isinstance(rhs, list):
            if len(rhs) == 1:
                return {"term": {self.lhs.var: rhs[0]}}
            else:
                return {"terms": {self.lhs.var: rhs}}
        else:
            return {"term": {self.lhs.var: rhs}}
    else:
        return self.to_painless(schema).to_esfilter(schema)



@extend(MissingOp)
def to_painless(self, schema, not_null=False, boolean=True):
    if isinstance(self.expr, Variable):
        if self.expr.var == "_id":
            return Painless(type=BOOLEAN, expr="false", frum=self)
        else:
            columns = schema.leaves(self.expr.var)
            if len(columns)==1:
                return Painless(type=BOOLEAN, expr="doc[" + quote(columns[0].es_column) + "].empty", frum=self)
            else:
                return AndOp("and", [
                    Painless(type=BOOLEAN, expr="doc[" + quote(c.es_column) + "].empty", frum=self)
                    for c in columns
                ]).partial_eval().to_painless()
    elif isinstance(self.expr, Literal):
        return self.expr.missing().to_painless(schema)
    else:
        return self.expr.missing().to_painless(schema)


@extend(MissingOp)
def to_esfilter(self, schema):
    if isinstance(self.expr, Variable):
        return {"bool": {"must_not": {"exists": {"field": self.expr.var}}}}
    else:
        return ScriptOp("script", self.to_painless(schema)).to_esfilter(schema)


@extend(NotLeftOp)
def to_painless(self, schema):
    v = StringOp("string", self.value).partial_eval().to_painless(schema).expr
    l = NumberOp("number", self.length).partial_eval().to_painless(schema).expr

    expr = "(" + v + ").substring((int)Math.max(0, (int)Math.min(" + v + ".length(), " + l + ")))"
    return Painless(
        miss=OrOp("or", [self.value.missing(), self.length.missing()]),
        type=STRING,
        expr=expr,
        frum=self
    )


@extend(NeOp)
def to_painless(self, schema):
    return NotOp("not", EqOp("eq", [self.lhs, self.rhs])).partial_eval().to_painless(schema)


@extend(NeOp)
def to_esfilter(self, schema):
    if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
        return {"bool": {"must_not": {"term": {self.lhs.var: self.rhs.to_esfilter(schema)}}}}
    else:

        calc = self.to_painless(schema)

        return {"bool": {"must": [
            # TODO: MAKE TESTS TO SEE IF THIS LOGIC IS CORRECT
            {"bool": {"must": [{"exists": {"field": v}} for v in self.vars()]}},
            ScriptOp("script", self.to_painless(schema)).to_esfilter(schema)
        ]}}


@extend(NotOp)
def to_painless(self, schema):
    return Painless(type=BOOLEAN, expr="!(" + self.term.to_painless(boolean=True).expr + ")")


@extend(NotOp)
def to_esfilter(self, schema):
    operand = self.term.to_esfilter(schema)
    return {"bool": {"must_not": operand}}


@extend(AndOp)
def to_painless(self, schema):
    if not self.terms:
        return TrueOp().to_painless()
    else:
        return Painless(
            miss=FalseOp(),
            type=BOOLEAN,
            expr=" && ".join("(" + t.to_painless(schema).expr + ")" for t in self.terms),
            frum=self
        )


@extend(AndOp)
def to_esfilter(self, schema):
    if not len(self.terms):
        return {"match_all": {}}
    else:
        return {"bool": {"must": [t.to_esfilter(schema) for t in self.terms]}}


@extend(OrOp)
def to_painless(self, schema):
    return Painless(
        miss=FalseOp(),
        type=BOOLEAN,
        expr=" || ".join("(" + t.to_painless(schema).expr + ")" for t in self.terms if t),
        frum=self
    )


@extend(OrOp)
def to_esfilter(self, schema):
    return {"bool": {"should": [t.to_esfilter(schema) for t in self.terms]}}


@extend(LengthOp)
def to_painless(self, schema):
    value = StringOp("string", self.term).to_painless(schema)
    missing = self.term.missing().partial_eval()
    return Painless(
        miss=missing,
        type=INTEGER,
        expr="(" + value.expr + ").length()",
        frum=self
    )

@extend(BooleanOp)
def to_painless(self, schema):
    value = self.term.to_painless(schema)

    for t in "insj":
        if getattr(value, t):
            Log.error("Got {{type|quote}}, expecting a boolean from {{term}}", type=t, term=self.term.__data__())
    return value


@extend(IntegerOp)
def to_painless(self, schema):
    value = self.term.to_painless(schema)
    if value.type == BOOLEAN:
        return Painless(
            miss=value.missing,
            type=INTEGER,
            expr=value.expr + " ? 1 : 0",
            frum=self
        )
    elif value.type == INTEGER:
        return value
    elif value.type == NUMBER:
        return Painless(
            miss=value.missing,
            type=INTEGER,
            expr="(int)(" + value.expr + ")",
            frum=self
        )
    elif value.type == STRING:
        return Painless(
            miss=value.missing,
            type=INTEGER,
            expr="Integer.parseInt(" + value.expr + ")",
            frum=self
        )
    elif value.many:
        return IntegerOp("integer", Painless(
            miss=value.missing,
            type=value.type,
            expr="(" + value.expr + ")[0]",
            frum=value.frum
        )).to_painless(schema)
    else:
        return Painless(
            miss=value.missing,
            type=INTEGER,
            expr="((" + value.expr + ") instanceof String) ? Integer.parseInt(" + value.expr + ") : (int)(" + value.expr + ")",
            frum=self
        )

@extend(NumberOp)
def to_painless(self, schema):

    term=self.term.partial_eval()
    if isinstance(term, CoalesceOp):
        return CoalesceOp("coalesce", [NumberOp("number", t) for t in term.terms]).partial_eval().to_painless(schema)

    value = term.to_painless(schema)
    if value.type == BOOLEAN:
        return Painless(
            miss=term.missing(),
            type=INTEGER,
            expr=value.expr + " ? 1 : 0",
            frum=self
        )
    elif value.type == INTEGER:
        return Painless(
            miss=term.missing(),
            type=INTEGER,
            expr=value.expr,
            frum=self
        )
    elif value.type == NUMBER:
        return Painless(
            miss=term.missing(),
            type=NUMBER,
            expr=value.expr,
            frum=self
        )
    elif value.type == STRING:
        return Painless(
            miss=term.missing(),
            type=NUMBER,
            expr="Double.parseDouble(" + value.expr + ")",
            frum=self
        )
    elif value.type == OBJECT:
        return Painless(
            miss=term.missing(),
            type=NUMBER,
            expr="((" + value.expr + ") instanceof String) ? Double.parseDouble(" + value.expr + ") : (" + value.expr + ")",
            frum=self
        )


@extend(IsNumberOp)
def to_painless(self, schema):
    value = self.term.to_painless(schema)
    if value.expr or value.i:
        return TrueOp().to_painless(schema)
    else:
        return Painless(
            miss=FalseOp(),
            type=BOOLEAN,
            expr="(" + value.expr + ") instanceof java.lang.Double",
            frum=self
        )

@extend(CountOp)
def to_painless(self, schema):
    return Painless(
        miss=FalseOp(),
        type=INTEGER,
        expr="+".join("((" + t.missing().partial_eval().to_painless().expr + ") ? 0 : 1)" for t in self.terms),
        frum=self
    )


@extend(LengthOp)
def to_esfilter(self, schema):
    return {"regexp": {self.var.var: json2value(self.pattern.json)}}


@extend(MaxOp)
def to_painless(self, schema):
    acc = NumberOp("number", self.terms[-1]).partial_eval().to_painless(schema).expr
    for t in reversed(self.terms[0:-1]):
        acc = "Math.max(" + NumberOp("number", t).partial_eval().to_painless(schema).expr + " , " + acc + ")"
    return Painless(
        miss=AndOp("or", [t.missing() for t in self.terms]),
        type=NUMBER,
        expr=acc,
        frum=self
    )


@extend(MinOp)
def to_painless(self, schema):
    acc = NumberOp("number", self.terms[-1]).partial_eval().to_painless(schema).expr
    for t in reversed(self.terms[0:-1]):
        acc = "Math.min(" + NumberOp("number", t).partial_eval().to_painless(schema).expr + " , " + acc + ")"
    return Painless(
        miss=AndOp("or", [t.missing() for t in self.terms]),
        type=NUMBER,
        expr=acc,
        frum=self
    )


@extend(MultiOp)
def to_painless(self, schema):
    op, unit = MultiOp.operators[self.op]
    if self.nulls:
        calc = op.join(
            "((" + t.missing().to_painless(boolean=True).expr + ") ? " + unit + " : (" + NumberOp("number", t).partial_eval().to_painless(schema).expr + "))" for
            t in self.terms
        )
        return WhenOp(
            "when",
            AndOp("and", [t.missing() for t in self.terms]),
            **{"then": self.default, "else": Painless(type=NUMBER, expr=calc)}
        ).partial_eval().to_painless(schema)
    else:
        calc = op.join(
            "(" + NumberOp("number", t).to_painless(schema).expr + ")"
            for t in self.terms
        )
        return WhenOp(
            "when",
            OrOp("or", [t.missing() for t in self.terms]),
            **{"then": self.default, "else": Painless(type=NUMBER, expr=calc, frum=self)}
        ).partial_eval().to_painless(schema)


@extend(RegExpOp)
def to_esfilter(self, schema):
    return {"regexp": {self.var.var: json2value(self.pattern.json)}}


@extend(StringOp)
def to_painless(self, schema):
    value = self.term.to_painless(schema)

    if isinstance(value.frum, CoalesceOp):
        return CoalesceOp("coalesce", [StringOp("string", t).partial_eval() for t in value.frum.terms]).to_painless(schema)

    if value.many:
        return StringOp("string", Painless(
            miss=value.miss,
            type=value.type,
            expr="(" + value.expr + ")[0]",
            frum=self
        )).to_painless(schema)

    if value.type == BOOLEAN:
        return Painless(
            miss=self.term.missing().partial_eval(),
            type=STRING,
            expr=value.expr + ' ? "T" : "F"',
            frum=self
        )
    elif value.type == INTEGER:
        return Painless(
            miss=self.term.missing().partial_eval(),
            type=STRING,
            expr="String.valueOf(" + value.expr + ")",
            frum=self
        )
    elif value.type == NUMBER:
        return Painless(
            miss=self.term.missing().partial_eval(),
            type=STRING,
            expr="(" + value.expr + "==(int)(" + value.expr + ")) ? String.valueOf((int)" + value.expr + "):String.valueOf(" + value.expr + ")",
            frum=self
        )
    elif value.type == STRING:
        return value
    else:
        return Painless(
            miss=self.term.missing().partial_eval(),
            type=STRING,
            expr="((" + value.expr + ") instanceof java.lang.Double) ? String.valueOf(" + value.expr + ").replaceAll('\\\\.0$', '') : String.valueOf(" + value.expr + ")",
            frum=self
        )

@extend(TrueOp)
def to_painless(self, schema):
    return Painless(type=BOOLEAN, expr="true", frum=self)


@extend(TrueOp)
def to_esfilter(self, schema):
    return {"match_all": {}}


@extend(PrefixOp)
def to_painless(self, schema):
    return "(" + self.field.to_painless(schema) + ").startsWith(" + self.prefix.to_painless(schema) + ")"


@extend(PrefixOp)
def to_esfilter(self, schema):
    if isinstance(self.field, Variable) and isinstance(self.prefix, Literal):
        return {"prefix": {self.field.var: json2value(self.prefix.json)}}
    else:
        return ScriptOp("script",  self.to_painless(schema)).to_esfilter(schema)


@extend(RightOp)
def to_painless(self, schema):
    v = StringOp("string", self.value).partial_eval().to_painless(schema).expr
    l = NumberOp("number", self.length).partial_eval().to_painless(schema).expr

    expr = "(" + v + ").substring((int)Math.min(" + v + ".length(), (int)Math.max(0, (" + v + ").length() - (" + l + "))))"
    return Painless(
        miss=OrOp("or", [self.value.missing(), self.length.missing()]),
        type=STRING, expr=expr
    )


@extend(NotRightOp)
def to_painless(self, schema):
    v = StringOp("string", self.value).partial_eval().to_painless(schema).expr
    l = NumberOp("number", self.length).partial_eval().to_painless(schema).expr

    expr = "(" + v + ").substring(0, (int)Math.min(" + v + ".length(), (int)Math.max(0, (" + v + ").length() - (" + l + "))))"
    return Painless(
        miss=OrOp("or", [self.value.missing(), self.length.missing()]),
        type=STRING,
        expr=expr,
        frum=self
    )


@extend(InOp)
def to_painless(self, schema):
    return Painless(type=BOOLEAN, expr="(" + self.superset.to_painless(schema).expr + ").contains(" + self.value.to_painless(schema).expr + ")")


@extend(InOp)
def to_esfilter(self, schema):
    if isinstance(self.value, Variable):
        return {"terms": {self.value.var: json2value(self.superset.json)}}
    else:
        return {"script": {"script": {"lang": "painless", "inline": self.to_painless(schema).script}}}


@extend(LeftOp)
def to_painless(self, schema):
    v = StringOp("string", self.value).partial_eval().to_painless(schema).expr
    l = NumberOp("number", self.length).partial_eval().to_painless(schema).expr

    expr = "(" + v + ").substring(0, (int)Math.max(0, (int)Math.min((" + v + ").length(), " + l + ")))"
    return Painless(
        miss=OrOp("or", [self.value.missing(), self.length.missing()]),
        type=STRING,
        expr=expr
    )


@extend(ScriptOp)
def to_painless(self, schema):
    return Painless(type=OBJECT, expr=self.script)


@extend(ScriptOp)
def to_esfilter(self, schema):
    return {"script": {"script": {"lang": "painless", "inline": self.script}}}


@extend(Variable)
def to_painless(self, schema):
    if self.var == ".":
        return "_source"
    else:
        if self.var == "_id":
            return Painless(type=STRING, expr='doc["_uid"].value.substring(doc["_uid"].value.indexOf(\'#\')+1)', frum=self)

        columns = schema.leaves(self.var)
        acc = []
        for c in columns:
            varname = c.es_column
            q = quote(varname)
            acc.append(Painless(
                miss=Painless(expr="doc[" + q + "].empty", type=BOOLEAN, frum=self),
                type=c.type,
                expr="doc[" + q + "].values",
                frum=self,
                many=True
            ))

        if len(acc) == 0:
            return NullOp().to_painless(schema)
        elif len(acc) == 1:
            return acc[0]
        else:
            return CoalesceOp("coalesce", acc).to_painless(schema)


@extend(WhenOp)
def to_painless(self, schema):
    if self.simplified:
        when = self.when.to_painless(schema)
        then = self.then.to_painless(schema)
        els_ = self.els_.to_painless(schema)

        if isinstance(when, TrueOp):
            return then
        elif isinstance(when, FalseOp):
            return els_
        elif isinstance(then.miss, TrueOp):
            return Painless(
                miss=self.missing(),
                type=els_.type,
                expr=els_.expr,
                frum=self
            )
        elif isinstance(els_.miss, TrueOp):
            return Painless(
                miss=self.missing(),
                type=then.type,
                expr=then.expr,
                frum=self
            )

        elif then.type == els_.type:
            return Painless(
                miss=self.missing(),
                type=then.type,
                expr="(" + when.expr + ") ? (" + then.expr + ") : (" + els_.expr + ")",
                frum=self
            )
        else:
            Log.error("do not know how to handle")
    else:
        return self.partial_eval().to_painless(schema)


@extend(WhenOp)
def to_esfilter(self, schema):
    return OrOp("or", [
        AndOp("and", [self.when, self.then]),
        AndOp("and", [NotOp("not", self.when), self.els_])
    ]).partial_eval().to_esfilter(schema)


@extend(BasicIndexOfOp)
def to_painless(self, schema):
    v = StringOp("string", self.value).to_painless(schema).expr
    find = StringOp("string", self.find).to_painless(schema).expr
    start = IntegerOp("integer", self.start).to_painless(schema).expr

    return Painless(
        miss=FalseOp(),
        type=INTEGER,
        expr="(" + v + ").indexOf(" + find + ", " + start + ")",
        frum=self
    )


@extend(BasicIndexOfOp)
def to_esfilter(self, schema):
    return ScriptOp("", self.to_painless(schema)).to_esfilter(schema)


@extend(BasicSubstringOp)
def to_painless(self, schema):
    v = StringOp("string", self.value).partial_eval().to_painless(schema).expr
    start = IntegerOp("string", self.start).partial_eval().to_painless(schema).expr
    end = IntegerOp("integer", self.end).partial_eval().to_painless(schema).expr

    return Painless(
        miss=FalseOp(),
        type=STRING,
        expr="(" + v + ").substring(" + start + ", " + end + ")",
        frum=self
    )






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


def get_type(var_name):
    type_ = var_name.split(".$")[1:]
    if not type_:
        return "j"
    return json_type_to_painless_type.get(type_[0], "j")


json_type_to_painless_type = {
    "string": "s",
    "boolean": "b",
    "number": "n"
}
