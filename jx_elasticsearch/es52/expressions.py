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
from mo_dots import coalesce, wrap, Null, unwraplist, literal_field, set_default, Data, listwrap
from mo_json import json2value, quote
from mo_logs import Log, suppress_exception
from mo_math import OR, MAX
from pyLibrary.convert import json_decoder, string2regexp

from jx_base.expressions import Variable, DateOp, TupleOp, LeavesOp, BinaryOp, OrOp, ScriptOp, \
    WhenOp, InequalityOp, extend, RowsOp, Literal, NullOp, TrueOp, FalseOp, DivOp, FloorOp, \
    EqOp, NeOp, NotOp, LengthOp, NumberOp, StringOp, CountOp, MultiOp, RegExpOp, CoalesceOp, MissingOp, ExistsOp, \
    PrefixOp, UnixOp, NotLeftOp, RightOp, NotRightOp, FindOp, BetweenOp, InOp, RangeOp, CaseOp, AndOp, \
    ConcatOp, IsNumberOp, TRUE_FILTER, FALSE_FILTER, LeftOp, Expression, BasicIndexOfOp, MaxOp, MinOp, BasicEqOp, BooleanOp, IntegerOp


class Painless(Expression):
    __slots__ = ("missing", "b", "n", "s", "j", "many")

    def __init__(self, missing=None, b=None, i=None, n=None, s=None, j=None, many=False):
        self._missing = coalesce(missing, FalseOp())  # Expression that will return true/false to indicate missing result
        self.b = b  # script that returns boolean
        self.i = i  # script that returns integer
        self.n = n  # script that returns number (double)
        self.s = s  # script that returns string
        self.j = j  # script that returns object
        self.many = many  # True if script returns multi-value

    @property
    def script(self):
        missing = self._missing.partial_eval()
        if isinstance(missing, FalseOp):
            return self.partial_eval().to_painless(schema).expression
        elif isinstance(missing, TrueOp):
            return "null"

        return "(" + missing.to_painless(not_null=True, boolean=True).b + ")?null:(" + self.expression + ")"

    @property
    def expression(self):
        return coalesce(self.j, self.b, self.n, self.s)  # ONLY ONE SHOULD EVER BE SET

    def to_esfilter(self):
        return {"script": {"script": {"lang": "painless", "inline": self.script}}}

    def to_painless(self, schema, not_null=False, boolean=False):
        if boolean:
            if self.b:
                return self
            else:
                return Painless(j="(" + self.expression + ")!=null")
        else:
            return self

    def missing(self):
        return self._missing

    def __data__(self):
        return {"script": self.script}


@extend(BetweenOp)
def to_painless(self, schema, not_null=False, boolean=False):
    start_index = CaseOp(
        "case",
        [
            WhenOp("when", self.prefix.missing(), **{"then": Literal(None, 0)}),
            WhenOp("when", IsNumberOp("is_number", self.prefix), **{"then": MaxOp("max", [Literal(None, 0), self.prefix])}),
            FindOp("find", [self.value, self.prefix], start=self.start)
        ]
    )
    len_prefix = CaseOp(
        "case",
        [
            WhenOp("when", self.prefix.missing(), **{"then": Literal(None, 0)}),
            WhenOp("when", IsNumberOp("is_number", self.prefix), **{"then": Literal(None, 0)}),
            LengthOp("length", self.prefix)
        ]
    )

    end_index = CaseOp(
        "case",
        [
            WhenOp("when", start_index.missing(), **{"then": NullOp()}),
            WhenOp("when", self.suffix.missing(), **{"then": LengthOp("length", self.value)}),
            WhenOp("when", IsNumberOp("is_number", self.suffix), **{"then": MinOp("min", [self.suffix, LengthOp("length", self.value)])}),
            FindOp("find", [self.value, self.suffix], start=MultiOp("add", [start_index, len_prefix]))
        ]
    )

    value = StringOp("string", self.value).partial_eval().to_painless(schema).s
    start = MultiOp("add", [start_index, len_prefix]).partial_eval().to_painless(schema).n
    end = end_index.partial_eval().to_painless(schema).n

    between = WhenOp(
        "when",
        end_index.missing(),
        **{
            "then": self.default,
            "else": Painless(s="(" + value + ").substring((" + start + ").intValue(), (" + end + ").intValue())")
        }
    )

    return between.partial_eval().to_painless(schema)


@extend(BetweenOp)
def missing(self):
    if isinstance(self.prefix, NullOp):
        prefix = Literal(None, 0)
    else:
        prefix = FindOp("find", [self.value, self.prefix])

    if isinstance(self.suffix, NullOp):
        expr = AndOp("and", [
            self.default.missing(),
            prefix.missing()
        ])
    else:
        expr = AndOp("and", [
            self.default.missing(),
            OrOp("or", [
                prefix.missing(),
                FindOp("find", [self.value, self.suffix], start=prefix).missing(),
            ])
        ])
    return expr.partial_eval()


@extend(BetweenOp)
def to_esfilter(self):
    if isinstance(self.value, Variable):
        return {"terms": {self.value.var: json2value(self.superset.json)}}
    else:
        return {"script": self.to_painless(schema)}


@extend(BinaryOp)
def to_painless(self, schema, not_null=False, boolean=False):
    lhs = NumberOp("number", self.lhs).partial_eval().to_painless(schema).n
    rhs = NumberOp("number", self.rhs).partial_eval().to_painless(schema).n
    script = "(" + lhs + ") " + BinaryOp.operators[self.op] + " (" + rhs + ")"
    missing = OrOp("or", [self.lhs.missing(), self.rhs.missing()])

    if not_null:
        if self.default.missing():
            output = Painless(n=script)
        else:
            output = WhenOp(
                "when",
                missing,
                **{
                    "then": self.default,
                    "else":
                        Painless(n=script)
                }
            ).partial_eval().to_painless(schema)
    else:
        output = WhenOp(
            "when",
            missing,
            **{
                "then": self.default,
                "else":
                    Painless(n=script)
            }
        ).partial_eval().to_painless(schema)
    return output


@extend(BinaryOp)
def to_esfilter(self):
    if not isinstance(self.lhs, Variable) or not isinstance(self.rhs, Literal) or self.op in BinaryOp.algebra_ops:
        return ScriptOp("script",  self.to_painless(schema)).to_esfilter()

    if self.op in ["eq", "term"]:
        return {"term": {self.lhs.var: self.rhs.to_esfilter()}}
    elif self.op in ["ne", "neq"]:
        return {"bool": {"must_not":{"term": {self.lhs.var: self.rhs.to_esfilter()}}}}
    elif self.op in BinaryOp.ineq_ops:
        return {"range": {self.lhs.var: {self.op: json2value(self.rhs.json)}}}
    else:
        Log.error("Logic error")


@extend(CaseOp)
def to_painless(self, schema, not_null=False, boolean=False):
    acc = self.whens[-1].partial_eval().to_painless(schema)
    for w in reversed(self.whens[0:-1]):
        acc = WhenOp(
            "when",
            w.when,
            **{"then": w.then, "else": acc}
        ).partial_eval().to_painless(schema)
    return acc


@extend(CaseOp)
def to_esfilter(self):
    return ScriptOp("script",  self.to_painless(schema)).to_esfilter()


@extend(CaseOp)
def missing(self):
    m = self.whens[-1].partial_eval().to_painless(schema)._missing
    for w in reversed(self.whens[0:-1]):
        when = w.when.partial_eval()
        if isinstance(when, FalseOp):
            pass
        elif isinstance(when, TrueOp):
            m = w.then.partial_eval().to_painless(schema)._missing
        else:
            m = OrOp("or", [AndOp("and", [when, w.then.missing()]), m])
    return m


@extend(ConcatOp)
def to_esfilter(self):
    if isinstance(self.value, Variable) and isinstance(self.find, Literal):
        return {"regexp": {self.value.var: ".*" + string2regexp(json2value(self.find.json)) + ".*"}}
    else:
        return ScriptOp("script",  self.to_painless(schema)).to_esfilter()


@extend(ConcatOp)
def to_painless(self, schema, not_null=False, boolean=False):
    if len(self.terms) == 0:
        return self.default.to_painless(schema)

    acc = []
    separator = StringOp("string", self.separator)
    sep = separator.to_painless(schema).s
    for t in self.terms:
        val = WhenOp(
            "when",
            t.missing(),
            **{
                "then": Literal("literal", ""),
                "else": Painless(s=sep + "+" + StringOp(None, t).to_painless(schema).s)
            }
        )
        acc.append("(" + val.partial_eval().to_painless(schema).s + ")")
    expr_ = "(" + "+".join(acc) + ").substring(" + LengthOp("length", separator).to_painless(schema).n + ")"

    if isinstance(self.default, NullOp):
        return Painless(
            missing=self.missing(),
            s=expr_
        )
    else:
        return Painless(
            missing=self.missing(),
            s="((" + expr_ + ").length==0) ? (" + self.default.to_painless(schema).expression + ") : (" + expr_ + ")"
        )


@extend(Literal)
def to_painless(self, schema, not_null=False, boolean=False):
    def _convert(v):
        if v is None:
            return NullOp().to_painless(schema)
        if v is True:
            return Painless(b="true")
        if v is False:
            return Painless(b="false")
        if isinstance(v, (text_type, binary_type)):
            return Painless(s=quote(v))
        if isinstance(v, (int, long, float)):
            return Painless(n=text_type(v))
        if isinstance(v, dict):
            return Painless(j="[" + ", ".join(quote(k) + ": " + _convert(vv) for k, vv in v.items()) + "]")
        if isinstance(v, list):
            return Painless(j="[" + ", ".join(_convert(vv).expression for vv in v) + "]")

    return _convert(self.value)


@extend(CoalesceOp)
def to_painless(self, schema, not_null=False, boolean=False):
    if not self.terms:
        return Painless(missing=TrueOp())
    acc = self.terms[-1].to_painless(schema).expression
    for v in reversed(self.terms[:-1]):
        r = v.to_painless(schema)
        if r.many:
            acc = "(" + r.expression + ").size()>0 ? (" + r.expression + ") : (" + acc + ")"
        else:
            acc = "((" + r.expression + ") != null) ? (" + r.expression + ") : (" + acc + ")"
    return Painless(
        missing=AndOp("and", [t.missing() for t in self.terms]),
        j=acc
    )


@extend(CoalesceOp)
def to_esfilter(self):
    return {"bool": {"should": [{"exists": {"field": v}} for v in self.terms]}}


@extend(ExistsOp)
def to_painless(self, schema, not_null=False, boolean=False):
    if isinstance(self.field, Variable):
        return "!doc[" + quote(self.field.var) + "].empty"
    elif isinstance(self.field, Literal):
        return self.field.exists().to_painless(schema)
    else:
        return self.field.to_painless(schema) + " != null"


@extend(ExistsOp)
def to_esfilter(self):
    if isinstance(self.field, Variable):
        return {"exists": {"field": self.field.var}}
    else:
        return ScriptOp("script",  self.to_painless(schema)).to_esfilter()


@extend(FindOp)
def to_painless(self, schema, not_null=False, boolean=False):
    v = StringOp("string", self.value).partial_eval().to_painless(schema)
    find = StringOp("string", self.find).partial_eval().to_painless(schema)
    start = MaxOp("max", [Literal(None, 0), self.start]).partial_eval().to_painless(schema)
    index = v.s + ".indexOf(" + find.s + ", (" + start.n + ").intValue())"

    return WhenOp(
        "when",
        Painless(b=index + "==-1"),
        **{"then": self.default, "else": Painless(n=index)}
    ).partial_eval().to_painless(schema)


@extend(FindOp)
def to_esfilter(self):
    if isinstance(self.value, Variable) and isinstance(self.find, Literal):
        return {"regexp": {self.value.var: ".*" + string2regexp(json2value(self.find.json)) + ".*"}}
    else:
        return ScriptOp("script",  self.to_painless(schema)).to_esfilter()


@extend(Literal)
def to_esfilter(self):
    return json2value(self.json)


@extend(NullOp)
def to_painless(self, schema, not_null=False, boolean=False):
    return Painless(
        missing=TrueOp(),
        b="null",
        s="null",
        n="null",
        j="null"
    )

@extend(NullOp)
def to_esfilter(self):
    return {"bool": {"must_not": {"match_all": {}}}}


@extend(FalseOp)
def to_painless(self, schema, not_null=False, boolean=False):
    return Painless(b="false")


@extend(FalseOp)
def to_esfilter(self):
    return {"bool": {"must_not": {"match_all": {}}}}


@extend(DateOp)
def to_esfilter(self):
    return json2value(self.json)


@extend(DateOp)
def to_painless(self, schema, not_null=False, boolean=False):
    Log.error("not supported")


@extend(TupleOp)
def to_esfilter(self):
    Log.error("not supported")


@extend(LeavesOp)
def to_painless(self, schema, not_null=False, boolean=False):
    Log.error("not supported")


@extend(LeavesOp)
def to_esfilter(self):
    Log.error("not supported")


@extend(InequalityOp)
def to_painless(self, schema, not_null=False, boolean=False):
    lhs = NumberOp("number", self.lhs).partial_eval().to_painless(schema).n
    rhs = NumberOp("number", self.rhs).partial_eval().to_painless(schema).n
    script = "(" + lhs + ") " + InequalityOp.operators[self.op] + " (" + rhs + ")"

    output = WhenOp(
        "when",
        OrOp("or", [self.lhs.missing(), self.rhs.missing()]),
        **{
            "then": FalseOp(),
            "else":
                Painless(b=script)
        }
    ).partial_eval().to_painless(schema)
    return output


@extend(InequalityOp)
def to_esfilter(self):
    if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
        return {"range": {self.lhs.var: {self.op: json2value(self.rhs.json)}}}
    else:
        return ScriptOp("script", self.to_painless(boolean=True).script).to_esfilter()


@extend(DivOp)
def to_painless(self, schema, not_null=False, boolean=False):
    lhs = self.lhs.partial_eval()
    rhs = self.rhs.partial_eval()
    script = "Double.valueOf((double)(" + lhs.to_painless(schema).n + ") / (double)(" + rhs.to_painless(schema).n + "))"

    output = WhenOp(
        "when",
        OrOp("or", [lhs.missing(), rhs.missing(), EqOp("eq", [rhs, Literal("literal", 0)])]),
        **{
            "then": self.default,
            "else": Painless(n=script)
        }
    ).partial_eval().to_painless(schema)

    return output


@extend(DivOp)
def to_esfilter(self):
    if not isinstance(self.lhs, Variable) or not isinstance(self.rhs, Literal):
        return ScriptOp("script", self.to_painless(schema)).to_esfilter()
    else:
        Log.error("Logic error")


@extend(FloorOp)
def to_painless(self, schema, not_null=False, boolean=False):
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
def to_esfilter(self):
    Log.error("Logic error")

@extend(EqOp)
def to_painless(self, schema, not_null=False, boolean=False):
    lhs = self.lhs.partial_eval().to_painless(schema)
    rhs = self.rhs.partial_eval().to_painless(schema)

    if lhs.many:
        if rhs.many:
            return Painless(
                b=WhenOp(
                    "when",
                    self.lhs.missing(),
                    **{
                        "then": self.rhs.missing(),
                        "else": AndOp("and", [
                            Painless(b="("+lhs.expression+").size()==("+rhs.expression+").size()"),
                            Painless(b="("+rhs.expression+").containsAll("+lhs.expression+")")
                        ])
                    }
                ).partial_eval().to_painless(boolean=True).b
            )
        return Painless(
            b=WhenOp(
                "when",
                self.lhs.missing(),
                **{
                    "then": self.rhs.missing(),
                    "else": Painless(b="("+lhs.expression+").contains("+rhs.expression+")")
                }
            ).partial_eval().to_painless(boolean=True).expression
        )
    elif rhs.many:
        return Painless(
            b=WhenOp(
                "when",
                self.lhs.missing(),
                **{
                    "then": self.rhs.missing(),
                    "else": Painless(b="("+rhs.expression+").contains("+lhs.expression+")")
                }
            ).partial_eval().to_painless(boolean=True).expression
        )
    else:
        return Painless(b="(" + lhs.expression + "==" + rhs.expression + ")")


@extend(EqOp)
def to_esfilter(self):
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
        return self.to_painless(schema).to_esfilter()


@extend(BasicEqOp)
def to_painless(self, schema, not_null=False, boolean=False):
    lhs = self.lhs.partial_eval().to_painless(schema)
    rhs = self.rhs.partial_eval().to_painless(schema)

    if lhs.many:
        if rhs.many:
            return AndOp("and", [
                Painless(b="(" + lhs.expression + ").size()==(" + rhs.expression + ").size()"),
                Painless(b="(" + rhs.expression + ").containsAll(" + lhs.expression + ")")
            ]).to_painless(boolean=True)
        else:
            return Painless(b="("+lhs.expression+").contains("+rhs.expression+")")
    elif rhs.many:
        return Painless(b="("+rhs.expression+").contains("+lhs.expression+")")
    else:
        return Painless(b="(" + lhs.expression + "==" + rhs.expression + ")")


@extend(BasicEqOp)
def to_esfilter(self, not_null=False, boolean=False):
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
        return self.to_painless(schema).to_esfilter()



@extend(MissingOp)
def to_painless(self, schema, not_null=False, boolean=True):
    if isinstance(self.expr, Variable):
        if self.expr.var == "_id":
            return Painless(b="false")
        else:
            return Painless(b="doc[" + quote(self.expr.var) + "].empty")
    elif isinstance(self.expr, Literal):
        return self.expr.missing().to_painless(schema)
    else:
        return self.expr.missing().to_painless(schema)


@extend(MissingOp)
def to_esfilter(self):
    if isinstance(self.expr, Variable):
        return {"bool": {"must_not": {"exists": {"field": self.expr.var}}}}
    else:
        return ScriptOp("script", self.to_painless(schema)).to_esfilter()


@extend(NotLeftOp)
def to_painless(self, schema, not_null=False, boolean=False):
    v = StringOp("string", self.value).partial_eval().to_painless(schema).s
    l = NumberOp("number", self.length).partial_eval().to_painless(schema).n

    expr = "(" + v + ").substring((int)Math.max(0, (int)Math.min(" + v + ".length(), " + l + ")))"
    return Painless(
        missing=OrOp("or", [self.value.missing(), self.length.missing()]),
        s=expr
    )


@extend(NeOp)
def to_painless(self, schema, not_null=False, boolean=False):
    return NotOp("not", EqOp("eq", [self.lhs, self.rhs])).partial_eval().to_painless(schema)


@extend(NeOp)
def to_esfilter(self):
    if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
        return {"bool": {"must_not": {"term": {self.lhs.var: self.rhs.to_esfilter()}}}}
    else:

        calc = self.to_painless(schema)

        return {"bool": {"must": [
            # TODO: MAKE TESTS TO SEE IF THIS LOGIC IS CORRECT
            {"bool": {"must": [{"exists": {"field": v}} for v in self.vars()]}},
            ScriptOp("script", self.to_painless(schema)).to_esfilter()
        ]}}


@extend(NotOp)
def to_painless(self, schema, not_null=False, boolean=False):
    return Painless(b="!(" + self.term.to_painless(boolean=True).b + ")")


@extend(NotOp)
def to_esfilter(self):
    operand = self.term.to_esfilter()
    return {"bool": {"must_not": operand}}


@extend(AndOp)
def to_painless(self, schema, not_null=False, boolean=False):
    if not self.terms:
        return Painless(b="true")
    else:
        return Painless(b=" && ".join("(" + t.to_painless(boolean=True).b + ")" for t in self.terms))


@extend(AndOp)
def to_esfilter(self):
    if not len(self.terms):
        return {"match_all": {}}
    else:
        return {"bool": {"must": [t.to_esfilter() for t in self.terms]}}


@extend(OrOp)
def to_painless(self, schema, not_null=False, boolean=False):
    return Painless(b=" || ".join("(" + t.to_painless(boolean=True).b + ")" for t in self.terms if t))


@extend(OrOp)
def to_esfilter(self):
    return {"bool": {"should": [t.to_esfilter() for t in self.terms]}}


@extend(LengthOp)
def to_painless(self, schema, not_null=False, boolean=False):
    value = StringOp("string", self.term).to_painless(schema)
    if not_null:
        return Painless(n="(" + value.s + ").length()")

    missing = self.term.missing().partial_eval()
    return Painless(
        missing=missing,
        n="(" + value.s + ").length()"
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
    if value.b:
        return Painless(missing=value.missing, i=value.b+" ? 1 : 0")
    elif value.i:
        return value
    elif value.n:
        return Painless(missing=value.missing, i="(int)("+value.n+")")
    elif value.s:
        return Painless(missing=value.missing, n="Integer.parseInt(" + value.s + ")")
    elif value.many:
        return Painless(
            n="((" + value.j + ")[0] instanceof String) ? Integer.parseInt((" + value.j + ")[0]) : (int)(" + value.j + ")[0]"
        )
    else:
        return Painless(
            n="((" + value.j + ") instanceof String) ? Integer.parseInt(" + value.j + ") : (int)(" + value.j + ")"
        )

@extend(NumberOp)
def to_painless(self, schema, not_null=False, boolean=False):
    acc = []

    value = self.term.to_painless(schema)
    if value.b:
        acc.append(value.b+" ? 1 : 0")
    if value.i:
        acc.append(value.i)
    if value.n:
        acc.append(value.n)
    if value.s:
        acc.append("Double.parseDouble(" + value.s + ")")
    if value.j:
        acc.append("((" + value.j + ") instanceof String) ? Double.parseDouble(" + value.j + ") : (" + value.j + ")")

    if len(acc)==0:
        return Painless(missing=TrueOp())
    elif len(acc)==1:
        return Painless(missing.value._missing, n=acc[0])
    else:
        return Painless(
            missing.value._missing,

    output = self.to_painless(schema)
    output._missing = self.term.missing().partial_eval()
    return output

@extend(IsNumberOp)
def to_painless(self, schema, not_null=False, boolean=False):
    value = self.term.to_painless(schema)
    if value.n or value.i:
        return TrueOp().to_painless(schema)
    else:
        return Painless(
            b="(" + value.expression + ") instanceof java.lang.Double"
        )

@extend(IsNumberOp)
def partial_eval(self):
    if isinstance(self.term, Literal):
        if isinstance(self.term.value, (int, float)):
            return TrueOp()
        else:
            return FalseOp()
    else:
        value = self.term.to_painless(schema)
        if value.n:
            return TrueOp()
        elif value.s or value.b:
            return FalseOp()
        else:
            return Painless(
                b="(" + value.j + ") instanceof java.lang.Double"
            )


@extend(CountOp)
def to_painless(self, schema, not_null=False, boolean=False):
    return Painless(n="+".join("((" + t.missing().partial_eval().to_painless(boolean=True).b + ") ? 0 : 1)" for t in self.terms))


@extend(LengthOp)
def to_esfilter(self):
    return {"regexp": {self.var.var: json2value(self.pattern.json)}}


@extend(MaxOp)
def to_painless(self, schema, not_null=False, boolean=False):
    acc = NumberOp("number", self.terms[-1]).partial_eval().to_painless(schema).n
    for t in reversed(self.terms[0:-1]):
        acc = "Math.max(" + NumberOp("number", t).partial_eval().to_painless(schema).n + " , " + acc + ")"
    return Painless(
        missing=AndOp("or", [t.missing() for t in self.terms]),
        n=acc
    )


@extend(MinOp)
def to_painless(self, schema, not_null=False, boolean=False):
    acc = NumberOp("number", self.terms[-1]).partial_eval().to_painless(schema).n
    for t in reversed(self.terms[0:-1]):
        acc = "Math.min(" + NumberOp("number", t).partial_eval().to_painless(schema).n + " , " + acc + ")"
    return Painless(
        missing=AndOp("or", [t.missing() for t in self.terms]),
        n=acc
    )


@extend(MultiOp)
def to_painless(self, schema, not_null=False, boolean=False):
    op, unit = MultiOp.operators[self.op]
    if self.nulls:
        calc = op.join(
            "((" + t.missing().to_painless(boolean=True).b + ") ? " + unit + " : (" + NumberOp("number", t).partial_eval().to_painless(schema).n + "))" for
            t in self.terms
        )
        return WhenOp(
            "when",
            AndOp("and", [t.missing() for t in self.terms]),
            **{"then": self.default, "else": Painless(n=calc)}
        ).partial_eval().to_painless(schema)
    else:
        calc = op.join(
            "(" + NumberOp("number", t).to_painless(schema).n + ")"
            for t in self.terms
        )
        return WhenOp(
            "when",
            OrOp("or", [t.missing() for t in self.terms]),
            **{"then": self.default, "else": Painless(n=calc)}
        ).partial_eval().to_painless(not_null=not_null)


@extend(RegExpOp)
def to_esfilter(self):
    return {"regexp": {self.var.var: json2value(self.pattern.json)}}


@extend(StringOp)
def to_painless(self, schema, not_null=False, boolean=False):
    value = self.term.to_painless(schema)
    if not_null:
        if value.b:
            return Painless(s=value.b + ' ? "T" : "F"')
        elif value.n:
            return Painless(s="(" + value.n + " == (int)" + value.n + ")?String.valueOf((int)" + value.n + "):String.valueOf(" + value.n + ")")
        elif value.s:
            return value
        elif value.many:
            return Painless(
                s="((" + value.j + ")[0] instanceof java.lang.Double) ? String.valueOf((" + value.j + ")[0]).replaceAll('\\\\.0$', '') : String.valueOf((" + value.j + ")[0])"
            )
        else:
            return Painless(
                s="((" + value.j + ") instanceof java.lang.Double) ? String.valueOf(" + value.j + ").replaceAll('\\\\.0$', '') : String.valueOf(" + value.j + ")"
            )
    else:
        output = StringOp.to_painless(self, schema)
        output._missing=self.term.missing().partial_eval()
        return output

@extend(TrueOp)
def to_painless(self, schema, not_null=False, boolean=False):
    return Painless(b="true")


@extend(TrueOp)
def to_esfilter(self):
    return {"match_all": {}}


@extend(PrefixOp)
def to_painless(self, schema, not_null=False, boolean=False):
    return "(" + self.field.to_painless(schema) + ").startsWith(" + self.prefix.to_painless(schema) + ")"


@extend(PrefixOp)
def to_esfilter(self):
    if isinstance(self.field, Variable) and isinstance(self.prefix, Literal):
        return {"prefix": {self.field.var: json2value(self.prefix.json)}}
    else:
        return ScriptOp("script",  self.to_painless(schema)).to_esfilter()


@extend(RightOp)
def to_painless(self, schema, not_null=False, boolean=False):
    v = StringOp("string", self.value).partial_eval().to_painless(schema).s
    l = NumberOp("number", self.length).partial_eval().to_painless(schema).n

    expr = "(" + v + ").substring((int)Math.min(" + v + ".length(), (int)Math.max(0, (" + v + ").length() - (" + l + "))))"
    return Painless(
        missing=OrOp("or", [self.value.missing(), self.length.missing()]),
        s=expr
    )


@extend(NotRightOp)
def to_painless(self, schema, not_null=False, boolean=False):
    v = StringOp("string", self.value).partial_eval().to_painless(schema).s
    l = NumberOp("number", self.length).partial_eval().to_painless(schema).n

    expr = "(" + v + ").substring(0, (int)Math.min(" + v + ".length(), (int)Math.max(0, (" + v + ").length() - (" + l + "))))"
    return Painless(
        missing=OrOp("or", [self.value.missing(), self.length.missing()]),
        s=expr
    )


@extend(InOp)
def to_painless(self, schema, not_null=False, boolean=False):
    return Painless(b="(" + self.superset.to_painless(schema).expression + ").contains(" + self.value.to_painless(schema).expression + ")")


@extend(InOp)
def to_esfilter(self):
    if isinstance(self.value, Variable):
        return {"terms": {self.value.var: json2value(self.superset.json)}}
    else:
        return {"script": {"script": {"lang": "painless", "inline": self.to_painless(schema).script}}}



@extend(RangeOp)
def to_painless(self, schema, not_null=False, boolean=False):
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
def to_painless(self, schema, not_null=False, boolean=False):
    v = StringOp("string", self.value).partial_eval().to_painless(schema).s
    l = NumberOp("number", self.length).partial_eval().to_painless(schema).n

    expr = "(" + v + ").substring(0, (int)Math.max(0, (int)Math.min((" + v + ").length(), " + l + ")))"
    return Painless(
        missing=OrOp("or", [self.value.missing(), self.length.missing()]),
        s=expr
    )


@extend(ScriptOp)
def to_painless(self, schema, not_null=False, boolean=False):
    return Painless(j=self.script)


@extend(ScriptOp)
def to_esfilter(self):
    return {"script": {"script": {"lang": "painless", "inline": self.script}}}


@extend(ScriptOp)
def missing(self):
    return Painless(b="(" + self.script.to_painless(schema).expression + ")==null")


@extend(Variable)
def to_painless(self, schema, not_null=False, boolean=False):
    if self.var == ".":
        return "_source"
    else:
        if self.var == "_id":
            return Painless(s='doc["_uid"].value.substring(doc["_uid"].value.indexOf(\'#\')+1)')

        vars = listwrap(self.var)
        if len(vars) > 1:
            Log.error("can not handle multi-type yet")

        q = quote(vars[0])
        variable_type = get_type(vars[0])

        if not_null:
            if boolean:
                if variable_type=="b":
                    return Painless(b="doc[" + q + "].value")
                else:
                    return Painless(b="!doc[" + q + "].empty")
            else:
                return Painless(
                    **{variable_type:"doc[" + q + "].value"}
                )
        else:
            if boolean:
                if variable_type=="b":
                    return Painless(
                        missing=Painless(b="doc[" + q + "].empty"),
                        b="doc[" + q + "].value"
                    )
                else:
                    return Painless(
                        missing=Painless(b="doc[" + q + "].empty"),
                        b="!doc[" + q + "].empty"
                    )
            else:
                return Painless(
                    missing=Painless(b="doc[" + q + "].empty"),
                    many=True,
                    **{variable_type: "doc[" + q + "].values"}
                )


@extend(WhenOp)
def to_painless(self, schema, not_null=False, boolean=False):
    if self.simplified:
        when = self.when.to_painless(boolean=True)
        then = self.then.to_painless(schema)
        els_ = self.els_.to_painless(schema)

        if isinstance(then._missing, TrueOp):
            output = els_
        elif isinstance(els_._missing, TrueOp):
            output = then
        else:
            for t in "bnsj":
                if getattr(then, t) and getattr(els_, t):
                    output = Painless(
                        **{t: "(" + when.b + ") ? (" + getattr(then, t) + ") : (" + getattr(els_, t) + ")"}
                    )
                    break
            else:
                Log.error("do not know how to handle")
        if not not_null:
            output._missing = self.missing().partial_eval()
        return output
    else:
        return self.partial_eval().to_painless(schema)


@extend(WhenOp)
def to_esfilter(self):
    return OrOp("or", [
        AndOp("and", [self.when, self.then]),
        AndOp("and", [NotOp("not", self.when), self.els_])
    ]).partial_eval().to_esfilter()


@extend(BasicIndexOfOp)
def to_painless(self, schema, not_null=False, boolean=False):
    v = StringOp("string", self.value).to_painless(schema).s
    find = StringOp("string", self.find).to_painless(schema).s
    start = NumberOp("number", self.start).to_painless(schema).n

    return Painless(n="(" + v + ").indexOf(" + find + ", " + start + ")")


@extend(BasicIndexOfOp)
def to_esFilter(self):
    return ScriptOp("", self.to_painless(schema)).to_esfilter()







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
