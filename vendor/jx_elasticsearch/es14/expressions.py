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

from jx_base.expressions import Variable, TupleOp, LeavesOp, BinaryOp, OrOp, ScriptOp, \
    WhenOp, InequalityOp, extend, Literal, NullOp, TrueOp, FalseOp, DivOp, FloorOp, \
    EqOp, NeOp, NotOp, LengthOp, NumberOp, StringOp, CountOp, MultiOp, RegExpOp, CoalesceOp, MissingOp, ExistsOp, \
    PrefixOp, NotLeftOp, InOp, CaseOp, AndOp, \
    ConcatOp, IsNumberOp, Expression, BasicIndexOfOp, MaxOp, MinOp, BasicEqOp, BooleanOp, IntegerOp, BasicSubstringOp, ZERO, NULL, FirstOp, FALSE, TRUE, SuffixOp, simplified, ONE
from jx_elasticsearch.es14.util import es_not, es_script, es_or, es_and, es_missing
from mo_dots import coalesce, wrap, Null, set_default, literal_field
from mo_future import text_type
from mo_json import NUMBER, STRING, BOOLEAN, OBJECT, INTEGER
from mo_logs import Log, suppress_exception
from mo_logs.strings import expand_template, quote
from mo_math import MAX, OR
from pyLibrary.convert import string2regexp

TO_STRING = """
    ({it ->
        value = {{expr}};
        if (value==null) return "";
        output = String.valueOf(value);
        if (output.endsWith(".0")) output = output.substring(0, output.length() - 2);
        return output;
    })()
"""


LIST_TO_PIPE = """
StringBuffer output=new StringBuffer();
for(String s : {{expr}}){
    output.append("|");
    String sep2="";
    StringTokenizer parts = new StringTokenizer(s, "|");
    while (parts.hasMoreTokens()){
        output.append(sep2);
        output.append(parts.nextToken());
        sep2="||";
    }//for
}//for
output.append("|");
return output.toString()
"""


class EsScript(Expression):
    __slots__ = ("miss", "data_type", "expr", "many")

    def __init__(self, type, expr, frum, miss=None, many=False):
        self.miss = coalesce(miss, FALSE)  # Expression that will return true/false to indicate missing result
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
        if missing is FALSE:
            return self.partial_eval().to_es_script(schema).expr
        elif missing is TRUE:
            return "null"

        return "(" + missing.to_es_script(schema).expr + ")?null:(" + self.expr + ")"

    def to_esfilter(self, schema):
        return {"script": es_script(self.script(schema))}

    def to_es_script(self, schema):
        return self

    def missing(self):
        return self.miss

    def __data__(self):
        return {"script": self.script}

    def __eq__(self, other):
        if not isinstance(other, EsScript):
            return False
        elif self.expr==other.expr:
            return True
        else:
            return False


@extend(BinaryOp)
def to_es_script(self, schema):
    lhs = NumberOp("number", self.lhs).partial_eval().to_es_script(schema).expr
    rhs = NumberOp("number", self.rhs).partial_eval().to_es_script(schema).expr
    script = "(" + lhs + ") " + BinaryOp.operators[self.op] + " (" + rhs + ")"
    missing = OrOp("or", [self.lhs.missing(), self.rhs.missing()])

    return WhenOp(
        "when",
        missing,
        **{
            "then": self.default,
            "else":
                EsScript(type=NUMBER, expr=script, frum=self)
        }
    ).partial_eval().to_es_script(schema)


@extend(BinaryOp)
def to_esfilter(self, schema):
    if not isinstance(self.lhs, Variable) or not isinstance(self.rhs, Literal) or self.op in BinaryOp.operators:
        return self.to_es_script(schema).to_esfilter(schema)

    if self.op in ["eq", "term"]:
        return {"term": {self.lhs.var: self.rhs.to_esfilter(schema)}}
    elif self.op in ["ne", "neq"]:
        return es_not({"term": {self.lhs.var: self.rhs.to_esfilter(schema)}})
    elif self.op in BinaryOp.ineq_ops:
        return {"range": {self.lhs.var: {self.op: self.rhs.value}}}
    else:
        Log.error("Logic error")


@extend(CaseOp)
def to_es_script(self, schema):
    acc = self.whens[-1].partial_eval().to_es_script(schema)
    for w in reversed(self.whens[0:-1]):
        acc = WhenOp(
            "when",
            w.when,
            **{"then": w.then, "else": acc}
        ).partial_eval().to_es_script(schema)
    return acc


@extend(CaseOp)
def to_esfilter(self, schema):
    if self.type == BOOLEAN:
        return OrOp(
            "or",
            [
                AndOp("and", [w.when, w.then])
                for w in self.whens[:-1]
            ] +
            self.whens[-1:]
        ).partial_eval().to_esfilter(schema)
    else:
        Log.error("do not know how to handle")
        return ScriptOp("script", self.to_es_script(schema).script(schema)).to_esfilter(schema)


@extend(ConcatOp)
def to_esfilter(self, schema):
    if isinstance(self.value, Variable) and isinstance(self.find, Literal):
        return {"regexp": {self.value.var: ".*" + string2regexp(self.find.value) + ".*"}}
    else:
        return ScriptOp("script",  self.to_es_script(schema).script(schema)).to_esfilter(schema)


@extend(ConcatOp)
def to_es_script(self, schema):
    if len(self.terms) == 0:
        return self.default.to_es_script(schema)

    acc = []
    separator = StringOp("string", self.separator).partial_eval()
    sep = separator.to_es_script(schema).expr
    for t in self.terms:
        val = WhenOp(
            "when",
            t.missing(),
            **{
                "then": Literal("literal", ""),
                "else": EsScript(type=STRING, expr=sep + "+" + StringOp(None, t).partial_eval().to_es_script(schema).expr, frum=t)
                # "else": ConcatOp("concat", [sep, t])
            }
        )
        acc.append("(" + val.partial_eval().to_es_script(schema).expr + ")")
    expr_ = "(" + "+".join(acc) + ").substring(" + LengthOp("length", separator).to_es_script(schema).expr + ")"

    if isinstance(self.default, NullOp):
        return EsScript(
            miss=self.missing(),
            type=STRING,
            expr=expr_,
            frum=self
        )
    else:
        return EsScript(
            miss=self.missing(),
            type=STRING,
            expr="((" + expr_ + ").length==0) ? (" + self.default.to_es_script(schema).expr + ") : (" + expr_ + ")",
            frum=self
        )


@extend(Literal)
def to_es_script(self, schema):
    def _convert(v):
        if v is None:
            return NULL.to_es_script(schema)
        if v is True:
            return EsScript(
                type=BOOLEAN,
                expr="true",
                frum=self
            )
        if v is False:
            return EsScript(
                type=BOOLEAN,
                expr="false",
                frum=self
            )
        if isinstance(v, text_type):
            return EsScript(
                type=STRING,
                expr=quote(v),
                frum=self
            )
        if isinstance(v, int):
            return EsScript(
                type=INTEGER,
                expr=text_type(v),
                frum=self
            )
        if isinstance(v, float):
            return EsScript(
                type=NUMBER,
                expr=text_type(v),
                frum=self
            )
        if isinstance(v, dict):
            return EsScript(
                type=OBJECT,
                expr="[" + ", ".join(quote(k) + ": " + _convert(vv) for k, vv in v.items()) + "]",
                frum=self
            )
        if isinstance(v, (list, tuple)):
            return EsScript(
                type=OBJECT,
                expr="[" + ", ".join(_convert(vv).expr for vv in v) + "]",
                frum=self
            )

    return _convert(self.term)


@extend(CoalesceOp)
def to_es_script(self, schema):
    if not self.terms:
        return NULL.to_es_script(schema)

    v = self.terms[-1]
    acc = FirstOp("first", v).partial_eval().to_es_script(schema)
    for v in reversed(self.terms[:-1]):
        m = v.missing().partial_eval()
        e = NotOp("not", m).partial_eval().to_es_script(schema)
        r = FirstOp("first", v).partial_eval().to_es_script(schema)

        if r.miss is TRUE:
            continue
        elif r.miss is FALSE:
            acc = r
            continue
        elif acc.type == r.type:
            new_type = r.type
        elif acc.type == NUMBER and r.type == INTEGER:
            new_type = NUMBER
        elif acc.type == INTEGER and r.type == NUMBER:
            new_type = NUMBER
        else:
            new_type = OBJECT

        acc = EsScript(
            miss=AndOp("and", [acc.miss, m]).partial_eval(),
            type=new_type,
            expr="(" + e.expr + ") ? (" + r.expr + ") : (" + acc.expr + ")",
            frum=self
        )
    return acc


@extend(CoalesceOp)
def to_esfilter(self, schema):
    return {"bool": {"should": [{"exists": {"field": v}} for v in self.terms]}}


@extend(ExistsOp)
def to_es_script(self, schema):
    return self.field.exists().partial_eval().to_es_script(schema)


@extend(ExistsOp)
def to_esfilter(self, schema):
    return self.field.exists().partial_eval().to_esfilter(schema)


@extend(Literal)
def to_esfilter(self, schema):
    return self.json


@extend(NullOp)
def to_es_script(self, schema):
    return EsScript(
        miss=TRUE,
        type=OBJECT,
        expr="null",
        frum=self
    )

@extend(NullOp)
def to_esfilter(self, schema):
    return es_not({"match_all": {}})


@extend(FalseOp)
def to_es_script(self, schema):
    return EsScript(type=BOOLEAN, expr="false", frum=self)


@extend(FalseOp)
def to_esfilter(self, schema):
    return MATCH_NONE


@extend(TupleOp)
def to_esfilter(self, schema):
    Log.error("not supported")


@extend(TupleOp)
def to_es_script(self, schema):
    terms = [FirstOp("first", t).partial_eval().to_es_script(schema) for t in self.terms]
    expr = 'new Object[]{'+','.join(t.expr for t in terms)+'}'
    return EsScript(
        type=OBJECT,
        expr=expr,
        miss=FALSE,
        many=FALSE,
        frum=self
    )


@extend(LeavesOp)
def to_es_script(self, schema):
    Log.error("not supported")


@extend(LeavesOp)
def to_esfilter(self, schema):
    Log.error("not supported")


@extend(InequalityOp)
def to_es_script(self, schema):
    lhs = NumberOp("number", self.lhs).partial_eval().to_es_script(schema).expr
    rhs = NumberOp("number", self.rhs).partial_eval().to_es_script(schema).expr
    script = "(" + lhs + ") " + InequalityOp.operators[self.op] + " (" + rhs + ")"

    output = WhenOp(
        "when",
        OrOp("or", [self.lhs.missing(), self.rhs.missing()]),
        **{
            "then": FALSE,
            "else":
                EsScript(type=BOOLEAN, expr=script, frum=self)
        }
    ).partial_eval().to_es_script(schema)
    return output


@extend(InequalityOp)
def to_esfilter(self, schema):
    if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
        cols = schema.leaves(self.lhs.var)
        if not cols:
            lhs = self.lhs.var  # HAPPENS DURING DEBUGGING, AND MAYBE IN REAL LIFE TOO
        elif len(cols) == 1:
            lhs = schema.leaves(self.lhs.var)[0].es_column
        else:
            Log.error("operator {{op|quote}} does not work on objects", op=self.op)
        return {"range": {lhs: {self.op: self.rhs.value}}}
    else:
        script = self.to_es_script(schema)
        if script.miss is not FALSE:
            Log.error("inequality must be decisive")
        return {"script": es_script(script.expr)}


@extend(DivOp)
def to_es_script(self, schema):
    lhs = NumberOp("number", self.lhs).partial_eval()
    rhs = NumberOp("number", self.rhs).partial_eval()
    script = "(" + lhs.to_es_script(schema).expr + ") / (" + rhs.to_es_script(schema).expr + ")"

    output = WhenOp(
        "when",
        OrOp("or", [self.lhs.missing(), self.rhs.missing(), EqOp("eq", [self.rhs, ZERO])]),
        **{
            "then": self.default,
            "else": EsScript(type=NUMBER, expr=script, frum=self)
        }
    ).partial_eval().to_es_script(schema)

    return output


@extend(DivOp)
def to_esfilter(self, schema):
    return NotOp("not", self.missing()).partial_eval().to_esfilter(schema)


@extend(FloorOp)
def to_es_script(self, schema):
    lhs = self.lhs.partial_eval().to_es_script(schema)
    rhs = self.rhs.partial_eval().to_es_script(schema)

    if rhs.frum is ONE:
        script = "(int)Math.floor(" + lhs.expr + ")"
    else:
        script = "Math.floor((" + lhs.expr + ") / (" + rhs.expr + "))*(" + rhs.expr + ")"

    output = WhenOp(
        "when",
        OrOp("or", [lhs.miss, rhs.miss, EqOp("eq", [self.rhs, ZERO])]),
        **{
            "then": self.default,
            "else":
                EsScript(
                    type=NUMBER,
                    expr=script,
                    frum=self,
                    miss=FALSE
                )
        }
    ).to_es_script(schema)
    return output


@extend(FloorOp)
def to_esfilter(self, schema):
    Log.error("Logic error")


@simplified
@extend(EqOp)
def partial_eval(self):
    lhs = self.lhs.partial_eval()
    rhs = self.rhs.partial_eval()
    return EqOp("eq", [lhs, rhs])


@extend(EqOp)
def to_es_script(self, schema):
    return CaseOp("case", [
        WhenOp("when", self.lhs.missing(), **{"then": self.rhs.missing()}),
        WhenOp("when", self.rhs.missing(), **{"then": FALSE}),
        BasicEqOp("eq", [self.lhs, self.rhs])
    ]).partial_eval().to_es_script(schema)


@extend(EqOp)
def to_esfilter(self, schema):
    if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
        rhs = self.rhs.value
        lhs = self.lhs.var
        cols = schema.leaves(lhs)
        if cols:
            lhs = cols[0].es_column

        if isinstance(rhs, list):
            if len(rhs) == 1:
                return {"term": {lhs: rhs[0]}}
            else:
                return {"terms": {lhs: rhs}}
        else:
            return {"term": {lhs: rhs}}

    else:
        return CaseOp("case", [
            WhenOp("when", self.lhs.missing(), **{"then": self.rhs.missing()}),
            WhenOp("when", self.rhs.missing(), **{"then": FALSE}),
            BasicEqOp("eq", [self.lhs, self.rhs])
        ]).partial_eval().to_esfilter(schema)


@extend(BasicEqOp)
def to_es_script(self, schema):
    lhs = self.lhs.partial_eval().to_es_script(schema)
    rhs = self.rhs.partial_eval().to_es_script(schema)

    if lhs.many:
        if rhs.many:
            return AndOp("and", [
                EsScript(type=BOOLEAN, expr="(" + lhs.expr + ").size()==(" + rhs.expr + ").size()", frum=self),
                EsScript(type=BOOLEAN, expr="(" + rhs.expr + ").containsAll(" + lhs.expr + ")", frum=self)
            ]).to_es_script(schema)
        else:
            return EsScript(type=BOOLEAN, expr="(" + lhs.expr + ").contains(" + rhs.expr + ")", frum=self)
    elif rhs.many:
        return EsScript(
            type=BOOLEAN,
            expr="(" + rhs.expr + ").contains(" + lhs.expr + ")",
            frum=self
        )
    else:
        return EsScript(
            type=BOOLEAN,
            expr="(" + lhs.expr + "==" + rhs.expr + ")",
            frum=self
        )


@extend(BasicEqOp)
def to_esfilter(self, schema):
    if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
        lhs = self.lhs.var
        cols = schema.leaves(lhs)
        if cols:
            lhs = cols[0].es_column
        rhs = self.rhs.value
        if isinstance(rhs, list):
            if len(rhs) == 1:
                return {"term": {lhs: rhs[0]}}
            else:
                return {"terms": {lhs: rhs}}
        else:
            return {"term": {lhs: rhs}}
    else:
        return self.to_es_script(schema).to_esfilter(schema)



@extend(MissingOp)
def to_es_script(self, schema, not_null=False, boolean=True):
    if isinstance(self.expr, Variable):
        if self.expr.var == "_id":
            return EsScript(type=BOOLEAN, expr="false", frum=self)
        else:
            columns = schema.leaves(self.expr.var)
            if len(columns) == 1:
                return EsScript(type=BOOLEAN, expr="doc[" + quote(columns[0].es_column) + "].isEmpty()", frum=self)
            else:
                return AndOp("and", [
                    EsScript(
                        type=BOOLEAN,
                        expr="doc[" + quote(c.es_column) + "].isEmpty()",
                        frum=self
                    )
                    for c in columns
                ]).partial_eval().to_es_script(schema)
    elif isinstance(self.expr, Literal):
        return self.expr.missing().to_es_script(schema)
    else:
        return self.expr.missing().partial_eval().to_es_script(schema)


@extend(MissingOp)
def to_esfilter(self, schema):
    if isinstance(self.expr, Variable):
        cols = schema.leaves(self.expr.var)
        if not cols:
            return {"match_all": {}}
        elif len(cols) == 1:
            return es_missing(cols[0].es_column)
        else:
            return es_and([
                es_missing(c.es_column) for c in cols
            ])
    else:
        return ScriptOp("script", self.to_es_script(schema).script(schema)).to_esfilter(schema)


@extend(NotLeftOp)
def to_es_script(self, schema):
    v = StringOp("string", self.value).partial_eval().to_es_script(schema).expr
    l = NumberOp("number", self.length).partial_eval().to_es_script(schema).expr

    expr = "(" + v + ").substring((int)Math.max(0, (int)Math.min(" + v + ".length(), " + l + ")))"
    return EsScript(
        miss=OrOp("or", [self.value.missing(), self.length.missing()]),
        type=STRING,
        expr=expr,
        frum=self
    )


@extend(NeOp)
def to_es_script(self, schema):
    return CaseOp("case", [
        WhenOp("when", self.lhs.missing(), **{"then": NotOp("not", self.rhs.missing())}),
        WhenOp("when", self.rhs.missing(), **{"then": NotOp("not", self.lhs.missing())}),
        NotOp("not", BasicEqOp("eq", [self.lhs, self.rhs]))
    ]).partial_eval().to_es_script(schema)


@extend(NeOp)
def to_esfilter(self, schema):
    if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
        columns = schema.values(self.lhs.var)
        if len(columns) == 0:
            return {"match_all": {}}
        elif len(columns) == 1:
            return es_not({"term": {columns[0].es_column: self.rhs.value}})
        else:
            Log.error("column split to multiple, not handled")
    else:
        lhs = self.lhs.partial_eval().to_es_script(schema)
        rhs = self.rhs.partial_eval().to_es_script(schema)

        if lhs.many:
            if rhs.many:
                return es_not(
                    ScriptOp(
                        "script",
                        (
                            "(" + lhs.expr + ").size()==(" + rhs.expr + ").size() && " +
                            "(" + rhs.expr + ").containsAll(" + lhs.expr + ")"
                        )
                    ).to_esfilter(schema)
                )
            else:
                return es_not(
                    ScriptOp("script", "(" + lhs.expr + ").contains(" + rhs.expr + ")").to_esfilter(schema)
                )
        else:
            if rhs.many:
                return es_not(
                    ScriptOp("script", "(" + rhs.expr + ").contains(" + lhs.expr + ")").to_esfilter(schema)
                )
            else:
                return es_not(
                    ScriptOp("script", "(" + lhs.expr + ") != (" + rhs.expr + ")").to_esfilter(schema)
                )

@extend(NotOp)
def to_es_script(self, schema):
    return EsScript(
        type=BOOLEAN,
        expr="!(" + self.term.to_es_script(schema).expr + ")",
        frum=self
    )


@extend(NotOp)
def to_esfilter(self, schema):
    if isinstance(self.term, MissingOp) and isinstance(self.term.expr, Variable):
        v = self.term.expr.var
        cols = schema.leaves(v)
        if cols:
            v = cols[0].es_column
        return {"exists": {"field": v}}
    else:
        operand = self.term.to_esfilter(schema)
        return es_not(operand)


@extend(AndOp)
def to_es_script(self, schema):
    if not self.terms:
        return TRUE.to_es_script()
    else:
        return EsScript(
            miss=FALSE,
            type=BOOLEAN,
            expr=" && ".join("(" + t.to_es_script(schema).expr + ")" for t in self.terms),
            frum=self
        )


@extend(AndOp)
def to_esfilter(self, schema):
    if not len(self.terms):
        return {"match_all": {}}
    else:
        return es_and([t.to_esfilter(schema) for t in self.terms])


@extend(OrOp)
def to_es_script(self, schema):
    return EsScript(
        miss=FALSE,
        type=BOOLEAN,
        expr=" || ".join("(" + t.to_es_script(schema).expr + ")" for t in self.terms if t),
        frum=self
    )


@extend(OrOp)
def to_esfilter(self, schema):
    # OR(x) == NOT(AND(NOT(xi) for xi in x))
    output = es_not(es_and([
        NotOp("not", t).partial_eval().to_esfilter(schema)
        for t in self.terms
    ]))
    return output

    # WE REQUIRE EXIT-EARLY SEMANTICS, OTHERWISE EVERY EXPRESSION IS A SCRIPT EXPRESSION
    # {"bool":{"should"  :[a, b, c]}} RUNS IN PARALLEL
    # {"bool":{"must_not":[a, b, c]}} ALSO RUNS IN PARALLEL


@extend(LengthOp)
def to_es_script(self, schema):
    value = StringOp("string", self.term).to_es_script(schema)
    missing = self.term.missing().partial_eval()
    return EsScript(
        miss=missing,
        type=INTEGER,
        expr="(" + value.expr + ").length()",
        frum=self
    )


@extend(FirstOp)
def to_es_script(self, schema):
    if isinstance(self.term, Variable):
        columns = schema.values(self.term.var)
        if len(columns) == 1:
            return self.term.to_es_script(schema, many=False)

    term = self.term.to_es_script(schema)

    if isinstance(term.frum, CoalesceOp):
        return CoalesceOp("coalesce", [FirstOp("first", t.partial_eval().to_es_script(schema)) for t in term.frum.terms]).to_es_script(schema)

    if term.many:
        return EsScript(
            miss=term.miss,
            type=term.type,
            expr="(" + term.expr + ")[0]",
            frum=term.frum
        ).to_es_script(schema)
    else:
        return term


@extend(BooleanOp)
def to_es_script(self, schema):
    value = self.term.to_es_script(schema)
    if value.many:
        return BooleanOp("boolean", EsScript(
            miss=value.miss,
            type=value.type,
            expr="(" + value.expr + ")[0]",
            frum=value.frum
        )).to_es_script(schema)
    elif value.type == BOOLEAN:
        miss = value.miss
        value.miss = FALSE
        return WhenOp("when",  miss, **{"then": FALSE, "else": value}).partial_eval().to_es_script(schema)
    else:
        return NotOp("not", value.miss).partial_eval().to_es_script(schema)

@extend(BooleanOp)
def to_esfilter(self, schema):
    if isinstance(self.term, Variable):
        return {"term": {self.term.var: True}}
    else:
        return self.to_es_script(schema).to_esfilter(schema)


@extend(IntegerOp)
def to_es_script(self, schema):
    value = self.term.to_es_script(schema)
    if value.many:
        return IntegerOp("integer", EsScript(
            miss=value.missing,
            type=value.type,
            expr="(" + value.expr + ")[0]",
            frum=value.frum
        )).to_es_script(schema)
    elif value.type == BOOLEAN:
        return EsScript(
            miss=value.missing,
            type=INTEGER,
            expr=value.expr + " ? 1 : 0",
            frum=self
        )
    elif value.type == INTEGER:
        return value
    elif value.type == NUMBER:
        return EsScript(
            miss=value.missing,
            type=INTEGER,
            expr="(int)(" + value.expr + ")",
            frum=self
        )
    elif value.type == STRING:
        return EsScript(
            miss=value.missing,
            type=INTEGER,
            expr="Integer.parseInt(" + value.expr + ")",
            frum=self
        )
    else:
        return EsScript(
            miss=value.missing,
            type=INTEGER,
            expr="((" + value.expr + ") instanceof String) ? Integer.parseInt(" + value.expr + ") : (int)(" + value.expr + ")",
            frum=self
        )

@extend(NumberOp)
def to_es_script(self, schema):
    term = FirstOp("first", self.term).partial_eval()
    value = term.to_es_script(schema)

    if isinstance(value.frum, CoalesceOp):
        return CoalesceOp("coalesce", [NumberOp("number", t).partial_eval().to_es_script(schema) for t in value.frum.terms]).to_es_script(schema)

    if value.type == BOOLEAN:
        return EsScript(
            miss=term.missing().partial_eval(),
            type=NUMBER,
            expr=value.expr + " ? 1 : 0",
            frum=self
        )
    elif value.type == INTEGER:
        return EsScript(
            miss=term.missing().partial_eval(),
            type=NUMBER,
            expr=value.expr,
            frum=self
        )
    elif value.type == NUMBER:
        return EsScript(
            miss=term.missing().partial_eval(),
            type=NUMBER,
            expr=value.expr,
            frum=self
        )
    elif value.type == STRING:
        return EsScript(
            miss=term.missing().partial_eval(),
            type=NUMBER,
            expr="Double.parseDouble(" + value.expr + ")",
            frum=self
        )
    elif value.type == OBJECT:
        return EsScript(
            miss=term.missing().partial_eval(),
            type=NUMBER,
            expr="((" + value.expr + ") instanceof String) ? Double.parseDouble(" + value.expr + ") : (" + value.expr + ")",
            frum=self
        )


@extend(IsNumberOp)
def to_es_script(self, schema):
    value = self.term.to_es_script(schema)
    if value.expr or value.i:
        return TRUE.to_es_script(schema)
    else:
        return EsScript(
            miss=FALSE,
            type=BOOLEAN,
            expr="(" + value.expr + ") instanceof java.lang.Double",
            frum=self
        )

@extend(CountOp)
def to_es_script(self, schema):
    return EsScript(
        miss=FALSE,
        type=INTEGER,
        expr="+".join("((" + t.missing().partial_eval().to_es_script(schema).expr + ") ? 0 : 1)" for t in self.terms),
        frum=self
    )


@extend(LengthOp)
def to_esfilter(self, schema):
    return {"regexp": {self.var.var: self.pattern.value}}


@extend(MaxOp)
def to_es_script(self, schema):
    acc = NumberOp("number", self.terms[-1]).partial_eval().to_es_script(schema).expr
    for t in reversed(self.terms[0:-1]):
        acc = "Math.max(" + NumberOp("number", t).partial_eval().to_es_script(schema).expr + " , " + acc + ")"
    return EsScript(
        miss=AndOp("or", [t.missing() for t in self.terms]),
        type=NUMBER,
        expr=acc,
        frum=self
    )


@extend(MinOp)
def to_es_script(self, schema):
    acc = NumberOp("number", self.terms[-1]).partial_eval().to_es_script(schema).expr
    for t in reversed(self.terms[0:-1]):
        acc = "Math.min(" + NumberOp("number", t).partial_eval().to_es_script(schema).expr + " , " + acc + ")"
    return EsScript(
        miss=AndOp("or", [t.missing() for t in self.terms]),
        type=NUMBER,
        expr=acc,
        frum=self
    )


_painless_operators = {
    "add": (" + ", "0"),  # (operator, zero-array default value) PAIR
    "sum": (" + ", "0"),
    "mul": (" * ", "1"),
    "mult": (" * ", "1"),
    "multiply": (" * ", "1")
}


@extend(MultiOp)
def to_es_script(self, schema):
    op, unit = _painless_operators[self.op]
    if self.nulls:
        calc = op.join(
            "((" + t.missing().to_es_script(schema).expr + ") ? " + unit + " : (" + NumberOp("number", t).partial_eval().to_es_script(schema).expr + "))"
            for t in self.terms
        )
        return WhenOp(
            "when",
            AndOp("and", [t.missing() for t in self.terms]),
            **{"then": self.default, "else": EsScript(type=NUMBER, expr=calc, frum=self)}
        ).partial_eval().to_es_script(schema)
    else:
        calc = op.join(
            "(" + NumberOp("number", t).to_es_script(schema).expr + ")"
            for t in self.terms
        )
        return WhenOp(
            "when",
            OrOp("or", [t.missing() for t in self.terms]),
            **{"then": self.default, "else": EsScript(type=NUMBER, expr=calc, frum=self)}
        ).partial_eval().to_es_script(schema)


@extend(RegExpOp)
def to_esfilter(self, schema):
    if isinstance(self.pattern, Literal) and isinstance(self.var, Variable):
        cols = schema.leaves(self.var.var)
        if len(cols) == 0:
            return MATCH_NONE
        elif len(cols) == 1:
            return {"regexp": {cols[0].es_column: self.pattern.value}}
        else:
            Log.error("regex on not supported ")
    else:
        Log.error("regex only accepts a variable and literal pattern")


@extend(StringOp)
def to_es_script(self, schema):
    term = FirstOp("first", self.term).partial_eval()
    value = term.to_es_script(schema)

    if isinstance(value.frum, CoalesceOp):
        return CoalesceOp("coalesce", [StringOp("string", t).partial_eval() for t in value.frum.terms]).to_es_script(schema)

    if value.type == BOOLEAN:
        return EsScript(
            miss=self.term.missing().partial_eval(),
            type=STRING,
            expr=value.expr + ' ? "T" : "F"',
            frum=self
        )
    elif value.type == INTEGER:
        return EsScript(
            miss=self.term.missing().partial_eval(),
            type=STRING,
            expr="String.valueOf(" + value.expr + ")",
            frum=self
        )
    elif value.type == NUMBER:
        return EsScript(
            miss=self.term.missing().partial_eval(),
            type=STRING,
            expr=expand_template(TO_STRING, {"expr":value.expr}),
            frum=self
        )
    elif value.type == STRING:
        return value
    else:
        return EsScript(
            miss=self.term.missing().partial_eval(),
            type=STRING,
            expr=expand_template(TO_STRING, {"expr":value.expr}),
            frum=self
        )

    # ((Runnable)(() -> {int a=2; int b=3; System.out.println(a+b);})).run();
    # "((Runnable)((value) -> {String output=String.valueOf(value); if (output.endsWith('.0')) {return output.substring(0, output.length-2);} else return output;})).run(" + value.expr + ")"


@extend(TrueOp)
def to_es_script(self, schema):
    return EsScript(type=BOOLEAN, expr="true", frum=self)


@extend(TrueOp)
def to_esfilter(self, schema):
    return {"match_all": {}}


@extend(PrefixOp)
def to_es_script(self, schema):
    if not self.field:
        return "true"
    else:
        return "(" + self.field.to_es_script(schema) + ").startsWith(" + self.prefix.to_es_script(schema) + ")"


@extend(PrefixOp)
def to_esfilter(self, schema):
    if not self.expr:
        return {"match_all": {}}
    elif isinstance(self.expr, Variable) and isinstance(self.prefix, Literal):
        var = schema.leaves(self.expr.var)[0].es_column
        return {"prefix": {var: self.prefix.value}}
    else:
        return ScriptOp("script",  self.to_es_script(schema).script(schema)).to_esfilter(schema)

@extend(SuffixOp)
def to_es_script(self, schema):
    if not self.suffix:
        return "true"
    else:
        return "(" + self.expr.to_es_script(schema) + ").endsWith(" + self.suffix.to_es_script(schema) + ")"


@extend(SuffixOp)
def to_esfilter(self, schema):
    if not self.suffix:
        return {"match_all": {}}
    elif isinstance(self.expr, Variable) and isinstance(self.suffix, Literal):
        var = schema.leaves(self.expr.var)[0].es_column
        return {"regexp": {var: ".*"+string2regexp(self.suffix.value)}}
    else:
        return ScriptOp("script",  self.to_es_script(schema).script(schema)).to_esfilter(schema)


@extend(InOp)
def to_es_script(self, schema):
    superset = self.superset.to_es_script(schema)
    value = self.value.to_es_script(schema)
    return EsScript(
        type=BOOLEAN,
        expr="(" + superset.expr + ").contains(" + value.expr + ")",
        frum=self
    )


@extend(InOp)
def to_esfilter(self, schema):
    if isinstance(self.value, Variable):
        var = self.value.var
        cols = schema.leaves(var)
        if cols:
            var = cols[0].es_column
        return {"terms": {var: self.superset.value}}
    else:
        return ScriptOp("script",  self.to_es_script(schema).script(schema)).to_esfilter(schema)


@extend(ScriptOp)
def to_es_script(self, schema):
    return EsScript(type=self.data_type, expr=self.script, frum=self)


@extend(ScriptOp)
def to_esfilter(self, schema):
    return {"script": es_script(self.script)}


@extend(Variable)
def to_es_script(self, schema, many=True):
    if self.var == ".":
        return "_source"
    else:
        if self.var == "_id":
            return EsScript(type=STRING, expr='doc["_uid"].value.substring(doc["_uid"].value.indexOf(\'#\')+1)', frum=self)

        columns = schema.values(self.var)
        acc = []
        for c in columns:
            varname = c.es_column
            frum = Variable(c.es_column)
            q = quote(varname)
            if many:
                acc.append(EsScript(
                    miss=frum.missing(),
                    type=c.jx_type,
                    expr="doc[" + q + "].values" if c.jx_type != BOOLEAN else "doc[" + q + "].value==\"T\"",
                    frum=frum,
                    many=True
                ))
            else:
                acc.append(EsScript(
                    miss=frum.missing(),
                    type=c.jx_type,
                    expr="doc[" + q + "].value" if c.jx_type != BOOLEAN else "doc[" + q + "].value==\"T\"",
                    frum=frum,
                    many=True
            ))

        if len(acc) == 0:
            return NULL.to_es_script(schema)
        elif len(acc) == 1:
            return acc[0]
        else:
            return CoalesceOp("coalesce", acc).to_es_script(schema)


@extend(WhenOp)
def to_es_script(self, schema):
    if self.simplified:
        when = self.when.to_es_script(schema)
        then = self.then.to_es_script(schema)
        els_ = self.els_.to_es_script(schema)

        if when is TRUE:
            return then
        elif when is FALSE:
            return els_
        elif then.miss is TRUE:
            return EsScript(
                miss=self.missing(),
                type=els_.type,
                expr=els_.expr,
                frum=self
            )
        elif els_.miss is TRUE:
            return EsScript(
                miss=self.missing(),
                type=then.type,
                expr=then.expr,
                frum=self
            )

        elif then.type == els_.type:
            return EsScript(
                miss=self.missing(),
                type=then.type,
                expr="(" + when.expr + ") ? (" + then.expr + ") : (" + els_.expr + ")",
                frum=self
            )
        elif then.type in (INTEGER, NUMBER) and els_.type in (INTEGER, NUMBER):
            return EsScript(
                miss=self.missing(),
                type=NUMBER,
                expr="(" + when.expr + ") ? (" + then.expr + ") : (" + els_.expr + ")",
                frum=self
            )
        else:
            Log.error("do not know how to handle")
    else:
        return self.partial_eval().to_es_script(schema)


@extend(WhenOp)
def to_esfilter(self, schema):
    output = OrOp("or", [
        AndOp("and", [self.when, BooleanOp("boolean", self.then)]),
        AndOp("and", [NotOp("not", self.when), BooleanOp("boolean", self.els_)])
    ]).partial_eval()

    return output.to_esfilter(schema)


@extend(BasicIndexOfOp)
def to_es_script(self, schema):
    v = StringOp("string", self.value).to_es_script(schema).expr
    find = StringOp("string", self.find).to_es_script(schema).expr
    start = IntegerOp("integer", self.start).to_es_script(schema).expr

    return EsScript(
        miss=FALSE,
        type=INTEGER,
        expr="(" + v + ").indexOf(" + find + ", " + start + ")",
        frum=self
    )


@extend(BasicIndexOfOp)
def to_esfilter(self, schema):
    return ScriptOp("", self.to_es_script(schema).script(schema)).to_esfilter(schema)


@extend(BasicSubstringOp)
def to_es_script(self, schema):
    v = StringOp("string", self.value).partial_eval().to_es_script(schema).expr
    start = IntegerOp("string", self.start).partial_eval().to_es_script(schema).expr
    end = IntegerOp("integer", self.end).partial_eval().to_es_script(schema).expr

    return EsScript(
        miss=FALSE,
        type=STRING,
        expr="(" + v + ").substring(" + start + ", " + end + ")",
        frum=self
    )



MATCH_ALL = wrap({"match_all": {}})
MATCH_NONE = es_not({"match_all": {}})


def simplify_esfilter(esfilter):
    try:
        output = wrap(_normalize(wrap(esfilter)))
        output.isNormal = None
        return output
    except Exception as e:
        from mo_logs import Log

        Log.unexpected("programmer error", cause=e)


def _normalize(esfilter):
    """
    TODO: DO NOT USE Data, WE ARE SPENDING TOO MUCH TIME WRAPPING/UNWRAPPING
    REALLY, WE JUST COLLAPSE CASCADING `and` AND `or` FILTERS
    """
    if esfilter == MATCH_ALL or esfilter == MATCH_NONE or esfilter.isNormal:
        return esfilter

    # Log.note("from: " + convert.value2json(esfilter))
    isDiff = True

    while isDiff:
        isDiff = False

        if esfilter['and']:
            terms = esfilter['and']
            for (i0, t0), (i1, t1) in itertools.product(enumerate(terms), enumerate(terms)):
                if i0 == i1:
                    continue  # SAME, IGNORE
                # TERM FILTER ALREADY ASSUMES EXISTENCE
                with suppress_exception:
                    if t0.exists.field != None and t0.exists.field == t1.term.items()[0][0]:
                        terms[i0] = MATCH_ALL
                        continue

                # IDENTICAL CAN BE REMOVED
                with suppress_exception:
                    if t0 == t1:
                        terms[i0] = MATCH_ALL
                        continue

                # MERGE range FILTER WITH SAME FIELD
                if i0 > i1:
                    continue  # SAME, IGNORE
                with suppress_exception:
                    f0, tt0 = t0.range.items()[0]
                    f1, tt1 = t1.range.items()[0]
                    if f0 == f1:
                        set_default(terms[i0].range[literal_field(f1)], tt1)
                        terms[i1] = MATCH_ALL

            output = []
            for a in terms:
                if isinstance(a, (list, set)):
                    from mo_logs import Log

                    Log.error("and clause is not allowed a list inside a list")
                a_ = _normalize(a)
                if a_ is not a:
                    isDiff = True
                a = a_
                if a == MATCH_ALL:
                    isDiff = True
                    continue
                if a == MATCH_NONE:
                    return MATCH_NONE
                if a['and']:
                    isDiff = True
                    a.isNormal = None
                    output.extend(a['and'])
                else:
                    a.isNormal = None
                    output.append(a)
            if not output:
                return MATCH_ALL
            elif len(output) == 1:
                # output[0].isNormal = True
                esfilter = output[0]
                break
            elif isDiff:
                esfilter = es_and(output)
            continue

        if esfilter.bool.should:
            output = []
            for a in esfilter.bool.should:
                a_ = _normalize(a)
                if a_ is not a:
                    isDiff = True
                a = a_

                if a.bool.should:
                    a.isNormal = None
                    isDiff = True
                    output.extend(a.bool.should)
                else:
                    a.isNormal = None
                    output.append(a)
            if not output:
                return MATCH_NONE
            elif len(output) == 1:
                esfilter = output[0]
                break
            elif isDiff:
                esfilter = wrap({"bool": {"should": output}})
            continue

        if esfilter.term != None:
            if esfilter.term.keys():
                esfilter.isNormal = True
                return esfilter
            else:
                return MATCH_ALL

        if esfilter.terms:
            for k, v in esfilter.terms.items():
                if len(v) > 0:
                    if OR(vv == None for vv in v):
                        rest = [vv for vv in v if vv != None]
                        if len(rest) > 0:
                            output = es_or([
                                es_missing(k),
                                {"terms": {k: rest}}
                            ])
                        else:
                            output = es_missing(k)
                        output.isNormal = True
                        return output
                    else:
                        esfilter.isNormal = True
                        return esfilter
            return MATCH_NONE

        if esfilter['not']:
            _sub = esfilter['not']
            sub = _normalize(_sub)
            if sub == MATCH_NONE:
                return MATCH_ALL
            elif sub == MATCH_ALL:
                return MATCH_NONE
            elif sub is not _sub:
                sub.isNormal = None
                return wrap({"not": sub, "isNormal": True})
            else:
                sub.isNormal = None

    esfilter.isNormal = True
    return esfilter


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
        var_to_depth = {v.var: max(len(c.nested_path) - 1, 0) for v in vars_ for c in schema[v.var]}
        all_depths = set(var_to_depth.values())
        # if -1 in all_depths:
        #     Log.error(
        #         "Can not find column with name {{column|quote}}",
        #         column=unwraplist([k for k, v in var_to_depth.items() if v == -1])
        #     )
        if len(all_depths) == 0:
            all_depths = {0}
        output = wrap([[] for _ in range(MAX(all_depths) + 1)])
    else:
        all_depths = set(var_to_depth[v.var] for v in vars_)

    if len(all_depths) == 1:
        output[list(all_depths)[0]] += [where]
    elif isinstance(where, AndOp):
        for a in where.terms:
            split_expression_by_depth(a, schema, output, var_to_depth)
    else:
        Log.error("Can not handle complex where clause")

    return output


def get_type(var_name):
    type_ = var_name.split(".$")[1:]
    if not type_:
        return "j"
    return json_type_to_es_script_type.get(type_[0], "j")


json_type_to_es_script_type = {
    "string": "s",
    "boolean": "b",
    "number": "n"
}
