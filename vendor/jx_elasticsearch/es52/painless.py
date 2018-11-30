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

from jx_base.expressions import Variable, TupleOp, LeavesOp, BaseBinaryOp, OrOp, ScriptOp, WhenOp, extend, Literal, NullOp, TrueOp, FalseOp, DivOp, FloorOp, EqOp, NeOp, NotOp, LengthOp, NumberOp, StringOp, CountOp, BaseMultiOp, CoalesceOp, MissingOp, ExistsOp, PrefixOp, NotLeftOp, InOp, CaseOp, AndOp, ConcatOp, IsNumberOp, Expression, BasicIndexOfOp, MaxOp, MinOp, BasicEqOp, BooleanOp, IntegerOp, BasicSubstringOp, ZERO, NULL, FirstOp, FALSE, TRUE, SuffixOp, simplified, ONE, BasicStartsWithOp, BasicMultiOp, UnionOp, merge_types, BaseInequalityOp
from jx_elasticsearch.es52.expressions import box
from jx_elasticsearch.es52.util import es_script
from mo_dots import coalesce
from mo_future import text_type
from mo_json import NUMBER, STRING, BOOLEAN, OBJECT, INTEGER, IS_NULL
from mo_logs import Log
from mo_logs.strings import expand_template, quote
from mo_times import Date

NUMBER_TO_STRING = """
Optional.of({{expr}}).map(
    value -> {
        String output = String.valueOf(value);
        if (output.endsWith(".0")) output = output.substring(0, output.length() - 2);
        return output;
    }
).orElse(null)
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
        self.frum = frum  # THE ORIGINAL EXPRESSION THAT MADE expr

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

        return "(" + missing.to_es_script(schema).expr + ")?null:(" + box(self) + ")"

    def to_esfilter(self, schema):
        return {"script": es_script(self.script(schema))}

    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
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


@extend(Variable)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    if self.var == ".":
        return EsScript(type=OBJECT, expr="_source", frum=self)
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
                    expr="doc[" + q + "].values" if c.jx_type != BOOLEAN else "doc[" + q + "].value",
                    frum=frum,
                    many=c.jx_type != BOOLEAN
                ))
            else:
                acc.append(EsScript(
                    miss=frum.missing(),
                    type=c.jx_type,
                    expr="doc[" + q + "].value" if c.jx_type != BOOLEAN else "doc[" + q + "].value",
                    frum=frum,
                    many=True
                ))

        if len(acc) == 0:
            return NULL.to_es_script(schema)
        elif len(acc) == 1:
            return acc[0]
        else:
            return CoalesceOp(acc).to_es_script(schema)


@extend(BaseBinaryOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    lhs = NumberOp(self.lhs).partial_eval().to_es_script(schema).expr
    rhs = NumberOp(self.rhs).partial_eval().to_es_script(schema).expr
    script = "(" + lhs + ") " + _painless_operators[self.op][0] + " (" + rhs + ")"
    missing = OrOp([self.lhs.missing(), self.rhs.missing()])

    return WhenOp(
        missing,
        **{
            "then": self.default,
            "else":
                EsScript(type=NUMBER, expr=script, frum=self)
        }
    ).partial_eval().to_es_script(schema)


@extend(CaseOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    acc = self.whens[-1].partial_eval().to_es_script(schema)
    for w in reversed(self.whens[0:-1]):
        acc = WhenOp(
            w.when,
            **{"then": w.then, "else": acc}
        ).partial_eval().to_es_script(schema)
    return acc


@extend(ConcatOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    if len(self.terms) == 0:
        return self.default.to_es_script(schema)

    acc = []
    separator = StringOp(self.separator).partial_eval()
    sep = separator.to_es_script(schema).expr
    for t in self.terms:
        val = WhenOp(
            t.missing(),
            **{
                "then": Literal(""),
                "else": EsScript(type=STRING, expr=sep + "+" + StringOp(t).partial_eval().to_es_script(schema).expr, frum=t)
                # "else": ConcatOp([sep, t])
            }
        )
        acc.append("(" + val.partial_eval().to_es_script(schema).expr + ")")
    expr_ = "(" + "+".join(acc) + ").substring(" + LengthOp(separator).to_es_script(schema).expr + ")"

    if self.default is NULL:
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
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
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
        if isinstance(v, Date):
            return EsScript(
                type=NUMBER,
                expr=text_type(v.unix),
                frum=self
            )

    return _convert(self.term)


@extend(CoalesceOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    if not self.terms:
        return NULL.to_es_script(schema)
    # acc.miss WILL SAY IF THIS COALESCE RETURNS NULL,
    # acc.expr WILL ASSUMED TO BE A VALUE, SO THE LAST TERM IS ASSUMED NOT NULL
    v = self.terms[-1]
    acc = FirstOp(v).partial_eval().to_es_script(schema)
    for v in reversed(self.terms[:-1]):
        m = v.missing().partial_eval()
        e = NotOp(m).partial_eval().to_es_script(schema)
        r = FirstOp(v).partial_eval().to_es_script(schema)

        if r.miss is TRUE:
            continue
        elif r.miss is FALSE:
            acc = r
            continue
        elif acc.type == r.type or acc.type == IS_NULL:
            new_type = r.type
        elif acc.type == NUMBER and r.type == INTEGER:
            new_type = NUMBER
        elif acc.type == INTEGER and r.type == NUMBER:
            new_type = NUMBER
        else:
            new_type = OBJECT

        acc = EsScript(
            miss=AndOp([acc.miss, m]).partial_eval(),
            type=new_type,
            expr="(" + e.expr + ") ? (" + r.expr + ") : (" + acc.expr + ")",
            frum=self
        )
    return acc


@extend(ExistsOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    return self.field.exists().partial_eval().to_es_script(schema)


@extend(NullOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    return null_script


@extend(FalseOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    return false_script


@extend(TupleOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    expr = 'new Object[]{' + ','.join(
        FirstOp(t).partial_eval().to_es_script(schema).script(schema)
        for t in self.terms
    ) + '}'
    return EsScript(
        type=OBJECT,
        expr=expr,
        miss=FALSE,
        many=FALSE,
        frum=self
    )


@extend(LeavesOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    Log.error("not supported")


@extend(BaseInequalityOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    lhs = NumberOp(self.lhs).partial_eval().to_es_script(schema).expr
    rhs = NumberOp(self.rhs).partial_eval().to_es_script(schema).expr
    script = "(" + lhs + ") " + _painless_operators[self.op][0] + " (" + rhs + ")"

    output = WhenOp(
        OrOp([self.lhs.missing(), self.rhs.missing()]),
        **{
            "then": FALSE,
            "else":
                EsScript(type=BOOLEAN, expr=script, frum=self)
        }
    ).partial_eval().to_es_script(schema)
    return output


@extend(DivOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    lhs = NumberOp(self.lhs).partial_eval()
    rhs = NumberOp(self.rhs).partial_eval()
    script = "(" + lhs.to_es_script(schema).expr + ") / (" + rhs.to_es_script(schema).expr + ")"

    output = WhenOp(
        OrOp([self.lhs.missing(), self.rhs.missing(), EqOp([self.rhs, ZERO])]),
        **{
            "then": self.default,
            "else": EsScript(type=NUMBER, expr=script, frum=self)
        }
    ).partial_eval().to_es_script(schema)

    return output


@extend(FloorOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    lhs = self.lhs.partial_eval().to_es_script(schema)
    rhs = self.rhs.partial_eval().to_es_script(schema)

    if rhs.frum is ONE:
        script = "(int)Math.floor(" + lhs.expr + ")"
    else:
        script = "Math.floor((" + lhs.expr + ") / (" + rhs.expr + "))*(" + rhs.expr + ")"

    output = WhenOp(
        OrOp([lhs.miss, rhs.miss, EqOp([self.rhs, ZERO])]),
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


@simplified
@extend(EqOp)
def partial_eval(self):
    lhs = self.lhs.partial_eval()
    rhs = self.rhs.partial_eval()
    return EqOp([lhs, rhs])


@extend(EqOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    return CaseOp([
        WhenOp(self.lhs.missing(), **{"then": self.rhs.missing()}),
        WhenOp(self.rhs.missing(), **{"then": FALSE}),
        BasicEqOp([self.lhs, self.rhs])
    ]).partial_eval().to_es_script(schema)


@extend(BasicEqOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    simple_rhs = self.rhs.partial_eval()
    lhs = self.lhs.partial_eval().to_es_script(schema)
    rhs = simple_rhs.to_es_script(schema)

    if lhs.many:
        if rhs.many:
            return AndOp([
                EsScript(type=BOOLEAN, expr="(" + lhs.expr + ").size()==(" + rhs.expr + ").size()", frum=self),
                EsScript(type=BOOLEAN, expr="(" + rhs.expr + ").containsAll(" + lhs.expr + ")", frum=self)
            ]).to_es_script(schema)
        else:
            if lhs.type == BOOLEAN:
                if isinstance(simple_rhs, Literal) and simple_rhs.value in ('F', False):
                    return EsScript(type=BOOLEAN, expr="!" + lhs.expr, frum=self)
                elif isinstance(simple_rhs, Literal) and simple_rhs.value in ('T', True):
                    return EsScript(type=BOOLEAN, expr=lhs.expr, frum=self)
                else:
                    return EsScript(type=BOOLEAN, expr="(" + lhs.expr + ")==(" + rhs.expr + ")", frum=self)
            else:
                return EsScript(type=BOOLEAN, expr="(" + lhs.expr + ").contains(" + rhs.expr + ")",frum=self)
    elif rhs.many:
        return EsScript(
            type=BOOLEAN,
            expr="(" + rhs.expr + ").contains(" + lhs.expr + ")",
            frum=self
        )
    else:
        if lhs.type == BOOLEAN:
            if isinstance(simple_rhs, Literal) and simple_rhs.value in ('F', False):
                return EsScript(type=BOOLEAN, expr="!" + lhs.expr, frum=self)
            elif isinstance(simple_rhs, Literal) and simple_rhs.value in ('T', True):
                return EsScript(type=BOOLEAN, expr=lhs.expr, frum=self)
            else:
                return EsScript(type=BOOLEAN, expr="(" + lhs.expr + ")==(" + rhs.expr + ")", frum=self)
        else:
            return EsScript(type=BOOLEAN, expr="(" + lhs.expr + "==" + rhs.expr + ")", frum=self)


@extend(BasicMultiOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    op, identity = _painless_operators[self.op]
    if len(self.terms) == 0:
        return Literal("", identity).to_es_script(schema)
    elif len(self.terms) == 1:
        return self.terms[0].to_esscript()
    else:
        return EsScript(
            type=NUMBER,
            expr=op.join("(" + t.to_es_script(schema, not_null=True, many=False).expr + ")" for t in self.terms),
            frum=self
        )


@extend(MissingOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    if isinstance(self.expr, Variable):
        if self.expr.var == "_id":
            return EsScript(type=BOOLEAN, expr="false", frum=self)
        else:
            columns = schema.leaves(self.expr.var)
            return AndOp([
                EsScript(
                    type=BOOLEAN,
                    expr="doc[" + quote(c.es_column) + "].empty",
                    frum=self
                )
                for c in columns
            ]).partial_eval().to_es_script(schema)
    elif isinstance(self.expr, Literal):
        return self.expr.missing().to_es_script(schema)
    else:
        return self.expr.missing().partial_eval().to_es_script(schema)


@extend(NotLeftOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    v = StringOp(self.value).partial_eval().to_es_script(schema).expr
    l = NumberOp(self.length).partial_eval().to_es_script(schema).expr

    expr = "(" + v + ").substring((int)Math.max(0, (int)Math.min(" + v + ".length(), " + l + ")))"
    return EsScript(
        miss=OrOp([self.value.missing(), self.length.missing()]),
        type=STRING,
        expr=expr,
        frum=self
    )


@extend(NeOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    return CaseOp([
        WhenOp(self.lhs.missing(), **{"then": NotOp(self.rhs.missing())}),
        WhenOp(self.rhs.missing(), **{"then": NotOp(self.lhs.missing())}),
        NotOp(BasicEqOp([self.lhs, self.rhs]))
    ]).partial_eval().to_es_script(schema)


@extend(NotOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    return EsScript(
        type=BOOLEAN,
        expr="!(" + self.term.partial_eval().to_es_script(schema).expr + ")",
        frum=self
    )


@extend(AndOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    if not self.terms:
        return TRUE.to_es_script()
    else:
        return EsScript(
            miss=FALSE,
            type=BOOLEAN,
            expr=" && ".join("(" + t.to_es_script(schema).expr + ")" for t in self.terms),
            frum=self
        )


@extend(OrOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    return EsScript(
        miss=FALSE,
        type=BOOLEAN,
        expr=" || ".join("(" + t.to_es_script(schema).expr + ")" for t in self.terms if t),
        frum=self
    )


@extend(LengthOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    value = StringOp(self.term).to_es_script(schema)
    missing = self.term.missing().partial_eval()
    return EsScript(
        miss=missing,
        type=INTEGER,
        expr="(" + value.expr + ").length()",
        frum=self
    )


@extend(FirstOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    if isinstance(self.term, Variable):
        columns = schema.values(self.term.var)
        if len(columns) == 0:
            return null_script
        if len(columns) == 1:
            return self.term.to_es_script(schema, many=False)

    term = self.term.to_es_script(schema)

    if isinstance(term.frum, CoalesceOp):
        return CoalesceOp([FirstOp(t.partial_eval().to_es_script(schema)) for t in term.frum.terms]).to_es_script(schema)

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
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    value = self.term.to_es_script(schema)
    if value.many:
        return BooleanOp(EsScript(
            miss=value.miss,
            type=value.type,
            expr="(" + value.expr + ")[0]",
            frum=value.frum
        )).to_es_script(schema)
    elif value.type == BOOLEAN:
        miss = value.miss
        value.miss = FALSE
        return WhenOp( miss, **{"then": FALSE, "else": value}).partial_eval().to_es_script(schema)
    else:
        return NotOp(value.miss).partial_eval().to_es_script(schema)


@extend(IntegerOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    value = self.term.to_es_script(schema)
    if value.many:
        return IntegerOp(EsScript(
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
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    term = FirstOp(self.term).partial_eval()
    value = term.to_es_script(schema)

    if isinstance(value.frum, CoalesceOp):
        return CoalesceOp([NumberOp(t).partial_eval().to_es_script(schema) for t in value.frum.terms]).to_es_script(schema)

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
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
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


count_template = "long count=0; for(v in {{expr}}) if (v!=null) count+=1; return count;"


@extend(CountOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    return EsScript(
        miss=FALSE,
        type=INTEGER,
        expr=expand_template(count_template, {"expr": self.terms.partial_eval().to_es_script(schema).expr}),
        frum=self
    )


@extend(MaxOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    acc = NumberOp(self.terms[-1]).partial_eval().to_es_script(schema).expr
    for t in reversed(self.terms[0:-1]):
        acc = "Math.max(" + NumberOp(t).partial_eval().to_es_script(schema).expr + " , " + acc + ")"
    return EsScript(
        miss=AndOp([t.missing() for t in self.terms]),
        type=NUMBER,
        expr=acc,
        frum=self
    )


@extend(MinOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    acc = NumberOp(self.terms[-1]).partial_eval().to_es_script(schema).expr
    for t in reversed(self.terms[0:-1]):
        acc = "Math.min(" + NumberOp(t).partial_eval().to_es_script(schema).expr + " , " + acc + ")"
    return EsScript(
        miss=AndOp([t.missing() for t in self.terms]),
        type=NUMBER,
        expr=acc,
        frum=self
    )


_painless_operators = {
    "add": (" + ", "0"),  # (operator, zero-array default value) PAIR
    "sum": (" + ", "0"),
    "mul": (" * ", "1"),
    "basic.add": (" + ", "0"),
    "basic.mul": (" * ", "1"),
    "sub": ("-", None),
    "div": ("/", None),
    "exp": ("**", None),
    "mod": ("%", None),
    "gt": (">", None),
    "gte": (">=", None),
    "lte": ("<=", None),
    "lt": ("<", None)
}


@extend(BaseMultiOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    op, unit = _painless_operators[self.op]
    if self.nulls:
        calc = op.join(
            "((" + t.missing().to_es_script(schema).expr + ") ? " + unit + " : (" + NumberOp(t).partial_eval().to_es_script(schema).expr + "))"
            for t in self.terms
        )
        return WhenOp(
            AndOp([t.missing() for t in self.terms]),
            **{"then": self.default, "else": EsScript(type=NUMBER, expr=calc, frum=self)}
        ).partial_eval().to_es_script(schema)
    else:
        calc = op.join(
            "(" + NumberOp(t).to_es_script(schema).expr + ")"
            for t in self.terms
        )
        return WhenOp(
            OrOp([t.missing() for t in self.terms]),
            **{"then": self.default, "else": EsScript(type=NUMBER, expr=calc, frum=self)}
        ).partial_eval().to_es_script(schema)


@extend(StringOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    term = FirstOp(self.term).partial_eval()
    value = term.to_es_script(schema)

    if isinstance(value.frum, CoalesceOp):
        return CoalesceOp([StringOp(t).partial_eval() for t in value.frum.terms]).to_es_script(schema)

    if value.miss is TRUE or value.type is IS_NULL:
        return empty_string_script
    elif value.type == BOOLEAN:
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
            expr=expand_template(NUMBER_TO_STRING, {"expr":value.expr}),
            frum=self
        )
    elif value.type == STRING:
        return value
    else:
        return EsScript(
            miss=self.term.missing().partial_eval(),
            type=STRING,
            expr=expand_template(NUMBER_TO_STRING, {"expr":value.expr}),
            frum=self
        )

    # ((Runnable)(() -> {int a=2; int b=3; System.out.println(a+b);})).run();
    # "((Runnable)((value) -> {String output=String.valueOf(value); if (output.endsWith('.0')) {return output.substring(0, output.length-2);} else return output;})).run(" + value.expr + ")"


true_script = EsScript(type=BOOLEAN, expr="true", frum=TRUE)
false_script = EsScript(type=BOOLEAN, expr="false", frum=FALSE)
null_script = EsScript(miss=TRUE, type=IS_NULL, expr="null", frum=NULL)
empty_string_script = EsScript(miss=TRUE, type=STRING, expr='""', frum=NULL)


@extend(TrueOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    return true_script


@extend(BasicStartsWithOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    expr = FirstOp(self.value).partial_eval().to_es_script(schema)
    if expr is empty_string_script:
        return false_script

    prefix = self.prefix.to_es_script(schema).partial_eval()
    return EsScript(
        miss=FALSE,
        type=BOOLEAN,
        expr="(" + expr.expr + ").startsWith(" + prefix.expr + ")",
        frum=self
    )


@extend(PrefixOp)
def partial_eval(self):
    if not self.expr:
        return TRUE

    expr = StringOp(self.expr).partial_eval()
    prefix = StringOp(self.prefix).partial_eval()

    if self.expr is NULL:
        return TRUE

    return PrefixOp([expr, prefix])


@extend(PrefixOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    if not self.expr:
        return true_script
    else:
        return EsScript(
            type=BOOLEAN,
            expr = "(" + self.expr.to_es_script(schema).script(schema) + ").startsWith(" + self.prefix.to_es_script(schema).script(schema) + ")",
            frum=self
        )


@extend(SuffixOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    if not self.suffix:
        return true_script
    else:
        return EsScript(
            miss=OrOp([MissingOp(self.expr), MissingOp(self.suffix)]).partial_eval(),
            expr = "(" + self.expr.to_es_script(schema) + ").endsWith(" + self.suffix.to_es_script(schema) + ")"
        )


@extend(InOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    superset = self.superset.to_es_script(schema)
    value = self.value.to_es_script(schema)
    return EsScript(
        type=BOOLEAN,
        expr="(" + superset.expr + ").contains(" + value.expr + ")",
        frum=self
    )


@extend(ScriptOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    return EsScript(type=self.data_type, expr=self.script, frum=self)


@extend(WhenOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
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
            Log.error("do not know how to handle: {{self}}", self=self.__data__())
    else:
        return self.partial_eval().to_es_script(schema)


@extend(UnionOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    code = """
    HashSet output = new HashSet();
    {{LOOPS}}
    return output.toArray();
    """
    parts = [t.partial_eval().to_es_script(schema, many=True) for t in self.terms]
    loops = ["for (v in " + p.expr + ") output.add(v);" for p in parts]
    return EsScript(
        type=merge_types(p.type for p in parts),
        expr=code.replace("{{LOOPS}}", "\n".join(loops)),
        many=True,
        frum=self
    )


@extend(BasicIndexOfOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    v = StringOp(self.value).to_es_script(schema).expr
    find = StringOp(self.find).to_es_script(schema).expr
    start = IntegerOp(self.start).to_es_script(schema).expr

    return EsScript(
        miss=FALSE,
        type=INTEGER,
        expr="(" + v + ").indexOf(" + find + ", " + start + ")",
        frum=self
    )


@extend(BasicSubstringOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    v = StringOp(self.value).partial_eval().to_es_script(schema).expr
    start = IntegerOp(self.start).partial_eval().to_es_script(schema).expr
    end = IntegerOp(self.end).partial_eval().to_es_script(schema).expr

    return EsScript(
        miss=FALSE,
        type=STRING,
        expr="(" + v + ").substring(" + start + ", " + end + ")",
        frum=self
    )



