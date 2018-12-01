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

from collections import Mapping

from jx_base.expressions import (
    Variable as Variable_,
    DateOp as DateOp_,
    TupleOp as TupleOp_,
    LeavesOp as LeavesOp_,
    BaseBinaryOp as BaseBinaryOp_,
    OrOp as OrOp_,
    ScriptOp as ScriptOp_,
    RowsOp as RowsOp_,
    OffsetOp as OffsetOp_,
    GetOp as GetOp_,
    Literal as Literal_,
    NullOp as NullOp_,
    TrueOp as TrueOp_,
    FalseOp as FalseOp_,
    DivOp as DivOp_,
    FloorOp as FloorOp_,
    EqOp as EqOp_,
    NeOp as NeOp_,
    NotOp as NotOp_,
    LengthOp as LengthOp_,
    FirstOp as FirstOp_,
    NumberOp as NumberOp_,
    StringOp as StringOp_,
    CountOp as CountOp_,
    RegExpOp as RegExpOp_,
    CoalesceOp as CoalesceOp_,
    MissingOp as MissingOp_,
    ExistsOp as ExistsOp_,
    PrefixOp as PrefixOp_,
    NotLeftOp as NotLeftOp_,
    RightOp as RightOp_,
    NotRightOp as NotRightOp_,
    BasicIndexOfOp as BasicIndexOfOp_,
    FindOp as FindOp_,
    BetweenOp as BetweenOp_,
    RangeOp as RangeOp_,
    CaseOp as CaseOp_,
    AndOp as AndOp_,
    ConcatOp as ConcatOp_,
    InOp as InOp_,
    WhenOp as WhenOp_,
    MaxOp as MaxOp_,
    SplitOp as SplitOp_,
    NULL,
    SelectOp as SelectOp_,
    SuffixOp as SuffixOp_,
    LastOp as LastOp_,
    IntegerOp as IntegerOp_,
    BasicEqOp as BasicEqOp_,
    BaseInequalityOp as BaseInequalityOp_,
    BaseMultiOp as BaseMultiOp_,
    Expression,
    define_language,
    _jx_expression,
)
from jx_base.utils import Language
from jx_python.expression_compiler import compile_expression
from mo_dots import split_field
from mo_dots import unwrap
from mo_future import text_type
from mo_json import json2value
from mo_logs import Log
from mo_logs.strings import quote
from mo_times.dates import Date
from pyLibrary import convert


def jx_expression_to_function(expr):
    """
    RETURN FUNCTION THAT REQUIRES PARAMETERS (row, rownum=None, rows=None):
    """
    if isinstance(expr, Expression):
        if isinstance(expr, ScriptOp) and not isinstance(expr.script, text_type):
            return expr.script
        else:
            return compile_expression(_jx_expression(expr, language).to_python())
    if (
        expr != None
        and not isinstance(expr, (Mapping, list))
        and hasattr(expr, "__call__")
    ):
        return expr
    return compile_expression(_jx_expression(expr, language).to_python())


class Variable(Variable_):
    def to_python(self, not_null=False, boolean=False, many=False):
        path = split_field(self.var)
        agg = "row"
        if not path:
            return agg
        elif path[0] in ["row", "rownum"]:
            # MAGIC VARIABLES
            agg = path[0]
            path = path[1:]
            if len(path) == 0:
                return agg
        elif path[0] == "rows":
            if len(path) == 1:
                return "rows"
            elif path[1] in ["first", "last"]:
                agg = "rows." + path[1] + "()"
                path = path[2:]
            else:
                Log.error("do not know what {{var}} of `rows` is", var=path[1])

        for p in path[:-1]:
            if not_null:
                agg = agg + ".get(" + convert.value2quote(p) + ")"
            else:
                agg = agg + ".get(" + convert.value2quote(p) + ", EMPTY_DICT)"
        output = agg + ".get(" + convert.value2quote(path[-1]) + ")"
        if many:
            output = "listwrap(" + output + ")"
        return output


class OffsetOp(OffsetOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return (
            "row["
            + text_type(self.var)
            + "] if 0<="
            + text_type(self.var)
            + "<len(row) else None"
        )


class RowsOp(RowsOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        agg = "rows[rownum+" + IntegerOp(self.offset).to_python() + "]"
        path = split_field(json2value(self.var.json))
        if not path:
            return agg

        for p in path[:-1]:
            agg = agg + ".get(" + convert.value2quote(p) + ", EMPTY_DICT)"
        return agg + ".get(" + convert.value2quote(path[-1]) + ")"


class IntegerOp(IntegerOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return "int(" + self.term.to_python() + ")"


class GetOp(GetOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        obj = self.var.to_python()
        code = self.offset.to_python()
        return "listwrap(" + obj + ")[" + code + "]"


class LastOp(LastOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        term = self.term.to_python()
        return "listwrap(" + term + ").last()"


class SelectOp(SelectOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return (
            "wrap_leaves({"
            + ",".join(
                quote(t["name"]) + ":" + t["value"].to_python() for t in self.terms
            )
            + "})"
        )


class ScriptOp(ScriptOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return self.script


class Literal(Literal_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return text_type(repr(unwrap(json2value(self.json))))


class NullOp(NullOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return "None"


class TrueOp(TrueOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return "True"


class FalseOp(FalseOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return "False"


class DateOp(DateOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return text_type(Date(self.value).unix)


class TupleOp(TupleOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        if len(self.terms) == 0:
            return "tuple()"
        elif len(self.terms) == 1:
            return "(" + self.terms[0].to_python() + ",)"
        else:
            return "(" + (",".join(t.to_python() for t in self.terms)) + ")"


class LeavesOp(LeavesOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return "Data(" + self.term.to_python() + ").leaves()"


class BaseBinaryOp(BaseBinaryOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return (
            "("
            + self.lhs.to_python()
            + ") "
            + _python_operators[self.op][0]
            + " ("
            + self.rhs.to_python()
            + ")"
        )


class BaseInequalityOp(BaseInequalityOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return (
            "("
            + self.lhs.to_python()
            + ") "
            + _python_operators[self.op][0]
            + " ("
            + self.rhs.to_python()
            + ")"
        )


class InOp(InOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return self.value.to_python() + " in " + self.superset.to_python(many=True)


class DivOp(DivOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        miss = self.missing().to_python()
        lhs = self.lhs.to_python(not_null=True)
        rhs = self.rhs.to_python(not_null=True)
        return "None if (" + miss + ") else (" + lhs + ") / (" + rhs + ")"


class FloorOp(FloorOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return "Math.floor(" + self.lhs.to_python() + ", " + self.rhs.to_python() + ")"


class EqOp(EqOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return (
            "(" + self.rhs.to_python() + ") in listwrap(" + self.lhs.to_python() + ")"
        )


class BasicEqOp(BasicEqOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return "(" + self.rhs.to_python() + ") == (" + self.lhs.to_python() + ")"


class NeOp(NeOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        lhs = self.lhs.to_python()
        rhs = self.rhs.to_python()
        return (
            "(("
            + lhs
            + ") != None and ("
            + rhs
            + ") != None and ("
            + lhs
            + ") != ("
            + rhs
            + "))"
        )


class NotOp(NotOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return "not (" + self.term.to_python(boolean=True) + ")"


class AndOp(AndOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        if not self.terms:
            return "True"
        else:
            return " and ".join("(" + t.to_python() + ")" for t in self.terms)


class OrOp(OrOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return " or ".join("(" + t.to_python() + ")" for t in self.terms)


class LengthOp(LengthOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        value = self.term.to_python()
        return "len(" + value + ") if (" + value + ") != None else None"


class FirstOp(FirstOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        value = self.term.to_python()
        return "listwrap(" + value + ").first()"


class NumberOp(NumberOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        test = self.term.missing().to_python(boolean=True)
        value = self.term.to_python(not_null=True)
        return "float(" + value + ") if (" + test + ") else None"


class StringOp(StringOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        missing = self.term.missing().to_python(boolean=True)
        value = self.term.to_python(not_null=True)
        return "null if (" + missing + ") else text_type(" + value + ")"


class CountOp(CountOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return "+".join(
            "(0 if (" + t.missing().to_python(boolean=True) + ") else 1)"
            for t in self.terms
        )


class MaxOp(MaxOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return "max([" + (",".join(t.to_python() for t in self.terms)) + "])"


class BaseMultiOp(BaseMultiOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        sign, zero = _python_operators[self.op]
        if len(self.terms) == 0:
            return self.default.to_python()
        elif self.default is NULL:
            return sign.join(
                "coalesce(" + t.to_python() + ", " + zero + ")" for t in self.terms
            )
        else:
            return (
                "coalesce("
                + sign.join("(" + t.to_python() + ")" for t in self.terms)
                + ", "
                + self.default.to_python()
                + ")"
            )


class RegExpOp(RegExpOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return (
            "re.match("
            + quote(json2value(self.pattern.json) + "$")
            + ", "
            + self.var.to_python()
            + ")"
        )


class CoalesceOp(CoalesceOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return "coalesce(" + (", ".join(t.to_python() for t in self.terms)) + ")"


class MissingOp(MissingOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return self.expr.to_python() + " == None"


class ExistsOp(ExistsOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return self.field.to_python() + " != None"


class PrefixOp(PrefixOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return (
            "("
            + self.expr.to_python()
            + ").startswith("
            + self.prefix.to_python()
            + ")"
        )


class SuffixOp(SuffixOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return (
            "(" + self.expr.to_python() + ").endswith(" + self.suffix.to_python() + ")"
        )


class ConcatOp(ConcatOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        v = self.value.to_python()
        l = self.length.to_python()
        return (
            "None if "
            + v
            + " == None or "
            + l
            + " == None else "
            + v
            + "[0:max(0, "
            + l
            + ")]"
        )


class NotLeftOp(NotLeftOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        v = self.value.to_python()
        l = self.length.to_python()
        return (
            "None if "
            + v
            + " == None or "
            + l
            + " == None else "
            + v
            + "[max(0, "
            + l
            + "):]"
        )


class RightOp(RightOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        v = self.value.to_python()
        l = self.length.to_python()
        return (
            "None if "
            + v
            + " == None or "
            + l
            + " == None else "
            + v
            + "[max(0, len("
            + v
            + ")-("
            + l
            + ")):]"
        )


class NotRightOp(NotRightOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        v = self.value.to_python()
        l = self.length.to_python()
        return (
            "None if "
            + v
            + " == None or "
            + l
            + " == None else "
            + v
            + "[0:max(0, len("
            + v
            + ")-("
            + l
            + "))]"
        )


class BasicIndexOfOp(BasicIndexOfOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        find = "(" + self.value.to_python() + ").find(" + self.find.to_python() + ", " + self.start.to_python() + ")"
        return "[None if i==-1 else i for i in [" + find + "]][0]"


class SplitOp(SplitOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return "(" + self.value.to_python() + ").split(" + self.find.to_python() + ")"


class FindOp(FindOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return (
            "(("
            + quote(self.substring)
            + " in "
            + self.var.to_python()
            + ") if "
            + self.var.to_python()
            + "!=None else False)"
        )


class BetweenOp(BetweenOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return self.value.to_python() + " in " + self.superset.to_python(many=True)


class RangeOp(RangeOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return (
            "("
            + self.then.to_python(not_null=not_null)
            + ") if ("
            + self.when.to_python(boolean=True)
            + ") else ("
            + self.els_.to_python(not_null=not_null)
            + ")"
        )


class CaseOp(CaseOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        acc = self.whens[-1].to_python()
        for w in reversed(self.whens[0:-1]):
            acc = (
                "("
                + w.then.to_python()
                + ") if ("
                + w.when.to_python(boolean=True)
                + ") else ("
                + acc
                + ")"
            )
        return acc


class WhenOp(WhenOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return (
            "("
            + self.then.to_python()
            + ") if ("
            + self.when.to_python(boolean=True)
            + ") else ("
            + self.els_.to_python()
            + ")"
        )


language = define_language("Python", vars())


_python_operators = {
    "add": (" + ", "0"),  # (operator, zero-array default value) PAIR
    "sum": (" + ", "0"),
    "mul": (" * ", "1"),
    "sub": (" - ", None),
    "div": (" / ", None),
    "exp": (" ** ", None),
    "mod": (" % ", None),
    "gt": (" > ", None),
    "gte": (" >= ", None),
    "lte": (" <= ", None),
    "lt": (" < ", None),
}
