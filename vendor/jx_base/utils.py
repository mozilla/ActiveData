# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals


def first(values):
    return iter(values).next()


# EVERY OPERATOR WILL HAVE lang WHICH POINTS TO LANGUAGE
class LanguageElement(type):
    def __new__(cls, name, bases, dct):
        x = type.__new__(cls, name, bases, dct)
        x.lang = None
        return x


BaseExpression = LanguageElement(str("BaseExpression"), (object,), {})


class Language(object):
    pass


def define_language(lang_name, module_vars):
    # LET ALL EXPRESSIONS POINT TO lang OBJECT WITH ALL EXPRESSIONS
    # ENSURE THIS IS BELOW ALL SUB_CLASS DEFINITIONS SO var() CAPTURES ALL EXPRESSIONS
    language = Language if not lang_name else type(str(lang_name), (Language,), {})
    for name, obj in module_vars.items():
        if isinstance(obj, type) and issubclass(obj, BaseExpression):
            setattr(language, name, obj)
            curr = getattr(obj, "lang")
            if not curr:
                setattr(obj, "lang", language)

    return language

operators = {
    "add": "AddOp",
    "and": "AndOp",
    "basic.add": "BasicAddOp",
    "basic.mul": "BasicMulOp",
    "between": "BetweenOp",
    "case": "CaseOp",
    "coalesce": "CoalesceOp",
    "concat": "ConcatOp",
    "count": "CountOp",
    "date": "DateOp",
    "div": "DivOp",
    "divide": "DivOp",
    "eq": "EqOp",
    "exists": "ExistsOp",
    "exp": "ExpOp",
    "find": "FindOp",
    "first": "FirstOp",
    "floor": "FloorOp",
    "from_unix": "FromUnixOp",
    "get": "GetOp",
    "gt": "GtOp",
    "gte": "GteOp",
    "in": "InOp",
    "instr": "FindOp",
    "is_number": "IsNumberOp",
    "is_string": "IsStringOp",
    "last": "LastOp",
    "left": "LeftOp",
    "length": "LengthOp",
    "literal": "Literal",
    "lt": "LtOp",
    "lte": "LteOp",
    "match_all": "TrueOp",
    "max": "MaxOp",
    "minus": "SubOp",
    "missing": "MissingOp",
    "mod": "ModOp",
    "mul": "MulOp",
    "mult": "MulOp",
    "multiply": "MulOp",
    "ne": "NeOp",
    "neq": "NeOp",
    "not": "NotOp",
    "not_left": "NotLeftOp",
    "not_right": "NotRightOp",
    "null": "NullOp",
    "number": "NumberOp",
    "offset": "OffsetOp",
    "or": "OrOp",
    "postfix": "SuffixOp",
    "prefix": "PrefixOp",
    "range": "RangeOp",
    "regex": "RegExpOp",
    "regexp": "RegExpOp",
    "right": "RightOp",
    "rows": "RowsOp",
    "script": "ScriptOp",
    "select": "SelectOp",
    "split": "SplitOp",
    "string": "StringOp",
    "suffix": "SuffixOp",
    "sub": "SubOp",
    "subtract": "SubOp",
    "sum": "AddOp",
    "term": "EqOp",
    "terms": "InOp",
    "tuple": "TupleOp",
    "union": "UnionOp",
    "unix": "UnixOp",
    "when": "WhenOp",
}
