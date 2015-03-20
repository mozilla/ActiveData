# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from __future__ import division

from pyLibrary.debugs.logs import Log
from pyLibrary.maths import Math


def qb_expression_to_ruby(expr):
    if isinstance(expr, unicode):
        return expr
    if Math.is_number(expr):
        return unicode(expr)
    if not expr:
        return "true"
    op, term = expr.items()[0]

    mop = multi_operators.get(op)
    if mop:
        if isinstance(term, list):
            if not term:
                return mop[1]  # RETURN DEFAULT
            else:
                output = mop[0].join(["(" + qb_expression_to_ruby(t) + ")" for t in term])
                return output
        elif isinstance(term, dict):
            a, b = term.items()[0]
            output = "(" + qb_expression_to_ruby(a) + ")" + mop[0] + "(" + qb_expression_to_ruby(b) + ")"
            return output
        else:
            qb_expression_to_ruby(term)


    bop = binary_operators.get(op)
    if bop:
        if isinstance(term, list):
            output = bop.join(["(" + qb_expression_to_ruby(t) + ")" for t in term])
            return output
        elif isinstance(term, dict):
            if op == "eq":
                # eq CAN ACCEPT A WHOLE OBJECT OF key:value PAIRS TO COMPARE
                output = " and ".join("(" + qb_expression_to_ruby(a) + ")" + bop + "(" + qb_expression_to_ruby(b) + ")" for a, b in term.items())
                return output
            else:
                a, b = term.items()[0]
                output = "(" + qb_expression_to_ruby(a) + ")" + bop + "(" + qb_expression_to_ruby(b) + ")"
                return output
        else:
            Log.error("Expecting binary term")

    uop = unary_operators.get(op)
    if uop:
        output = uop+"("+qb_expression_to_ruby(term)+")"
        return output

    Log.error("`{{op}}` is not a recognized operation", {"op": op})


unary_operators = {
    "not": " not "
}

binary_operators = {
    "sub": " - ",
    "subtract": " - ",
    "minus": " - ",
    "div": " / ",
    "divide": " / ",
    "exp": " ** ",
    "mod": " % ",
    "gt": " > ",
    "gte": " >= ",
    "eq": " == ",
    "lte": " <= ",
    "lt": " < ",
    "ne": " != "
}

multi_operators = {
    "add": (" + ", "0"),  # (operator, zero-array default value) PAIR
    "sum": (" + ", "0"),
    "mul": (" * ", "1"),
    "mult": (" * ", "1"),
    "multiply": (" * ", "1"),
    "and": (" and ", "true"),
    "or": (" or ", "false")
}
