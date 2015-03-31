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

from pyLibrary import convert
from pyLibrary.dot import nvl
from pyLibrary.debugs.logs import Log
from pyLibrary.maths import Math
from pyLibrary.queries.domains import is_keyword
from pyLibrary.strings import expand_template
from pyLibrary.times.dates import Date

TRUE_FILTER = True
FALSE_FILTER = False



def compile_expression(source):
    # FORCE MODULES TO BE IN NAMESPACE
    _ = nvl
    _ = Date

    output = None
    exec """
def output(row, rownum=None, rows=None):
    try:
        return """ + source + """
    except Exception, e:
        Log.error("Problem with dynamic function {{func|quote}}", {"func": """ + convert.value2quote(source) + """})
"""
    return output


def qb_expression(expr):
    """
    WRAP A QB EXPRESSION WITH OBJECT REPRESENTATION (OF DUBIOUS VALUE)
    """
    op, term = expr.items()[0]
    return complex_operators[op](op, term)

def qb_expression_to_function(expr):
    return compile_expression(qb_expression_to_python(expr))

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
        output = expand_template(uop, {"term": qb_expression_to_ruby(term)})
        return output

    cop = complex_operators.get(op)
    if cop:
        output = cop(term).to_ruby()
        return output

    Log.error("`{{op}}` is not a recognized operation", {"op": op})


def qb_expression_to_python(expr):
    if expr == None:
        return "True"
    elif isinstance(expr, unicode):
        if expr == ".":
            return "row"
        elif is_keyword(expr):
            return "row[" + convert.value2quote(expr) + "]"
        else:
            Log.error("Expecting a json path")
    elif expr is True:
        return "True"
    elif expr is False:
        return "False"
    elif Math.is_number(expr):
        return unicode(expr)

    op, term = expr.items()[0]

    mop = multi_operators.get(op)
    if mop:
        if isinstance(term, list):
            if not term:
                return mop[1]  # RETURN DEFAULT
            else:
                output = mop[0].join(["(" + qb_expression_to_python(t) + ")" for t in term])
                return output
        elif isinstance(term, dict):
            a, b = term.items()[0]
            output = "(" + qb_expression_to_python(a) + ")" + mop[0] + "(" + qb_expression_to_python(b) + ")"
            return output
        else:
            qb_expression_to_python(term)

    bop = binary_operators.get(op)
    if bop:
        if isinstance(term, list):
            output = bop.join(["(" + qb_expression_to_python(t) + ")" for t in term])
            return output
        elif isinstance(term, dict):
            if op == "eq":
                # eq CAN ACCEPT A WHOLE OBJECT OF key:value PAIRS TO COMPARE
                output = " and ".join("(" + qb_expression_to_python(a) + ")" + bop + "(" + qb_expression_to_python(b) + ")" for a, b in term.items())
                return output
            else:
                a, b = term.items()[0]
                output = "(" + qb_expression_to_python(a) + ")" + bop + "(" + qb_expression_to_python(b) + ")"
                return output
        else:
            Log.error("Expecting binary term")

    uop = unary_operators.get(op)
    if uop:
        output = uop + "(" + qb_expression_to_python(term) + ")"
        return output

    Log.error("`{{op}}` is not a recognized operation", {"op": op})

def get_all_vars(expr):
    if expr == None:
        return set()
    elif isinstance(expr, unicode):
        if expr == "." or is_keyword(expr):
            return set([expr])
        else:
            Log.error("Expecting a json path")
    elif expr is True:
        return set()
    elif expr is False:
        return set()
    elif Math.is_number(expr):
        return set()

    op, term = expr.items()[0]

    mop = multi_operators.get(op)
    if mop:
        if isinstance(term, list):
            output = set()
            for t in term:
                output |= get_all_vars(t)
            return output
        elif isinstance(term, dict):
            a, b = term.items()[0]
            return get_all_vars(a) | get_all_vars(b)
        else:
            get_all_vars(term)

    bop = binary_operators.get(op)
    if bop:
        if isinstance(term, list):
            output = set()
            for t in term:
                output |= get_all_vars(t)
            return output
        elif isinstance(term, dict):
            if op == "eq":
                output = set()
                for a, b in term.items():
                    output |= get_all_vars(a) | get_all_vars(b)
                return output
            else:
                a, b = term.items()[0]
                return get_all_vars(a) | get_all_vars(b)
        else:
            Log.error("Expecting binary term")

    uop = unary_operators.get(op)
    if uop:
        return get_all_vars(term)

    Log.error("`{{op}}` is not a recognized operation", {"op": op})



unary_operators = {
    "not": "not {{term}}"
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
    "ne": " != ",
    "term": " == "
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

class BinaryOp(object):
    def __init__(self, op, term):
        self.op = op
        self.symbol = binary_operators[op]
        if isinstance(term, list):
            self.a, self.b = qb_expression(term[0]), qb_expression(term[1])
        elif isinstance(term, dict):
            self.a, self.b = map(qb_expression, term.items()[0])

    def to_ruby(self):
        return "(" + self.a.to_ruby() + ")" + self.symbol + "(" + self.b.to_ruby() + ")"

    def to_python(self):
        return "(" + self.a.to_python() + ")" + self.symbol + "(" + self.b.to_python() + ")"

    def to_esfilter(self):
        if self.op in ["gt", "gte", "lte", "lt"]:
            return {"range":{self.op: {self.a: self.b}}}
        else:
            Log.error("Operator {{op}} is not supported by ES", {"op":self.op})

    def vars(self):
        return self.a.vars() | self.b.vars()

class MultiOp(object):
    def __init__(self, op, terms):
        self.op = op
        self.symbol = multi_operators[op][0]
        if isinstance(terms, list):
            if not terms:
                self.terms = [qb_expression(multi_operators[op][1])]
            else:
                self.terms = map(qb_expression, terms)
        elif isinstance(terms, dict):
            self.terms = map(qb_expression, terms.items()[0])
        else:
            self.terms = [qb_expression_to_python(terms)]

    def to_ruby(self):
        return self.symbol.join("(" + t.to_ruby() + ")" for t in self.terms)

    def to_python(self):
        return self.symbol.join("(" + t.to_python() + ")" for t in self.terms)

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output


class TermsOp(object):
    def __init__(self, op, term):
        self.var, self.vals = term.items()[0]

    def to_ruby(self):
        return "[" + (",".join(map(convert.value2quote, self.vals))) + "].include?(" + self.var.to_ruby + ")"

    def to_python(self):
        return self.var.to_python() + " in [" + (",".join(map(convert.value2quote, self.vals))) + "]"

    def to_esfilter(self):
        return {"terms": {self.var: self.vals}}

    def vars(self):
        return set(self.var)


class ExistsOp(object):
    def __init__(self, op, term):
        if isinstance(term, basestring):
            self.field = term
        else:
            self.field = term.field

    def to_ruby(self):
        return "!"+qb_expression_to_ruby(self.field)+".nil?"

    def to_python(self):
        return qb_expression_to_python(self.field)+" != None"

    def to_esfilter(self):
        return {"exists": {"field": self.field}}

    def vars(self):
        return set(self.field)


class MissingOp(object):
    def __init__(self, op, term):
        if isinstance(term, basestring):
            self.field = term
        else:
            self.field = term.field

    def to_ruby(self):
        return qb_expression_to_ruby(self.field)+".nil?"

    def to_python(self):
        return qb_expression_to_python(self.field)+" == None"

    def to_esfilter(self):
        return {"missing": {"field": self.field}}

    def vars(self):
        return set(self.field)

class NotOp(object):
    def __init__(self, op, term):
        self.term = qb_expression(term)

    def to_ruby(self):
        return "not " + self.term.to_ruby()

    def to_python(self):
        return "not" + self.term.to_python()

    def to_esfilter(self):
        return {"not": self.term.to_esfilter()}

    def vars(self):
        return self.term.vars()




complex_operators = {
    "terms": TermsOp,
    "exists": ExistsOp,
    "missing": MissingOp
}

