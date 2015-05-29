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
from __future__ import absolute_import
from collections import Mapping
import itertools

from pyLibrary import convert
from pyLibrary.collections import OR
from pyLibrary.dot import coalesce, wrap, set_default, literal_field
from pyLibrary.debugs.logs import Log
from pyLibrary.maths import Math
from pyLibrary.queries.domains import is_keyword
from pyLibrary.strings import expand_template
from pyLibrary.times.dates import Date

TRUE_FILTER = True
FALSE_FILTER = False



def compile_expression(source):
    # FORCE MODULES TO BE IN NAMESPACE
    _ = coalesce
    _ = Date
    _ = convert

    output = None
    exec """
def output(row, rownum=None, rows=None):
    try:
        return """ + source + """
    except Exception, e:
        Log.error("Problem with dynamic function {{func|quote}}",  func= """ + convert.value2quote(source) + """, cause=e)
"""
    return output


def qb_expression(expr):
    """
    WRAP A QB EXPRESSION WITH OBJECT REPRESENTATION (OF DUBIOUS VALUE)
    """
    op, term = expr.items()[0]
    return complex_operators[op](op, term)


def qb_expression_to_function(expr):
    if expr!=None and not isinstance(expr, (Mapping, list)) and  hasattr(expr, "__call__"):
        return expr
    return compile_expression(qb_expression_to_python(expr))


def qb_expression_to_esfilter(expr):
    """
    CONVERT qb QUERY where CLAUSE TO ELASTICSEARCH FILTER FORMAT
    """
    if expr is True or expr == None:
        return {"match_all": {}}
    if expr is False:
        return False

    k, v = expr.items()[0]
    return converter_map.get(k, _no_convert)(k, v)


def qb_expression_to_ruby(expr):
    if expr == None:
        return "nil"
    elif Math.is_number(expr):
        return unicode(expr)
    elif is_keyword(expr):
        return "doc[" + convert.string2quote(expr) + "].value"
    elif isinstance(expr, basestring):
        Log.error("{{name|quote}} is not a valid variable name", name=expr)
    elif isinstance(expr, CODE):
        return expr.code
    elif isinstance(expr, Date):
        return unicode(expr.unix)
    elif expr is True:
        return "true"
    elif expr is False:
        return "false"

    op, term = expr.items()[0]

    mop = ruby_multi_operators.get(op)
    if mop:
        if isinstance(term, list):
            if not term:
                return mop[1]  # RETURN DEFAULT
            else:
                output = mop[0].join(["(" + qb_expression_to_ruby(t) + ")" for t in term])
                return output
        elif isinstance(term, Mapping):
            a, b = term.items()[0]
            output = "(" + qb_expression_to_ruby(a) + ")" + mop[0] + "(" + qb_expression_to_ruby(b) + ")"
            return output
        else:
            qb_expression_to_ruby(term)


    bop = ruby_binary_operators.get(op)
    if bop:
        if isinstance(term, list):
            output = bop.join(["(" + qb_expression_to_ruby(t) + ")" for t in term])
            return output
        elif isinstance(term, Mapping):
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

    uop = ruby_unary_operators.get(op)
    if uop:
        output = expand_template(uop, {"term": qb_expression_to_ruby(term)})
        return output

    cop = complex_operators.get(op)
    if cop:
        output = cop(term).to_ruby()
        return output

    Log.error("`{{op}}` is not a recognized operation",  op= op)


def qb_expression_to_python(expr):
    if expr == None:
        return "None"
    elif Math.is_number(expr):
        return unicode(expr)
    elif isinstance(expr, Date):
        return unicode(expr.unix)
    elif isinstance(expr, unicode):
        if expr == ".":
            return "row"
        elif is_keyword(expr):
            return "row[" + convert.value2quote(expr) + "]"
        else:
            Log.error("Expecting a json path")
    elif isinstance(expr, CODE):
        return expr.code
    elif expr is True:
        return "True"
    elif expr is False:
        return "False"

    op, term = expr.items()[0]

    mop = python_multi_operators.get(op)
    if mop:
        if isinstance(term, list):
            if not term:
                return mop[1]  # RETURN DEFAULT
            else:
                output = mop[0].join(["(" + qb_expression_to_python(t) + ")" for t in term])
                return output
        elif isinstance(term, Mapping):
            a, b = term.items()[0]
            output = "(" + qb_expression_to_python(a) + ")" + mop[0] + "(" + qb_expression_to_python(b) + ")"
            return output
        else:
            qb_expression_to_python(term)

    bop = python_binary_operators.get(op)
    if bop:
        if isinstance(term, list):
            output = bop.join(["(" + qb_expression_to_python(t) + ")" for t in term])
            return output
        elif isinstance(term, Mapping):
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

    uop = python_unary_operators.get(op)
    if uop:
        output = uop + "(" + qb_expression_to_python(term) + ")"
        return output

    Log.error("`{{op}}` is not a recognized operation",  op= op)


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

    mop = ruby_multi_operators.get(op)
    if mop:
        if isinstance(term, list):
            output = set()
            for t in term:
                output |= get_all_vars(t)
            return output
        elif isinstance(term, Mapping):
            a, b = term.items()[0]
            return get_all_vars(a) | get_all_vars(b)
        else:
            get_all_vars(term)

    bop = ruby_binary_operators.get(op)
    if bop:
        if isinstance(term, list):
            output = set()
            for t in term:
                output |= get_all_vars(t)
            return output
        elif isinstance(term, Mapping):
            if op == "eq":
                output = set()
                for a, b in term.items():
                    output |= get_all_vars(a)  # {k:v} k IS VARIABLE, v IS A VALUE
                return output
            else:
                a, b = term.items()[0]
                return get_all_vars(a)
        else:
            Log.error("Expecting binary term")

    uop = ruby_unary_operators.get(op)
    if uop:
        return get_all_vars(term)

    cop = complex_operators.get(op)
    if cop:
        return cop(op, term).vars()

    Log.error("`{{op}}` is not a recognized operation",  op= op)



python_unary_operators = {
    "not": "not {{term}}",
}

python_binary_operators = {
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

python_multi_operators = {
    "add": (" + ", "0"),  # (operator, zero-array default value) PAIR
    "sum": (" + ", "0"),
    "mul": (" * ", "1"),
    "mult": (" * ", "1"),
    "multiply": (" * ", "1"),
    "and": (" and ", "true"),
    "or": (" or ", "false")
}

ruby_unary_operators = {
    "not": "! {{term}}",
}

ruby_binary_operators = {
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

ruby_multi_operators = {
    "add": (" + ", "0"),  # (operator, zero-array default value) PAIR
    "sum": (" + ", "0"),
    "mul": (" * ", "1"),
    "mult": (" * ", "1"),
    "multiply": (" * ", "1"),
    "and": (" && ", "true"),
    "or": (" || ", "false")
}

default_multi_operators = {
    "add": 0,  # (operator, zero-array default value) PAIR
    "sum": 0,
    "mul": 1,
    "mult": 1,
    "multiply": 1,
    "and": True,
    "or": False
}






class BinaryOp(object):
    def __init__(self, op, term):
        self.op = op
        if isinstance(term, list):
            self.a, self.b = qb_expression(term[0]), qb_expression(term[1])
        elif isinstance(term, Mapping):
            self.a, self.b = map(qb_expression, term.items()[0])

    def to_ruby(self):
        symbol = ruby_multi_operators[self.op][0]
        return "(" + self.a.to_ruby() + ")" + symbol + "(" + self.b.to_ruby() + ")"

    def to_python(self):
        symbol = python_multi_operators[self.op][0]
        return "(" + self.a.to_python() + ")" + symbol + "(" + self.b.to_python() + ")"

    def to_esfilter(self):
        if self.op in ["gt", "gte", "lte", "lt"]:
            return {"range":{self.op: {self.a: self.b}}}
        else:
            Log.error("Operator {{op}} is not supported by ES",  op=self.op)

    def vars(self):
        return self.a.vars() | self.b.vars()

class MultiOp(object):
    def __init__(self, op, terms):
        self.op = op
        if isinstance(terms, list):
            if not terms:
                self.terms = [default_multi_operators[op]]
            else:
                self.terms = map(qb_expression, terms)
        elif isinstance(terms, Mapping):
            self.terms = map(qb_expression, terms.items()[0])
        else:
            self.terms = [qb_expression_to_python(terms)]

    def to_ruby(self):
        symbol = ruby_multi_operators[self.op][0]
        return symbol.join("(" + t.to_ruby() + ")" for t in self.terms)

    def to_python(self):
        symbol = python_multi_operators[self.op][0]
        return symbol.join("(" + t.to_python() + ")" for t in self.terms)

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output


class TermsOp(object):
    def __init__(self, op, term):
        self.var, self.vals = term.items()[0]

    def to_ruby(self):
        return "[" + (",".join(map(convert.value2quote, self.vals))) + "].include?(" + qb_expression_to_ruby(self.var) + ")"

    def to_python(self):
        return qb_expression_to_python(self.var) + " in [" + (",".join(map(convert.value2quote, self.vals))) + "]"

    def to_esfilter(self):
        return {"terms": {self.var: self.vals}}

    def vars(self):
        return set([self.var])


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
        return set([self.field])


class PrefixOp(object):
    def __init__(self, op, term):
        self.field, self.prefix = term.items()[0]

    def to_ruby(self):
        return qb_expression_to_ruby(self.field)+".start_with? "+convert.string2quote(self.prefix)

    def to_python(self):
        return qb_expression_to_python(self.field)+".startswith("+convert.string2quote(self.prefix)+")"

    def to_esfilter(self):
        return {"prefix": {self.field: self.prefix}}

    def vars(self):
        return set([self.field])


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
        return set([self.field])

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


class RangeOp(object):
    def __init__(self, op, term):
        self.field, self.cmp = term.items()[0]

    def to_ruby(self):
        return " and ".join(qb_expression_to_ruby([{o: {self.field: v}} for o, v in self.cmp.items()]))

    def to_python(self):
        return " and ".join(qb_expression_to_python([{o: {self.field: v}} for o, v in self.cmp.items()]))

    def to_esfilter(self):
        return {"range": {self.field, self.cmp}}

    def vars(self):
        return set([self.field])




complex_operators = {
    "terms": TermsOp,
    "exists": ExistsOp,
    "missing": MissingOp,
    "prefix": PrefixOp,
    "range": RangeOp
}






def simplify_esfilter(esfilter):
    try:
        output = normalize_esfilter(qb_expression_to_esfilter(esfilter))
        if output is TRUE_FILTER:
            return {"match_all": {}}
        output.isNormal = None
        return output
    except Exception, e:
        from pyLibrary.debugs.logs import Log

        Log.unexpected("programmer error", e)



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
    TODO: DO NOT USE Dicts, WE ARE SPENDING TOO MUCH TIME WRAPPING/UNWRAPPING
    REALLY, WE JUST COLLAPSE CASCADING `and` AND `or` FILTERS
    """
    if esfilter is TRUE_FILTER or esfilter is FALSE_FILTER or esfilter.isNormal:
        return esfilter

    # Log.note("from: " + convert.value2json(esfilter))
    isDiff = True

    while isDiff:
        isDiff = False

        if esfilter["and"] != None:
            terms = esfilter["and"]
            # MERGE range FILTER WITH SAME FIELD
            for (i0, t0), (i1, t1) in itertools.product(enumerate(terms), enumerate(terms)):
                if i0 >= i1:
                    continue  # SAME, IGNORE
                try:
                    f0, tt0 = t0.range.items()[0]
                    f1, tt1 = t1.range.items()[0]
                    if f0 == f1:
                        set_default(terms[i0].range[literal_field(f1)], tt1)
                        terms[i1] = True
                except Exception, e:
                    pass


            output = []
            for a in terms:
                if isinstance(a, (list, set)):
                    from pyLibrary.debugs.logs import Log
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
                if a.get("and"):
                    isDiff = True
                    a.isNormal = None
                    output.extend(a.get("and"))
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


def _convert_many(k, v):
    return {k: [qb_expression_to_esfilter(vv) for vv in v]}


def _convert_not(k, v):
    return {k: qb_expression_to_esfilter(v)}


def _convert_not_equal(op, term):
    if isinstance(term, list):
        Log.error("the 'ne' clause does not accept a list parameter")

    var, val = term.items()[0]
    if isinstance(val, list):
        return {"not": {"terms": term}}
    else:
        return {"not": {"term": term}}


def _convert_eq(eq, term):
    if not term:
        return {"match_all":{}}

    if isinstance(term, list):
        if len(term) != 2:
            Log.error("the 'eq' clause only accepts list of length 2")

        output = {"script": {"script" : qb_expression_to_ruby({"eq":term})}}
        return output

    def _convert(k, v):
        if isinstance(v, list):
            return {"terms": {k: v}}
        else:
            return {"term": {k: v}}

    items = term.items()
    if len(items) > 1:
        return {"and": [_convert(k, v) for k, v in items]}
    else:
        return _convert(*items[0])


def _convert_in(op, term):
    if not term:
        Log.error("Expecting a term")
    if not isinstance(term, Mapping):
        Log.error("Expecting {{op}} to have dict value",  op= op)
    var, val = term.items()[0]

    if isinstance(val, list):
        v2 = [vv for vv in val if vv != None]

        if len(v2) == 0:
            if len(val) == 0:
                return False
            else:
                return {"missing": {"field": var}}

        if len(v2) == 1:
            output = {"term": {var: v2[0]}}
        else:
            output = {"terms": {var: v2}}

        if len(v2) != len(val):
            output = {"or": [
                {"missing": {"field": var}},
                output
            ]}
        return output
    else:
        return {"term": term}


def _convert_inequality(ine, term):
    var, val = term.items()[0]
    return {"range": {var: {ine: val}}}


def _no_convert(op, term):
    return {op: term}


def _convert_field(k, var):
    if isinstance(var, basestring):
        return {k: {"field": var}}
    if isinstance(var, Mapping) and var.get("field"):
        return {k: var}
    Log.error("do not know how to handle {{value}}",  value= {k: var})


converter_map = {
    "and": _convert_many,
    "or": _convert_many,
    "not": _convert_not,
    "term": _convert_in,
    "terms": _convert_in,
    "eq": _convert_eq,
    "ne": _convert_not_equal,
    "neq": _convert_not_equal,
    "in": _convert_in,
    "missing": _convert_field,
    "exists": _convert_field,
    "gt": _convert_inequality,
    "gte": _convert_inequality,
    "lt": _convert_inequality,
    "lte": _convert_inequality
}


class CODE(object):
    """
    WRAP SAFE CODE
    DO NOT USE ON UNKNOWN SOURCES, OTHERWISE YOU GET REMOTE CODE EXPLOITS
    """
    def __init__(self, code):
        self.code = code
