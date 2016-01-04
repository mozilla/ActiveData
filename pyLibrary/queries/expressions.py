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
from decimal import Decimal
import itertools

from pyLibrary import convert
from pyLibrary.collections import OR, MAX, UNION
from pyLibrary.dot import coalesce, wrap, set_default, literal_field, listwrap, Null, Dict
from pyLibrary.debugs.logs import Log
from pyLibrary.maths import Math
from pyLibrary.queries.domains import is_keyword
from pyLibrary.times.dates import Date


ALLOW_SCRIPTING = False
TRUE_FILTER = True
FALSE_FILTER = False

_Query = None


def _late_import():
    global _Query

    from pyLibrary.queries.query import Query as _Query

    _ = _Query


def qb_expression(expr):
    """
    WRAP A QB EXPRESSION WITH OBJECT REPRESENTATION
    """
    if expr in (True, False, None) or expr == None or isinstance(expr, (float, int, Decimal)) or isinstance(expr, Date):
        return Literal(None, expr)
    elif is_keyword(expr):
        return Variable(expr)

    try:
        items = expr.items()
    except Exception, e:
        Log.error("programmer error expr = {{value|quote}}", value=expr, cause=e)
    op, term = items[0]

    if len(items) == 1:
        class_ = operators.get(op)
        if not class_:
            Log.error("{{operator|quote}} is not a known operator", operator=op)
        clauses = {}
    else:
        for item in items:
            op, term = item
            class_ = operators.get(op)
            if class_:
                clauses = {k: qb_expression(v) for k, v in expr.items() if k != op}
                break
        else:
            raise Log.error("{{operator|quote}} is not a known operator", operator=op)

    if class_ is Literal:
        return class_(op, term)
    elif class_ is ScriptOp:
        if ALLOW_SCRIPTING:
            Log.warning("Scripting has been activated:  This has known security holes!!\nscript = {{script||quote}}", script=term)
            return class_(op, term)
        else:
            Log.error("scripting is disabled")
    elif term == None:
        return class_(op, [], **clauses)
    elif isinstance(term, list):
        terms = map(qb_expression, term)
        return class_(op, terms, **clauses)
    elif isinstance(term, Mapping):
        items = term.items()
        if class_.has_simple_form:
            if len(items) == 1:
                k, v = items[0]
                return class_(op, [Variable(k), Literal(None, v)], **clauses)
            else:
                return class_(op, {k: Literal(None, v) for k, v in items}, **clauses)
        else:
            return class_(op, qb_expression(term), **clauses)
    else:
        return class_(op, qb_expression(term), **clauses)


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


def qb_expression_to_function(expr):
    """
    RETURN FUNCTION THAT REQUIRES PARAMETERS (row, rownum=None, rows=None):
    """
    if expr != None and not isinstance(expr, (Mapping, list)) and hasattr(expr, "__call__"):
        return expr
    return compile_expression(qb_expression(expr).to_python())


def query_get_all_vars(query, exclude_where=False):
    """
    :param query:
    :param exclude_where: Sometimes we do not what to look at the where clause
    :return: all variables in use by query
    """
    output = set()
    for s in listwrap(query.select):
        output |= select_get_all_vars(s)
    for s in listwrap(query.edges):
        output |= edges_get_all_vars(s)
    for s in listwrap(query.groupby):
        output |= edges_get_all_vars(s)
    if not exclude_where:
        output |= qb_expression(query.where).vars()
    return output


def select_get_all_vars(s):
    if isinstance(s.value, list):
        return set(s.value)
    elif isinstance(s.value, basestring):
        return {s.value}
    elif s.value == None or s.value == ".":
        return set()
    else:
        if s.value == "*":
            return {"*"}
        return qb_expression(s.value).vars()


def edges_get_all_vars(e):
    output = set()
    if isinstance(e.value, basestring):
        output.add(e.value)
    if e.domain.key:
        output.add(e.domain.key)
    if e.domain.where:
        output |= qb_expression(e.domain.where).vars()
    if e.domain.partitions:
        for p in e.domain.partitions:
            if p.where:
                output |= qb_expression(p.where).vars()
    return output


class Expression(object):
    has_simple_form = False


    def __init__(self, op, terms):
        if isinstance(terms, (list, tuple)):
            if not all(isinstance(t, Expression) for t in terms):
                Log.error("Expecting an expression")
        elif isinstance(terms, Mapping):
            if not all(isinstance(k, Variable) and isinstance(v, Literal) for k, v in terms.items()):
                Log.error("Expecting an {<variable>: <literal}")
        elif terms == None:
            pass
        else:
            if not isinstance(terms, Expression):
                Log.error("Expecting an expression")

    def to_ruby(self):
        raise NotImplementedError

    def to_python(self):
        raise NotImplementedError

    def to_esfilter(self):
        raise NotImplementedError

    def to_dict(self):
        raise NotImplementedError

    def __json__(self):
        return convert.value2json(self.to_dict())

    def vars(self):
        raise NotImplementedError

    def map(self, map):
        raise NotImplementedError

    def missing(self):
        # RETURN FILTER THAT INDICATE THIS EXPRESSIOn RETURNS null
        raise NotImplementedError

    def exists(self):
        return NotOp("not", self.missing())


class Variable(Expression):

    def __init__(self, var):
        Expression.__init__(self, "", None)
        if not is_keyword(var):
            Log.error("Expecting a variable")
        self.var = var

    def to_ruby(self):
        if self.var == ".":
            return "_source"
        else:
            q = convert.string2quote(self.var)
            return "(doc[" + q + "].isEmpty() ? null : doc[" + q + "].value)"

    def to_python(self):
        if self.var == ".":
            return "row"
        else:
            return "row[" + convert.value2quote(self.var) + "]"

    def to_dict(self):
        return self.var

    def vars(self):
        return {self.var}

    def map(self, map_):
        if not isinstance(map_, Mapping):
            Log.error("Expecting Mapping")

        return Variable(coalesce(map_.get(self.var), self.var))

    def missing(self):
        # RETURN FILTER THAT INDICATE THIS EXPRESSION RETURNS null
        return MissingOp("missing", self)

    def exists(self):
        return ExistsOp("exists", self)

    def __hash__(self):
        return self.var.__hash__()

    def __eq__(self, other):
        return self.var.__eq__(other)

    def __unicode__(self):
        return self.var

    def __str__(self):
        return str(self.var)


class ScriptOp(Expression):

    def __init__(self, op, script):
        Expression.__init__(self, "", None)
        self.script = script

    def to_ruby(self):
        return self.script

    def vars(self):
        return set()

    def map(self, map_):
        return self

    def __unicode__(self):
        return self.script

    def __str__(self):
        return str(self.script)


class Literal(Expression):
    """
    A literal JSON document
    """

    def __new__(cls, op, term):
        if term == None:
            return NullOp()
        if term is True:
            return TrueOp()
        if term is False:
            return FalseOp()
        return object.__new__(cls, op, term)

    def __init__(self, op, term):
        Expression.__init__(self, "", None)
        self.json = convert.value2json(term)

    def __nonzero__(self):
        return True

    def __eq__(self, other):
        Log.warning("expensive")

        from pyLibrary.testing.fuzzytestcase import assertAlmostEqual

        try:
            assertAlmostEqual(convert.json2value(self.json), other)
            return True
        except Exception:
            return False

    def to_ruby(self):
        def _convert(v):
            if v is None:
                return "null"
            if v is True:
                return "true"
            if v is False:
                return "false"
            if isinstance(v, basestring):
                return convert.string2quote(v)
            if isinstance(v, (int, long, float)):
                return unicode(v)
            if isinstance(v, dict):
                return "[" + ", ".join(convert.string2quote(k) + ": " + _convert(vv) for k, vv in v.items()) + "]"
            if isinstance(v, list):
                return "[" + ", ".join(_convert(vv) for vv in v) + "]"

        return _convert(convert.json_decoder(self.json))

    def to_python(self):
        return self.json

    def to_esfilter(self):
        return convert.json2value(self.json)

    def to_dict(self):
        return {"literal": convert.json2value(self.json)}

    def vars(self):
        return set()

    def map(self, map_):
        return self

    def missing(self):
        return FalseOp()

    def __unicode__(self):
        return self.json

    def __str__(self):
        return str(self.json)


class NullOp(Literal):

    def __new__(cls, *args, **kwargs):
        return object.__new__(cls, *args, **kwargs)

    def __init__(self, op=None, term=None):
        Literal.__init__(self, op, None)

    def __nonzero__(self):
        return False

    def __eq__(self, other):
        return other == None

    def to_ruby(self):
        return "null"

    def to_python(self):
        return "None"

    def to_esfilter(self):
        return {"not": {"match_all": {}}}

    def to_dict(self):
        return {"null": {}}

    def vars(self):
        return set()

    def map(self, map_):
        return self

    def missing(self):
        return TrueOp()

    def exists(self):
        return FalseOp()

    def __unicode__(self):
        return "null"

    def __str__(self):
        return b"null"


class TrueOp(Literal):
    def __new__(cls, *args, **kwargs):
        return object.__new__(cls, *args, **kwargs)

    def __init__(self, op=None, term=None):
        Literal.__init__(self, op, True)

    def __nonzero__(self):
        return True

    def __eq__(self, other):
        return other == True

    def to_ruby(self):
        return "true"

    def to_python(self):
        return "True"

    def to_esfilter(self):
        return {"match_all": {}}

    def to_dict(self):
        return True

    def vars(self):
        return set()

    def map(self, map_):
        return self

    def missing(self):
        return FalseOp()

    def __unicode__(self):
        return "true"

    def __str__(self):
        return b"true"


class FalseOp(Literal):
    def __new__(cls, *args, **kwargs):
        return object.__new__(cls, *args, **kwargs)

    def __init__(self, op=None, term=None):
        Literal.__init__(self, op, False)

    def __nonzero__(self):
        return True

    def __eq__(self, other):
        return other == False

    def to_ruby(self):
        return "false"

    def to_python(self):
        return "False"

    def to_esfilter(self):
        return {"not": {"match_all": {}}}

    def to_dict(self):
        return False

    def vars(self):
        return set()

    def map(self, map_):
        return self

    def missing(self):
        return self

    def __unicode__(self):
        return "false"

    def __str__(self):
        return b"false"


class BinaryOp(Expression):
    has_simple_form = True

    operators = {
        "sub": "-",
        "subtract": "-",
        "minus": "-",
        "mul": "*",
        "mult": "*",
        "multiply": "*",
        "div": "/",
        "divide": "/",
        "exp": "**",
        "mod": "%",
        "gt": ">",
        "gte": ">=",
        "eq": "==",
        "lte": "<=",
        "lt": "<",
        "term": "=="
    }

    algebra_ops = {
        "add",
        "sub",
        "subtract",
        "minus",
        "mul",
        "mult",
        "multiply",
        "div",
        "divide",
        "exp",
        "mod",
    }

    ineq_ops = {
        "gt",
        "gte",
        "lte",
        "lt"
    }


    def __init__(self, op, terms):
        Expression.__init__(self, op, terms)
        if op not in BinaryOp.operators:
            Log.error("{{op|quote}} not a recognized operator", op=op)
        self.op = op
        self.lhs, self.rhs = terms

    def to_ruby(self):
        output = WhenOp(
            "when",
            OrOp("or", [MissingOp("missing", self.lhs), MissingOp("missing", self.rhs)]),
            **{
                "then": NullOp(),
                "else":
                    ScriptOp(None, "(" + self.lhs.to_ruby() + ") " + BinaryOp.operators[self.op] + " (" + self.rhs.to_ruby()+")")
            }
        ).to_ruby()
        return output

    def to_python(self):
        return "(" + self.lhs.to_python() + ") " + BinaryOp.operators[self.op] + " (" + self.rhs.to_python()+")"

    def to_esfilter(self):
        if not isinstance(self.lhs, Variable) or not isinstance(self.rhs, Literal) or self.op in BinaryOp.algebra_ops:
            return {"script": {"script": self.to_ruby()}}

        if self.op in ["eq", "term"]:
            return {"term": {self.lhs.var: self.rhs.to_esfilter()}}
        elif self.op in ["ne", "neq"]:
            return {"not": {"term": {self.lhs.var: self.rhs.to_esfilter()}}}
        elif self.op in BinaryOp.ineq_ops:
            return {"range": {self.lhs.var: {self.op: convert.json2value(self.rhs.json)}}}
        else:
            Log.error("Logic error")

    def to_dict(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {self.op: {self.lhs.var, convert.json2value(self.rhs.json)}}
        else:
            return {self.op: [self.lhs.to_dict(), self.rhs.to_dict()]}

    def vars(self):
        return self.lhs.vars() | self.rhs.vars()

    def map(self, map_):
        return BinaryOp(self.op, [self.lhs.map(map_), self.rhs.map(map_)])

    def missing(self):
        return OrOp("or", [self.lhs.missing(), self.rhs.missing()])




class EqOp(Expression):
    has_simple_form = True

    def __new__(cls, op, terms):
        if isinstance(terms, list):
            return object.__new__(cls, op, terms)

        items = terms.items()
        if len(items) == 1:
            if isinstance(items[0][1], list):
                return InOp("in", items[0])
            else:
                return EqOp("eq", items[0])
        else:
            acc = []
            for a, b in items:
                if b.json.startswith("["):
                    acc.append(InOp("in", [Variable(a), b]))
                else:
                    acc.append(EqOp("eq", [Variable(a), b]))
            return AndOp("and", acc)

    def __init__(self, op, terms):
        Expression.__init__(self, op, terms)
        self.op = op
        self.lhs, self.rhs = terms

    def to_ruby(self):
        return "(" + self.lhs.to_ruby() + ") == (" + self.rhs.to_ruby()+")"

    def to_python(self):
        return "(" + self.lhs.to_python() + ") == (" + self.rhs.to_python()+")"

    def to_esfilter(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {"term": {self.lhs.var: convert.json2value(self.rhs.json)}}
        else:
            return {"script": {"script": self.to_ruby()}}

    def to_dict(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {"eq": {self.lhs.var, convert.json2value(self.rhs.json)}}
        else:
            return {"eq": [self.lhs.to_dict(), self.rhs.to_dict()]}

    def vars(self):
        return self.lhs.vars() | self.rhs.vars()

    def map(self, map_):
        return EqOp(self.op, [self.lhs.map(map_), self.rhs.map(map_)])

    def missing(self):
        return FalseOp()

    def exists(self):
        return TrueOp()



class NeOp(Expression):
    has_simple_form = True

    def __init__(self, op, terms):
        Expression.__init__(self, op, terms)
        if isinstance(terms, (list, tuple)):
            self.lhs, self.rhs = terms
        elif isinstance(terms, Mapping):
            self.rhs, self.lhs = terms.items()[0]
        else:
            Log.error("logic error")

    def to_ruby(self):
        lhs = self.lhs.to_ruby()
        rhs = self.rhs.to_ruby()
        return "((" + lhs + ")!=null) && ((" + rhs + ")!=null) && ((" + lhs + ")!=(" + rhs + "))"

    def to_python(self):
        lhs = self.lhs.to_python()
        rhs = self.rhs.to_python()
        return "((" + lhs + ") != None and (" + rhs + ") != None and (" + lhs + ") != (" + rhs + "))"

    def to_esfilter(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {"not": {"term": {self.lhs.var: self.rhs.to_esfilter()}}}
        else:
            return {"and": [
                {"and": [{"exists": {"field": v}} for v in self.vars()]},
                {"script": {"script": self.to_ruby()}}
            ]}


    def to_dict(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {"ne": {self.lhs.var, convert.json2value(self.rhs.json)}}
        else:
            return {"ne": [self.lhs.to_dict(), self.rhs.to_dict()]}

    def vars(self):
        return self.lhs.vars() | self.rhs.vars()

    def map(self, map_):
        return NeOp("ne", [self.lhs.map(map_), self.rhs.map(map_)])

    def missing(self):
        return OrOp("or", [self.lhs.missing(), self.rhs.missing()])



class NotOp(Expression):
    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        self.term = term

    def to_ruby(self):
        return "!(" + self.term.to_ruby() + ")"

    def to_python(self):
        return "not (" + self.term.to_python() + ")"

    def vars(self):
        return self.term.vars()

    def to_esfilter(self):
        return {"not": self.term.to_esfilter()}

    def to_dict(self):
        return {"not": self.term.to_dict()}

    def map(self, map_):
        return NotOp("not", self.term.map(map_))

    def missing(self):
        return self.term.missing()


class AndOp(Expression):
    def __init__(self, op, terms):
        Expression.__init__(self, op, terms)
        if terms == None:
            self.terms = []
        elif isinstance(terms, list):
            self.terms = terms
        else:
            self.terms = [terms]

    def to_ruby(self):
        return " && ".join("(" + t.to_ruby() + ")" for t in self.terms)

    def to_python(self):
        return " and ".join("(" + t.to_python() + ")" for t in self.terms)

    def to_esfilter(self):
        return {"and": [t.to_esfilter() for t in self.terms]}

    def to_dict(self):
        return {"and": [t.to_dict() for t in self.terms]}

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output

    def map(self, map_):
        return AndOp("and", [t.map(map_) for t in self.terms])

    def missing(self):
        return False


class OrOp(Expression):
    def __init__(self, op, terms):
        Expression.__init__(self, op, terms)
        self.terms = terms

    def to_ruby(self):
        return " || ".join("(" + t.to_ruby() + ")" for t in self.terms)

    def to_python(self):
        return " or ".join("(" + t.to_python() + ")" for t in self.terms)

    def to_esfilter(self):
        return {"or": [t.to_esfilter() for t in self.terms]}

    def to_dict(self):
        return {"or": [t.to_dict() for t in self.terms]}

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output

    def map(self, map_):
        return OrOp("or", [t.map(map_) for t in self.terms])

    def missing(self):
        return False


class LengthOp(Expression):
    def __init__(self, op, term):
        Expression.__init__(self, op, [term])
        self.term = term

    def to_ruby(self):
        value = self.term.to_ruby()
        return "((" + value + ") == null ) ? null : (" + value + ").length()"

    def to_python(self):
        value = self.term.to_python()
        return "len(" + value + ") if (" + value + ") != None else None"

    def to_dict(self):
        return {"length": self.term.to_dict()}

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return LengthOp("length", self.term.map(map_))

    def missing(self):
        return self.term.missing()


class NumberOp(Expression):
    def __init__(self, op, term):
        Expression.__init__(self, op, [term])
        self.term = term

    def to_ruby(self):
        value = self.term.to_ruby()
        return "((" + value + ") == null ) ? null : (" + value + ").to_f()"

    def to_python(self):
        value = self.term.to_python()
        return "float(" + value + ") if (" + value + ") != None else None"

    def to_dict(self):
        return {"number": self.term.to_dict()}

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return NumberOp("number", self.term.map(map_))

    def missing(self):
        return self.term.missing()


class MultiOp(Expression):
    has_simple_form = True

    operators = {
        "add": (" + ", "0"),  # (operator, zero-array default value) PAIR
        "sum": (" + ", "0"),
        "mul": (" * ", "1"),
        "mult": (" * ", "1"),
        "multiply": (" * ", "1")
    }

    def __init__(self, op, terms, **clauses):
        Expression.__init__(self, op, terms)
        self.op = op
        self.terms = terms
        self.default = coalesce(clauses.get("default"), NullOp())

    def to_ruby(self):
        op, unit = MultiOp.operators[self.op]
        null_test = CoalesceOp(None, self.terms).missing().to_ruby()
        acc = op.join("((" + t.to_ruby() + "!=null) ? (" + t.to_ruby() + ") : " + unit + ")" for t in self.terms)
        return "((" + null_test + ") ? (" + self.default.to_ruby() + ") : (" + acc + "))"

    def to_python(self):
        return MultiOp.operators[self.op][0].join("(" + t.to_python() + ")" for t in self.terms)

    def to_dict(self):
        return {self.op: [t.to_dict() for t in self.terms]}

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output

    def map(self, map_):
        return MultiOp(self.op, [t.map(map_) for t in self.terms])

    def missing(self):
        if self.default == None:
            return AndOp("and", [t.missing() for t in self.terms])
        else:
            return FalseOp

    def exists(self):
        return OrOp("or", [t.exists() for t in self.terms])



class RegExpOp(Expression):
    has_simple_form = True

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        self.var, self.pattern = term

    def to_python(self):
        return "re.match(" + self.pattern + ", " + self.var.to_python() + ")"

    def to_esfilter(self):
        return {"regexp": {self.var.var: convert.json2value(self.pattern.json)}}

    def to_dict(self):
        return {"regexp": {self.var.var: self.pattern}}

    def vars(self):
        return {self.var.var}

    def map(self, map_):
        return RegExpOp("regex", [self.var.map(map_), self.pattern])

    def missing(self):
        return FalseOp()

    def exists(self):
        return TrueOp()



class ContainsOp(Expression):
    """
    RETURN true IF substring CAN BE FOUND IN var, ELSE RETURN false
    """
    has_simple_form = True

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        self.var, self.substring = term

    def to_python(self):
        return "((" + convert.string2quote(self.substring) + " in " + self.var.to_python() + ") if " + self.var.to_python() + "!=None else False)"

    def to_ruby(self):
        v = self.var.to_ruby()
        c = self.substring.to_ruby()
        return "((" + v + ") == null ? false : q.indexOf(" + c + ")>=0)"

    def to_esfilter(self):
        if isinstance(self.var, Variable) and isinstance(self.substring, Literal):
            return {"regexp": {self.var.var: ".*" + convert.string2regexp(convert.json2value(self.substring.json)) + ".*"}}
        else:
            return {"script": {"script": self.to_ruby()}}

    def to_dict(self):
        return {"contains": {self.var.var: self.substring}}

    def vars(self):
        return {self.var.var}

    def map(self, map_):
        return ContainsOp(None, [self.var.map(map_), self.substring])

    def missing(self):
        return FalseOp()

    def exists(self):
        return TrueOp()


class CoalesceOp(Expression):
    def __init__(self, op, terms):
        Expression.__init__(self, op, terms)
        self.terms = terms

    def to_ruby(self):
        acc = self.terms[-1].to_ruby()
        for v in reversed(self.terms[:-1]):
            r = v.to_ruby()
            acc = "(((" + r + ") != null) ? (" + r + ") : (" + acc + "))"
        return acc

    def to_python(self):
        return "coalesce(" + (",".join(t.to_python() for t in self.terms)) + ")"

    def to_esfilter(self):
        return {"or": [{"exists": {"field": v}} for v in self.terms]}

    def to_dict(self):
        return {"coalesce": [t.to_dict() for t in self.terms]}

    def missing(self):
        # RETURN true FOR RECORDS THE WOULD RETURN NULL
        return AndOp("and", [v.missing() for v in self.terms])

    def vars(self):
        output = set()
        for v in self.terms:
            output |= v.vars()
        return output

    def map(self, map_):
        return CoalesceOp("coalesce", [v.map(map_) for v in self.terms])


class MissingOp(Expression):
    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        self.field = term

    def to_ruby(self):
        if isinstance(self.field, Variable):
            return "doc["+convert.string2quote(self.field.var)+"].isEmpty()"
        elif isinstance(self.field, Literal):
            return self.field.missing().to_ruby()
        else:
            return self.field.to_ruby() + " == null"

    def to_python(self):
        return self.field.to_python() + " == None"

    def to_esfilter(self):
        if isinstance(self.field, Variable):
            return {"missing": {"field": self.field.var}}
        else:
            return {"script": {"script": self.to_ruby()}}

    def to_dict(self):
        return {"missing": self.field.var}

    def vars(self):
        return {self.field.var}

    def map(self, map_):
        return MissingOp("missing", self.field.map(map_))

    def missing(self):
        return FalseOp()

    def exists(self):
        return TrueOp()


class ExistsOp(Expression):
    def __init__(self, op, term):
        Expression.__init__(self, op, [term])
        self.field = term

    def to_ruby(self):
        if isinstance(self.field, Variable):
            return "!doc["+convert.string2quote(self.field.var)+"].isEmpty()"
        elif isinstance(self.field, Literal):
            return self.field.exists().to_ruby()
        else:
            return self.field.to_ruby() + " != null"

    def to_python(self):
        return self.field.to_python() + " != None"

    def to_esfilter(self):
        if isinstance(self.field, Variable):
            return {"exists": {"field": self.field.var}}
        else:
            return {"script": {"script": self.to_ruby()}}

    def to_dict(self):
        return {"exists": self.field.to_dict()}

    def vars(self):
        return self.field.vars()

    def map(self, map_):
        return ExistsOp("exists", self.field.map(map_))

    def missing(self):
        return FalseOp()

    def exists(self):
        return TrueOp()


class PrefixOp(Expression):
    has_simple_form = True

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        if isinstance(term, Mapping):
            self.field, self.prefix = term.items()[0]
        else:
            self.field, self.prefix = term

    def to_ruby(self):
        return "(" + self.field.to_ruby() + ").startsWith(" + self.prefix.to_ruby() + ")"

    def to_python(self):
        return "(" + self.field.to_python() + ").startswith(" + self.prefix.to_python() + ")"

    def to_esfilter(self):
        if isinstance(self.field, Variable) and isinstance(self.prefix, Literal):
            return {"prefix": {self.field.var: convert.json2value(self.prefix.json)}}
        else:
            return {"script": {"script": self.to_ruby()}}

    def to_dict(self):
        if isinstance(self.field, Variable) and isinstance(self.prefix, Literal):
            return {"prefix": {self.field.var: convert.json2value(self.prefix.json)}}
        else:
            return {"prefix": [self.field.to_dict(), self.prefix.to_dict()]}

    def vars(self):
        return {self.field.var}

    def map(self, map_):
        return PrefixOp("prefix", [self.field.map(map_), self.prefix.map(map_)])


class LeftOp(Expression):
    has_simple_form = True

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        if isinstance(term, Mapping):
            self.value, self.length = term.items()[0]
        else:
            self.value, self.length = term

    def to_ruby(self):
        v = self.value.to_ruby()
        l = self.length.to_ruby()
        expr = "((" + v + ") == null || (" + l + ") == null) ? null : (" + v + ".substring(0, max(0, min(" + v + ".length(), " + l + "))))"
        return expr

    def to_python(self):
        v = self.value.to_python()
        l = self.length.to_python()
        return "None if " + v + " == None or " + l + " == None else " + v + "[0:min(0, " + l + ")]"

    def to_esfilter(self):
        raise NotImplementedError

    def to_dict(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            return {"prefix": {self.value.var: convert.json2value(self.length.json)}}
        else:
            return {"prefix": [self.value.to_dict(), self.length.to_dict()]}

    def vars(self):
        return self.value.vars() | self.length.vars()

    def map(self, map_):
        return LeftOp("left", [self.value.map(map_), self.length.map(map_)])


class InOp(Expression):
    has_simple_form = True

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        self.field, self.values = term

    def to_ruby(self):
        return self.values.to_ruby() + ".contains(" + self.field.to_ruby() + ")"

    def to_python(self):
        return self.field.to_python() + " in " + self.values.to_python()

    def to_esfilter(self):
        return {"terms": {self.field.var: convert.json2value(self.values.json)}}

    def to_dict(self):
        if isinstance(self.field, Variable) and isinstance(self.values, Literal):
            return {"in": {self.field.var: convert.json2value(self.values.json)}}
        else:
            return {"in": [self.field.to_dict(), self.values.to_dict()]}

    def vars(self):
        return self.field.vars()

    def map(self, map_):
        return InOp("in", [self.field.map(map_), self.values])


class RangeOp(Expression):
    has_simple_form = True

    def __new__(cls, op, term, *args, **kwargs):
        Expression.__new__(cls, *args, **kwargs)
        field, cmps = term.items()[0]
        return AndOp("and", [{op: {field: value}} for op, value in cmps.items()])

    def __init__(self, op, term):
        Log.error("Should never happen!")


class WhenOp(Expression):
    def __init__(self, op, term, **clauses):
        Expression.__init__(self, op, [term])
        self.when = term
        self.then = coalesce(clauses.get("then"), NullOp())
        self.els_ = coalesce(clauses.get("else"), NullOp())

    def to_ruby(self):
        return "(" + self.when.to_ruby() + ") ? (" + self.then.to_ruby() + ") : (" + self.els_.to_ruby() + ")"

    def to_python(self):
        return "(" + self.when.to_python() + ") ? (" + self.then.to_python() + ") : (" + self.els_.to_python() + ")"

    def to_esfilter(self):
        return {"script": {"script": self.to_ruby()}}

    def to_dict(self):
        return {"when": self.when.to_dict(), "then": self.then.to_dict() if self.then else None, "else": self.els_.to_dict() if self.els_ else None}

    def vars(self):
        return self.when.vars() | self.then.vars() | self.els_.vars()

    def map(self, map_):
        return WhenOp("when", self.when.map(map_), **{"then": self.then.map(map_), "else": self.els_.map(map_)})


class CaseOp(Expression):
    def __init__(self, op, term, **clauses):
        if not isinstance(term, (list, tuple)):
            Log.error("case expression requires a list of `when` sub-clauses")
        Expression.__init__(self, op, term)
        if len(term) == 0:
            self.whens = [NullOp()]
        else:
            for w in term[:-1]:
                if not isinstance(w, WhenOp) or w.els_:
                    Log.error("case expression does not allow `else` clause in `when` sub-clause")
            self.whens = term

    def to_ruby(self):
        acc = self.whens[-1].to_ruby()
        for w in reversed(self.whens[0:-1]):
            acc = "(" + w.when.to_ruby() + ") ? (" + w.then.to_ruby() + ") : (" + acc + ")"
        return acc

    def to_python(self):
        acc = self.whens[-1].to_python()
        for w in reversed(self.whens[0:-1]):
            acc = "(" + w.when.to_python() + ") ? (" + w.then.to_python() + ") : (" + acc + ")"
        return acc

    def to_esfilter(self):
        return {"script": {"script": self.to_ruby()}}

    def to_dict(self):
        return {"case": [w.to_dict() for w in self.whens]}

    def vars(self):
        output = set()
        for w in self.whens:
            output |= w.vars()
        return output

    def map(self, map_):
        return CaseOp("case", [w.map(map_) for w in self.whens])

    def missing(self):
        return self.whens[-1].missing()


def simplify_esfilter(esfilter):
    try:
        output = normalize_esfilter(esfilter)
        if output is TRUE_FILTER:
            return {"match_all": {}}
        elif output is FALSE_FILTER:
            return {"not": {"match_all": {}}}

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


def split_expression_by_depth(where, schema, map_, output=None, var_to_depth=None):
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
        var_to_depth = {v: len(listwrap(schema[v].nested_path)) for v in vars_}
        all_depths = set(var_to_depth.values())
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


operators = {
    "in": InOp,
    "terms": InOp,
    "exists": ExistsOp,
    "missing": MissingOp,
    "prefix": PrefixOp,
    "range": RangeOp,
    "regexp": RegExpOp,
    "regex": RegExpOp,
    "literal": Literal,
    "null": NullOp,
    "coalesce": CoalesceOp,
    "left": LeftOp,
    "gt": BinaryOp,
    "gte": BinaryOp,
    "eq": EqOp,
    "lte": BinaryOp,
    "lt": BinaryOp,
    "ne": NeOp,
    "neq": NeOp,
    "term": EqOp,
    "not": NotOp,
    "and": AndOp,
    "or": OrOp,
    "length": LengthOp,
    "contains": ContainsOp,
    "number": NumberOp,
    "add": MultiOp,
    "sum": MultiOp,
    "sub": BinaryOp,
    "subtract": BinaryOp,
    "minus": BinaryOp,
    "mul": MultiOp,
    "mult": MultiOp,
    "multiply": MultiOp,
    "div": BinaryOp,
    "divide": BinaryOp,
    "exp": BinaryOp,
    "mod": BinaryOp,
    "when": WhenOp,
    "case": CaseOp,
    "match_all": TrueOp,
    "script": ScriptOp
}

