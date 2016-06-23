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
from collections import Mapping
from decimal import Decimal

from pyLibrary import convert
from pyLibrary.collections import OR, MAX
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce, wrap, set_default, literal_field, listwrap, Null, split_field, startswith_field, \
    Dict, join_field, unwraplist, unwrap
from pyLibrary.maths import Math
from pyLibrary.queries.containers import STRUCT
from pyLibrary.queries.domains import is_keyword
from pyLibrary.queries.expression_compiler import compile_expression
from pyLibrary.times.dates import Date

ALLOW_SCRIPTING = False
TRUE_FILTER = True
FALSE_FILTER = False
EMPTY_DICT = {}

_Query = None


def _late_import():
    global _Query

    from pyLibrary.queries.query import QueryOp as _Query

    _ = _Query


def jx_expression(expr):
    """
    WRAP A JSON EXPRESSION WITH OBJECT REPRESENTATION
    """
    if isinstance(expr, Expression):
        Log.error("Expecting JSON, not expression")

    if expr in (True, False, None) or expr == None or isinstance(expr, (float, int, Decimal, Date)):
        return Literal(None, expr)
    elif isinstance(expr, unicode):
        if is_keyword(expr):
            return Variable(expr)
        elif not expr.strip():
            Log.error("expression is empty")
        else:
            Log.error("expression is not recognized: {{expr}}", expr=expr)
    elif isinstance(expr, (list, tuple)):
        return jx_expression({"tuple": expr})  # FORMALIZE

    expr = wrap(expr)
    if expr.date:
        return DateOp("date", expr)

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
                clauses = {k: jx_expression(v) for k, v in expr.items() if k != op}
                break
        else:
            raise Log.error("{{operator|quote}} is not a known operator", operator=op)

    if class_ is Literal:
        return class_(op, term)
    elif class_ is ScriptOp:
        if ALLOW_SCRIPTING:
            Log.warning("Scripting has been activated:  This has known security holes!!\nscript = {{script|quote}}", script=term)
            return class_(op, term)
        else:
            Log.error("scripting is disabled")
    elif term == None:
        return class_(op, [], **clauses)
    elif isinstance(term, list):
        terms = map(jx_expression, term)
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
            return class_(op, jx_expression(term), **clauses)
    else:
        if op in ["literal", "date"]:
            return class_(op, term, **clauses)
        else:
            return class_(op, jx_expression(term), **clauses)


def jx_expression_to_function(expr):
    """
    RETURN FUNCTION THAT REQUIRES PARAMETERS (row, rownum=None, rows=None):
    """
    if isinstance(expr, Expression):
        return compile_expression(expr.to_python())
    if expr != None and not isinstance(expr, (Mapping, list)) and hasattr(expr, "__call__"):
        return expr
    return compile_expression(jx_expression(expr).to_python())


class Expression(object):
    has_simple_form = False

    def __init__(self, op, terms):
        if isinstance(terms, (list, tuple)):
            if not all(isinstance(t, Expression) for t in terms):
                Log.error("Expecting an expression")
        elif isinstance(terms, Mapping):
            if not all(isinstance(k, Variable) and isinstance(v, Literal) for k, v in terms.items()):
                Log.error("Expecting an {<variable>: <literal>}")
        elif terms == None:
            pass
        else:
            if not isinstance(terms, Expression):
                Log.error("Expecting an expression")

    @property
    def name(self):
        return self.__class_.__name__

    def to_ruby(self, not_null=False, boolean=False):
        """
        :param not_null:  (Optimization) SET TO True IF YOU KNOW THIS EXPRESSION CAN NOT RETURN null
        :param boolean:   (Optimization) SET TO True IF YOU WANT A BOOLEAN RESULT
        :return: jRuby/ES code (unicode)
        """
        raise NotImplementedError

    def to_python(self, not_null=False, boolean=False):
        """
        :param not_null:  (Optimization) SET TO True IF YOU KNOW THIS EXPRESSION CAN NOT RETURN null
        :param boolean:   (Optimization) SET TO True IF YOU WANT A BOOLEAN RESULT
        :return: Python code (unicode)
        """
        raise Log.error("{{type}} has no `to_python` method", type=self.__class__.__name__)

    def to_sql(self, schema, not_null=False, boolean=False):
        """
        :param not_null:  IF YOU KNOW THIS WILL NOT RETURN NULL (DO NOT INCLUDE NULL CHECKS)
        :param boolean: IF YOU KNOW THIS WILL RETURN A BOOLEAN VALUE
        :return: A LIST OF {"name": col, "sql": value, "nested_path":nested_path} dicts WHERE
            col (string) IS THE PATH VALUE TO SET
            value IS A dict MAPPING TYPE TO SQL : (s=string, n=number, b=boolean, 0=null, j=json)
            nested_path IS THE IDEAL DEPTH THIS VALUE IS CALCULATED AT
        """
        raise Log.error("{{type}} has no `to_sql` method", type=self.__class__.__name__)

    def to_esfilter(self):
        raise Log.error("{{type}} has no `to_esfilter` method", type=self.__class__.__name__)

    def to_dict(self):
        raise NotImplementedError

    def __json__(self):
        return convert.value2json(self.to_dict())

    def vars(self):
        raise Log.error("{{type}} has no `vars` method", type=self.__class__.__name__)

    def map(self, map):
        raise Log.error("{{type}} has no `map` method", type=self.__class__.__name__)

    def missing(self):
        # RETURN FILTER THAT INDICATE THIS EXPRESSIOn RETURNS null
        raise Log.error("{{type}} has no `missing` method", type=self.__class__.__name__)

    def exists(self):
        return NotOp("not", self.missing())

    def is_true(self):
        """
        :return: True, IF THIS EXPRESSION ALWAYS RETURNS BOOLEAN true
        """
        return FalseOp()  # GOOD DEFAULT ASSUMPTION

    def is_false(self):
        """
        :return: True, IF THIS EXPRESSION ALWAYS RETURNS BOOLEAN false
        """
        return FalseOp()  # GOOD DEFAULT ASSUMPTION


class Variable(Expression):

    def __init__(self, var):
        Expression.__init__(self, "", None)
        if not is_keyword(var):
            Log.error("Expecting a variable")
        self.var = var

    def to_ruby(self, not_null=False, boolean=False):
        if self.var == ".":
            return "_source"
        else:
            q = convert.string2quote(self.var)
            if not_null:
                if boolean:
                    return "doc[" + q + "].value==\"T\""
                else:
                    return "doc[" + q + "].value"
            else:
                if boolean:
                    return "doc[" + q + "].isEmpty() ? null : (doc[" + q + "].value==\"T\")"
                else:
                    return "doc[" + q + "].isEmpty() ? null : doc[" + q + "].value"

    def to_python(self, not_null=False, boolean=False):
        path = split_field(self.var)
        agg = "row"
        if not path:
            return agg
        for p in path[:-1]:
            agg = agg+".get("+convert.value2quote(p)+", EMPTY_DICT)"
        return agg+".get("+convert.value2quote(path[-1])+")"

    def to_sql(self, schema, not_null=False, boolean=False):
        cols = schema.get(self.var, None)
        if cols is None:
            # DOES NOT EXIST
            return wrap([{"name": ".", "sql": {"n": "NULL"}, "nested_path": ["."]}])

        acc = Dict()
        nested_path = ["."]
        for c in cols:
            nested_path = wrap_nested_path(c.nested_path)
            acc[json_type_to_sql_type[c.type]] = c.es_index + "." + convert.string2quote(c.es_column)

        return wrap([{"name": ".", "sql": acc, "nested_path": nested_path}])

    def __call__(self, row, rownum=None, rows=None):
        path = split_field(self.var)
        for p in path:
            row = row.get(p)
            if row is None:
                return None
        return row

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

    def __call__(self, row=None, rownum=None, rows=None):
        return row[self.var]

    def __hash__(self):
        return self.var.__hash__()

    def __eq__(self, other):
        return self.var.__eq__(other)

    def __unicode__(self):
        return self.var

    def __str__(self):
        return str(self.var)


class ScriptOp(Expression):
    """
    ONLY FOR TESTING AND WHEN YOU TRUST THE SCRIPT SOURCE
    """

    def __init__(self, op, script):
        Expression.__init__(self, "", None)
        self.script = script

    def to_ruby(self, not_null=False, boolean=False):
        return self.script

    def to_python(self, not_null=False, boolean=False):
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
        if isinstance(term, Mapping) and term.date:
            # SPECIAL CASE
            return object.__new__(DateOp, None, term)
        return object.__new__(cls, op, term)

    def __init__(self, op, term):
        Expression.__init__(self, "", None)
        self.json = convert.value2json(term)

    def __nonzero__(self):
        return True

    def __eq__(self, other):
        if other == None:
            if self.json == "null":
                return True
            else:
                return False
        elif self.json == "null":
            return False

        Log.warning("expensive")

        from pyLibrary.testing.fuzzytestcase import assertAlmostEqual

        try:
            assertAlmostEqual(convert.json2value(self.json), other)
            return True
        except Exception:
            return False

    def to_ruby(self, not_null=False, boolean=False):
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

    def to_python(self, not_null=False, boolean=False):
        return self.json

    def to_sql(self, schema, not_null=False, boolean=False):
        value = convert.json2value(self.json)
        v = sql_quote(value)
        if v == None:
            return wrap([{"name": "."}])
        elif isinstance(value, unicode):
            return wrap([{"name": ".", "sql": {"s": sql_quote(value)}}])
        elif Math.is_number(v):
            return wrap([{"name": ".", "sql": {"n": sql_quote(value)}}])
        elif v in [True, False]:
            return wrap([{"name": ".", "sql": {"b": sql_quote(value)}}])
        else:
            return wrap([{"name": ".", "sql": {"j": sql_quote(self.json)}}])

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

    def __call__(self, row=None, rownum=None, rows=None):
        return convert.json2value(self.json)

    def __unicode__(self):
        return self.json

    def __str__(self):
        return str(self.json)


class NullOp(Literal):
    """
    FOR USE WHEN EVERYTHING IS EXPECTED TO BE AN Expression
    USE IT TO EXPECT A NULL VALUE IN assertAlmostEqual
    """

    def __new__(cls, *args, **kwargs):
        return object.__new__(cls, *args, **kwargs)

    def __init__(self, op=None, term=None):
        Literal.__init__(self, op, None)

    def __nonzero__(self):
        return False

    def __eq__(self, other):
        return other == None

    def to_ruby(self, not_null=False, boolean=False):
        return "null"

    def to_python(self, not_null=False, boolean=False):
        return "None"

    def to_sql(self, schema, not_null=False, boolean=False):
        return {}

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

    def __call__(self, row=None, rownum=None, rows=None):
        return Null

    def __unicode__(self):
        return "null"

    def __str__(self):
        return b"null"

    def __json__(self):
        return "null"

class TrueOp(Literal):
    def __new__(cls, *args, **kwargs):
        return object.__new__(cls, *args, **kwargs)

    def __init__(self, op=None, term=None):
        Literal.__init__(self, op, True)

    def __nonzero__(self):
        return True

    def __eq__(self, other):
        return other == True

    def to_ruby(self, not_null=False, boolean=False):
        return "true"

    def to_python(self, not_null=False, boolean=False):
        return "True"

    def to_sql(self, schema, not_null=False, boolean=False):
        return wrap([{"b": "1"}])

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

    def is_true(self):
        return TrueOp()

    def is_false(self):
        return FalseOp()

    def __call__(self, row=None, rownum=None, rows=None):
        return True

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
        return False

    def __eq__(self, other):
        return other == False

    def to_ruby(self, not_null=False, boolean=False):
        return "false"

    def to_python(self, not_null=False, boolean=False):
        return "False"

    def to_sql(self, schema, not_null=False, boolean=False):
        return {"b": "0"}

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

    def is_true(self):
        return FalseOp()

    def is_false(self):
        return TrueOp()

    def __call__(self, row=None, rownum=None, rows=None):
        return False

    def __unicode__(self):
        return "false"

    def __str__(self):
        return b"false"


class DateOp(Literal):
    def __init__(self, op, term):
        self.value = term.date
        Literal.__init__(self, op, Date(term.date).unix)

    def to_python(self, not_null=False, boolean=False):
        return "Date("+convert.string2quote(self.value)+")"

    def to_sql(self, schema, not_null=False, boolean=False):
        return {"n": sql_quote(self.value.unix)}

    def to_esfilter(self):
        return convert.json2value(self.json)

    def to_dict(self):
        return {"date": self.value}

    def __call__(self, row=None, rownum=None, rows=None):
        return Date(self.value)

    def __unicode__(self):
        return self.json

    def __str__(self):
        return str(self.json)


class TupleOp(Expression):

    def __init__(self, op, terms):
        Expression.__init__(self, op, terms)
        if terms == None:
            self.terms = []
        elif isinstance(terms, list):
            self.terms = terms
        else:
            self.terms = [terms]

    def to_ruby(self, not_null=False, boolean=False):
        Log.error("not supported")

    def to_python(self, not_null=False, boolean=False):
        if len(self.terms) == 0:
            return "tuple()"
        elif len(self.terms) == 1:
            return "(" + self.terms[0].to_python() + ",)"
        else:
            return "(" + (",".join(t.to_python() for t in self.terms)) + ")"

    def to_esfilter(self):
        Log.error("not supported")

    def to_dict(self):
        return {"tuple": [t.to_dict() for t in self.terms]}

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output

    def map(self, map_):
        return TupleOp("tuple", [t.map(map_) for t in self.terms])

    def missing(self):
        return False


class LeavesOp(Expression):

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        self.term = term

    def to_ruby(self, not_null=False, boolean=False):
        Log.error("not supported")

    def to_python(self, not_null=False, boolean=False):
        return "Dict(" + self.term.to_python() + ").leaves()"

    def to_sql(self, schema, not_null=False, boolean=False):
        if not isinstance(self.term, Variable):
            Log.error("Can only handle Variable")
        term = self.term.var
        path = split_field(term)
        return [
            (literal_field(join_field(split_field(c.name)[:len(path)])), term.to_sql())
            for n, cols in schema.items()
            if startswith_field(n, term)
            for c in cols
            if c.type not in STRUCT
            ]

    def to_esfilter(self):
        Log.error("not supported")

    def to_dict(self):
        return {"leaves": self.term.to_dict()}

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return LeavesOp("leaves", self.term.map(map_))

    def missing(self):
        return False


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
        "mod": "%"
    }

    def __init__(self, op, terms, default=NullOp()):
        Expression.__init__(self, op, terms)
        if op not in BinaryOp.operators:
            Log.error("{{op|quote}} not a recognized operator", op=op)
        self.op = op
        self.lhs, self.rhs = terms
        self.default = default

    @property
    def name(self):
        return self.op;

    def to_ruby(self, not_null=False, boolean=False):
        lhs = self.lhs.to_ruby(not_null=True)
        rhs = self.rhs.to_ruby(not_null=True)
        script = "(" + lhs + ") " + BinaryOp.operators[self.op] + " (" + rhs + ")"
        missing = OrOp("or", [self.lhs.missing(), self.rhs.missing()])

        if self.op in BinaryOp.algebra_ops:
            script = "(" + script + ").doubleValue()"  # RETURN A NUMBER, NOT A STRING

        output = WhenOp(
            "when",
            missing,
            **{
                "then": self.default,
                "else":
                    ScriptOp("script", script)
            }
        ).to_ruby()
        return output

    def to_python(self, not_null=False, boolean=False):
        return "(" + self.lhs.to_python() + ") " + BinaryOp.operators[self.op] + " (" + self.rhs.to_python()+")"

    def to_sql(self, schema, not_null=False, boolean=False):
        lhs = self.lhs.to_sql(schema)[0].sql.n
        rhs = self.rhs.to_sql(schema)[0].sql.n

        return wrap([{"name": ".", "sql": {"n": "(" + lhs + ") " + BinaryOp.operators[self.op] + " (" + rhs + ")"}}])

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
            return {self.op: {self.lhs.var, convert.json2value(self.rhs.json)}, "default": self.default}
        else:
            return {self.op: [self.lhs.to_dict(), self.rhs.to_dict()], "default": self.default}

    def vars(self):
        return self.lhs.vars() | self.rhs.vars() | self.default.vars()

    def map(self, map_):
        return BinaryOp(self.op, [self.lhs.map(map_), self.rhs.map(map_)], default=self.default.map(map_))

    def missing(self):
        if self.default.exists():
            return FalseOp()
        else:
            return OrOp("or", [self.lhs.missing(), self.rhs.missing()])


class InequalityOp(Expression):
    has_simple_form = True

    operators = {
        "gt": ">",
        "gte": ">=",
        "lte": "<=",
        "lt": "<"
    }

    def __init__(self, op, terms, default=NullOp()):
        Expression.__init__(self, op, terms)
        if op not in InequalityOp.operators:
            Log.error("{{op|quote}} not a recognized operator", op=op)
        self.op = op
        self.lhs, self.rhs = terms
        self.default = default

    @property
    def name(self):
        return self.op;

    def to_ruby(self, not_null=False, boolean=False):
        lhs = self.lhs.to_ruby(not_null=True)
        rhs = self.rhs.to_ruby(not_null=True)
        script = "(" + lhs + ") " + InequalityOp.operators[self.op] + " (" + rhs + ")"
        missing = OrOp("or", [self.lhs.missing(), self.rhs.missing()])

        output = WhenOp(
            "when",
            missing,
            **{
                "then": self.default,
                "else":
                    ScriptOp("script", script)
            }
        ).to_ruby()
        return output

    def to_python(self, not_null=False, boolean=False):
        return "(" + self.lhs.to_python() + ") " + InequalityOp.operators[self.op] + " (" + self.rhs.to_python()+")"

    def to_sql(self, schema, not_null=False, boolean=False):
        lhs = self.lhs.to_sql(schema)[0]
        rhs = self.rhs.to_sql(schema)[0]
        lhs_exists = self.lhs.exists().to_sql()[0]
        rhs_exists = self.rhs.exists().to_sql()[0]

        if len(lhs) == 1 and len(rhs) == 1:
            return [{"name":".", "sql": {
                "b": "(" + lhs.values()[0] + ") " + InequalityOp.operators[self.op] + " (" + rhs.values()[0] + ")"
            }}]

        ors = []
        for l in "bns":
            ll = lhs[l]
            if not ll:
                continue
            for r in "bns":
                rr = rhs[r]
                if not rr:
                    continue
                elif r == l:
                    ors.append(
                        "(" + lhs_exists[l] + ") AND (" + rhs_exists[r] + ") AND (" + lhs[l] + ") " +
                        InequalityOp.operators[self.op] + " (" + rhs[r] + ")"
                    )
                elif (l > r and self.op in ["gte", "gt"]) or (l < r and self.op in ["lte", "lt"]):
                    ors.append(
                        "(" + lhs_exists[l] + ") AND (" + rhs_exists[r] + ")"
                    )
        sql = "(" + ") OR (".join(ors) + ")"

        return wrap([{"name":".", "sql": {"b": sql}}])

    def to_esfilter(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {"range": {self.lhs.var: {self.op: convert.json2value(self.rhs.json)}}}
        else:
            return {"script": {"script": self.to_ruby()}}

    def to_dict(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {self.op: {self.lhs.var, convert.json2value(self.rhs.json)}, "default": self.default}
        else:
            return {self.op: [self.lhs.to_dict(), self.rhs.to_dict()], "default": self.default}

    def vars(self):
        return self.lhs.vars() | self.rhs.vars() | self.default.vars()

    def map(self, map_):
        return InequalityOp(self.op, [self.lhs.map(map_), self.rhs.map(map_)], default=self.default.map(map_))

    def missing(self):
        if self.default.exists():
            return FalseOp()
        else:
            return OrOp("or", [self.lhs.missing(), self.rhs.missing()])


class DivOp(Expression):
    has_simple_form = True

    def __init__(self, op, terms, default=NullOp()):
        Expression.__init__(self, op, terms)
        self.lhs, self.rhs = terms
        self.default = default

    def to_ruby(self, not_null=False, boolean=False):
        lhs = self.lhs.to_ruby(not_null=True)
        rhs = self.rhs.to_ruby(not_null=True)
        script = "((double)(" + lhs + ") / (double)(" + rhs + ")).doubleValue()"

        output = WhenOp(
            "when",
            OrOp("or", [self.lhs.missing(), self.rhs.missing(), EqOp("eq", [self.rhs, Literal("literal", 0)])]),
            **{
                "then": self.default,
                "else":
                    ScriptOp("script", script)
            }
        ).to_ruby()
        return output

    def to_python(self, not_null=False, boolean=False):
        return "float(" + self.lhs.to_python() + ") / float(" + self.rhs.to_python()+")"

    def to_sql(self, schema, not_null=False, boolean=False):
        lhs = self.lhs.to_sql(schema).n
        rhs = self.rhs.to_sql(schema).n
        if lhs and rhs:
            return "(" + self.lhs.to_sql(schema).n + ") / (" + self.rhs.to_sql(schema).n+")"
        else:
            return "NULL"

    def to_esfilter(self):
        if not isinstance(self.lhs, Variable) or not isinstance(self.rhs, Literal):
            return {"script": {"script": self.to_ruby()}}
        else:
            Log.error("Logic error")

    def to_dict(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {"div": {self.lhs.var, convert.json2value(self.rhs.json)}, "default": self.default}
        else:
            return {"div": [self.lhs.to_dict(), self.rhs.to_dict()], "default": self.default}

    def vars(self):
        return self.lhs.vars() | self.rhs.vars() | self.default.vars()

    def map(self, map_):
        return DivOp("div", [self.lhs.map(map_), self.rhs.map(map_)], default=self.default.map(map_))

    def missing(self):
        if self.default.exists():
            return FalseOp()
        else:
            return OrOp("or", [self.lhs.missing(), self.rhs.missing(), EqOp("eq", [self.rhs, Literal("literal", 0)])])


class FloorOp(Expression):
    has_simple_form = True

    def __init__(self, op, terms, default=NullOp()):
        Expression.__init__(self, op, terms)
        self.lhs, self.rhs = terms
        self.default = default

    def to_ruby(self, not_null=False, boolean=False):
        lhs = self.lhs.to_ruby(not_null=True)
        rhs = self.rhs.to_ruby(not_null=True)
        script = "Math.floor(((double)(" + lhs + ") / (double)(" + rhs + ")).doubleValue())*(" + rhs + ")"

        output = WhenOp(
            "when",
            OrOp("or", [self.lhs.missing(), self.rhs.missing(), EqOp("eq", [self.rhs, Literal("literal", 0)])]),
            **{
                "then": self.default,
                "else":
                    ScriptOp("script", script)
            }
        ).to_ruby()
        return output

    def to_python(self, not_null=False, boolean=False):
        return "Math.floor(" + self.lhs.to_python() + ", " + self.rhs.to_python()+")"

    def to_esfilter(self):
        Log.error("Logic error")

    def to_dict(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {"floor": {self.lhs.var, convert.json2value(self.rhs.json)}, "default": self.default}
        else:
            return {"floor": [self.lhs.to_dict(), self.rhs.to_dict()], "default": self.default}

    def vars(self):
        return self.lhs.vars() | self.rhs.vars() | self.default.vars()

    def map(self, map_):
        return FloorOp("floor", [self.lhs.map(map_), self.rhs.map(map_)], default=self.default.map(map_))

    def missing(self):
        if self.default.exists():
            return FalseOp()
        else:
            return OrOp("or", [self.lhs.missing(), self.rhs.missing(), EqOp("eq", [self.rhs, Literal("literal", 0)])])


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

    def to_ruby(self, not_null=False, boolean=False):
        return "(" + self.lhs.to_ruby() + ") == (" + self.rhs.to_ruby()+")"

    def to_python(self, not_null=False, boolean=False):
        return "(" + self.lhs.to_python() + ") == (" + self.rhs.to_python()+")"

    def to_sql(self, schema, not_null=False, boolean=False):
        lhs = self.lhs.to_sql(schema)
        rhs = self.rhs.to_sql(schema)
        acc = []
        for t in "bsnj":
            if lhs[t] and rhs[t]:
                acc.append("(" + self.lhs[t] + ") = (" + self.rhs[t] + ")")
        if not acc:
            return FalseOp().to_sql(schema)
        else:
            return {"b": " OR ".join(acc)}

    def to_esfilter(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            rhs = convert.json2value(self.rhs.json)
            if isinstance(rhs, list):
                if len(rhs) == 1:
                    return {"term": {self.lhs.var: rhs[0]}}
                else:
                    return {"terms": {self.lhs.var: rhs}}
            else:
                return {"term": {self.lhs.var: rhs}}
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

    def to_ruby(self, not_null=False, boolean=False):
        lhs = self.lhs.to_ruby()
        rhs = self.rhs.to_ruby()
        return "((" + lhs + ")!=null) && ((" + rhs + ")!=null) && ((" + lhs + ")!=(" + rhs + "))"

    def to_python(self, not_null=False, boolean=False):
        lhs = self.lhs.to_python()
        rhs = self.rhs.to_python()
        return "((" + lhs + ") != None and (" + rhs + ") != None and (" + lhs + ") != (" + rhs + "))"

    def to_sql(self, schema, not_null=False, boolean=False):
        lhs = self.lhs.to_sql(schema)
        rhs = self.rhs.to_sql(schema)
        acc = []
        for t in "bsnj":
            if lhs[t] and rhs[t]:
                acc.append("(" + self.lhs[t] + ") = (" + self.rhs[t] + ")")
        if not acc:
            return FalseOp().to_sql(schema)
        else:
            return {"b": "NOT (" + " OR ".join(acc) + ")"}

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

    def to_ruby(self, not_null=False, boolean=False):
        return "!(" + self.term.to_ruby() + ")"

    def to_python(self, not_null=False, boolean=False):
        return "not (" + self.term.to_python() + ")"

    def to_sql(self, schema, not_null=False, boolean=False):
        return {"b": "NOT (" + self.term.to_sql(schema).b + ")"}

    def vars(self):
        return self.term.vars()

    def to_esfilter(self):
        operand = self.term.to_esfilter()
        if operand.get("script"):
            return {"script": {"script": "!(" + operand.get("script", {}).get("script") + ")"}}
        else:
            return {"not": operand}

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

    def to_ruby(self, not_null=False, boolean=False):
        if not self.terms:
            return "true"
        else:
            return " && ".join("(" + t.to_ruby() + ")" for t in self.terms)

    def to_python(self, not_null=False, boolean=False):
        if not self.terms:
            return "True"
        else:
            return " and ".join("(" + t.to_python() + ")" for t in self.terms)

    def to_sql(self, schema, not_null=False, boolean=False):
        if not self.terms:
            return {"b": "1"}
        else:
            return {"b": " AND ".join("(" + t.to_sql(schema).b + ")" for t in self.terms)}

    def to_esfilter(self):
        if not len(self.terms):
            return {"match_all": {}}
        else:
            return {"bool": {"must": [t.to_esfilter() for t in self.terms]}}

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

    def to_ruby(self, not_null=False, boolean=False):
        return " || ".join("(" + t.to_ruby(boolean=True) + ")" for t in self.terms if t)

    def to_python(self, not_null=False, boolean=False):
        return " or ".join("(" + t.to_python() + ")" for t in self.terms)

    def to_sql(self, schema, not_null=False, boolean=False):
        return {"b": " OR ".join("(" + t.to_sql(schema).b + ")" for t in self.terms)}

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

    def __call__(self, row=None, rownum=None, rows=None):
        return any(t(row, rownum, rows) for t in self.terms)


class LengthOp(Expression):
    def __init__(self, op, term):
        Expression.__init__(self, op, [term])
        self.term = term

    def to_ruby(self, not_null=False, boolean=False):
        value = self.term.to_ruby()
        return "((" + value + ") == null ) ? null : (" + value + ").length()"

    def to_python(self, not_null=False, boolean=False):
        value = self.term.to_python()
        return "len(" + value + ") if (" + value + ") != None else None"

    def to_sql(self, schema, not_null=False, boolean=False):
        value = self.term.to_sql(schema).s
        return {"n": "CASE WHEN (" + value + ") IS NULL THEN NULL ELSE LENGTH(" + value + ") END"}

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

    def to_ruby(self, not_null=False, boolean=False):
        test = self.term.missing().to_ruby(boolean=True)
        value = self.term.to_ruby(not_null=True)
        return "(" + test + ") ? null : (((" + value + ") instanceof String) ? Double.parseDouble(" + value + ") : (" + value + "))"

    def to_python(self, not_null=False, boolean=False):
        test = self.term.missing().to_python(boolean=True)
        value = self.term.to_python(not_null=True)
        return "float(" + value + ") if (" + test + ") else None"

    def to_sql(self, schema, not_null=False, boolean=False):
        test = self.term.missing().to_sql(boolean=True).b
        value = self.term.to_sql(not_null=True)
        acc = []
        for t, v in value:
            if t == "b":
                acc.append("CASE WHEN (" + test + ") THEN NULL WHEN (" + value.b + ") THEN 1 ELSE 0 END")
            elif t == "s":
                acc.append("CASE WHEN (" + test + ") THEN NULL ELSE CAST(" + value.s + " as FLOAT) END")
            elif t == "n":
                acc.append(value.n)

        if not acc:
            return {}
        else:
            return {"n": "COALESCE(" + ",".join(acc) + ")"}

    def to_dict(self):
        return {"number": self.term.to_dict()}

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return NumberOp("number", self.term.map(map_))

    def missing(self):
        return self.term.missing()


class StringOp(Expression):
    def __init__(self, op, term):
        Expression.__init__(self, op, [term])
        self.term = term

    def to_ruby(self, not_null=False, boolean=False):
        value = self.term.to_ruby(not_null=True)
        missing = self.term.missing().to_ruby()
        return "(" + missing + ") ? null : (((" + value + ") instanceof java.lang.Double) ? String.valueOf(" + value + ").replaceAll('\\\\.0$', '') : String.valueOf(" + value + "))"  #"\\.0$"

    def to_python(self, not_null=False, boolean=False):
        missing = self.term.missing().to_python(boolean=True)
        value = self.term.to_python(not_null=True)
        return "null if (" + missing + ") else unicode(" + value + ")"

    def to_sql(self, schema, not_null=False, boolean=False):
        test = self.term.missing().to_sql(boolean=True).b
        value = self.term.to_sql(not_null=True)
        acc = []
        for t, v in value:
            if t == "b":
                acc.append("CASE WHEN (" + test + ") THEN NULL WHEN (" + v + ") THEN 'true' ELSE 'false' END")
            elif t == "s":
                acc.append(v)
            else:
                acc.append("CASE WHEN (" + test + ") THEN NULL ELSE CAST(" + v + " as TEXT) END")
        if not acc:
            return {}
        elif len(acc) == 1:
            return {"s": acc[0]}
        else:
            return {"s": "COALESCE(" + ",".join(acc) + ")"}

    def to_dict(self):
        return {"string": self.term.to_dict()}

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return StringOp("string", self.term.map(map_))

    def missing(self):
        return self.term.missing()


class CountOp(Expression):
    has_simple_form = False

    def __init__(self, op, terms, **clauses):
        Expression.__init__(self, op, terms)
        self.terms = terms

    def to_ruby(self, not_null=False, boolean=False):
        return "+".join("((" + t.missing().to_ruby(boolean=True) + ") ? 0 : 1)" for t in self.terms)

    def to_python(self, not_null=False, boolean=False):
        return "+".join("(0 if (" + t.missing().to_python(boolean=True) + ") else 1)" for t in self.terms)

    def to_sql(self, schema, not_null=False, boolean=False):
        acc = []
        for term in self.terms:
            for t, v in term.to_sql(schema).items():
                if t in ["b", "s", "n"]:
                    acc.append("CASE WHEN (" + v + ") IS NULL THEN 0 ELSE 1 END")
                else:
                    acc.append("1")

        if not acc:
            return {}
        else:
            return {"n": "+".join(acc)}

    def to_dict(self):
        return {"count": [t.to_dict() for t in self.terms]}

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output

    def map(self, map_):
        return CountOp("count", [t.map(map_) for t in self.terms])

    def missing(self):
        return FalseOp

    def exists(self):
        return TrueOp


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
        self.nulls = coalesce(clauses.get("nulls"), FalseOp())

    def to_ruby(self, not_null=False, boolean=False):
        if self.nulls:
            op, unit = MultiOp.operators[self.op]
            null_test = CoalesceOp("coalesce", self.terms).missing().to_ruby(boolean=True)
            acc = op.join("((" + t.missing().to_ruby(boolean=True) + ") ? " + unit + " : (" + t.to_ruby(not_null=True) + "))" for t in self.terms)
            return "((" + null_test + ") ? (" + self.default.to_ruby() + ") : (" + acc + "))"
        else:
            op, unit = MultiOp.operators[self.op]
            null_test = OrOp("or", [t.missing() for t in self.terms]).to_ruby()
            acc = op.join("(" + t.to_ruby(not_null=True) + ")" for t in self.terms)
            return "((" + null_test + ") ? (" + self.default.to_ruby() + ") : (" + acc + "))"


    def to_python(self, not_null=False, boolean=False):
        return MultiOp.operators[self.op][0].join("(" + t.to_python() + ")" for t in self.terms)

    def to_dict(self):
        return {self.op: [t.to_dict() for t in self.terms], "default": self.default, "nulls": self.nulls}

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output

    def map(self, map_):
        return MultiOp(self.op, [t.map(map_) for t in self.terms], **{"default": self.default, "nulls": self.nulls})

    def missing(self):
        if self.nulls:
            if self.default == None:
                return AndOp("and", [t.missing() for t in self.terms])
            else:
                return FalseOp
        else:
            if self.default == None:
                return OrOp("or", [t.missing() for t in self.terms])
            else:
                return FalseOp

    def exists(self):
        if self.nulls:
            return OrOp("or", [t.exists() for t in self.terms])
        else:
            return AndOp("and", [t.exists() for t in self.terms])


class RegExpOp(Expression):
    has_simple_form = True

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        self.var, self.pattern = term

    def to_python(self, not_null=False, boolean=False):
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

    def to_python(self, not_null=False, boolean=False):
        return "((" + convert.string2quote(self.substring) + " in " + self.var.to_python() + ") if " + self.var.to_python() + "!=None else False)"

    def to_ruby(self, not_null=False, boolean=False):
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

    def to_ruby(self, not_null=False, boolean=False):
        acc = self.terms[-1].to_ruby()
        for v in reversed(self.terms[:-1]):
            r = v.to_ruby()
            acc = "(((" + r + ") != null) ? (" + r + ") : (" + acc + "))"
        return acc

    def to_python(self, not_null=False, boolean=False):
        return "coalesce(" + (",".join(t.to_python() for t in self.terms)) + ")"

    def to_sql(self, schema, not_null=False, boolean=False):
        acc = {
            "b": [],
            "s": [],
            "n": []
        }

        for term in self.terms:
            for t, v in term.to_sql(schema).items():
                acc[t].append(v)

        output = {}
        for t, terms in acc:
            if not terms:
                continue
            elif len(terms) == 1:
                output[t] = terms[0]
            else:
                output[t] = "COALESCE(" + ",".join(terms) + ")"

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

    def to_ruby(self, not_null=False, boolean=True):
        if not_null:
            return "false"
        else:
            if isinstance(self.field, Variable):
                return "doc[" + convert.string2quote(self.field.var) + "].isEmpty()"
            elif isinstance(self.field, Literal):
                return self.field.missing().to_ruby()
            else:
                return self.field.to_ruby() + " == null"

    def to_python(self, not_null=False, boolean=False):
        return self.field.to_python() + " == None"

    def to_sql(self, schema, not_null=False, boolean=False):
        field = self.field.to_sql(schema)
        acc = []
        for t, v in field.items():
            if t in ["b", "s", "n"]:
                acc.append("(" + v + " IS NULL)")

        if not acc:
            return "1"
        else:
            return " OR ".join(acc)

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

    def to_ruby(self, not_null=False, boolean=False):
        if isinstance(self.field, Variable):
            return "!doc["+convert.string2quote(self.field.var)+"].isEmpty()"
        elif isinstance(self.field, Literal):
            return self.field.exists().to_ruby()
        else:
            return self.field.to_ruby() + " != null"

    def to_python(self, not_null=False, boolean=False):
        return self.field.to_python() + " != None"

    def to_sql(self, schema, not_null=False, boolean=False):
        field = self.field.to_sql(schema)
        acc = []
        for t, v in field.items():
            if t in ["b", "s", "n"]:
                acc.append("(" + v + " IS NOT NULL)")

        if not acc:
            return "0"
        else:
            return " OR ".join(acc)

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

    def to_ruby(self, not_null=False, boolean=False):
        return "(" + self.field.to_ruby() + ").startsWith(" + self.prefix.to_ruby() + ")"

    def to_python(self, not_null=False, boolean=False):
        return "(" + self.field.to_python() + ").startswith(" + self.prefix.to_python() + ")"

    def to_sql(self, schema, not_null=False, boolean=False):
        return {"b": "INSTR(" + self.field.to_sql(schema).s + ", " + self.prefix.to_sql().s + ")==1"}

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

    def to_ruby(self, not_null=False, boolean=False):
        test_v = self.value.missing().to_ruby(boolean=True)
        test_l = self.length.missing().to_ruby(boolean=True)
        v = self.value.to_ruby(not_null=True)
        l = self.length.to_ruby(not_null=True)

        expr = "((" + test_v + ") || (" + test_l + ")) ? null : (" + v + ".substring(0, max(0, min(" + v + ".length(), " + l + ")).intValue()))"
        return expr

    def to_python(self, not_null=False, boolean=False):
        v = self.value.to_python()
        l = self.length.to_python()
        return "None if " + v + " == None or " + l + " == None else " + v + "[0:max(0, " + l + ")]"

    def to_sql(self, schema, not_null=False, boolean=False):
        v = self.value.to_sql(schema).s
        l = self.length.to_sql(schema).n
        return "CASE WHEN " + v + " IS NULL THEN NULL WHEN " + l + " IS NULL THEN NULL ELSE SUBSTR(" + v + ", 1, " + l + ") END"

    def to_dict(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            return {"left": {self.value.var: convert.json2value(self.length.json)}}
        else:
            return {"left": [self.value.to_dict(), self.length.to_dict()]}

    def vars(self):
        return self.value.vars() | self.length.vars()

    def map(self, map_):
        return LeftOp("left", [self.value.map(map_), self.length.map(map_)])

    def missing(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            return MissingOp(None, self.value)
        else:
            return OrOp(None, [self.value.missing(), self.length.missing()])


class NotLeftOp(Expression):
    has_simple_form = True

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        if isinstance(term, Mapping):
            self.value, self.length = term.items()[0]
        else:
            self.value, self.length = term

    def to_ruby(self, not_null=False, boolean=False):
        test_v = self.value.missing().to_ruby(boolean=True)
        test_l = self.length.missing().to_ruby(boolean=True)
        v = self.value.to_ruby(not_null=True)
        l = self.length.to_ruby(not_null=True)

        expr = "((" + test_v + ") || (" + test_l + ")) ? null : (" + v + ".substring(max(0, min(" + v + ".length(), " + l + ")).intValue()))"
        return expr

    def to_python(self, not_null=False, boolean=False):
        v = self.value.to_python()
        l = self.length.to_python()
        return "None if " + v + " == None or " + l + " == None else " + v + "[max(0, " + l + "):]"

    def to_dict(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            return {"not_left": {self.value.var: convert.json2value(self.length.json)}}
        else:
            return {"not_left": [self.value.to_dict(), self.length.to_dict()]}

    def vars(self):
        return self.value.vars() | self.length.vars()

    def map(self, map_):
        return NotLeftOp(None, [self.value.map(map_), self.length.map(map_)])

    def missing(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            return MissingOp(None, self.value)
        else:
            return OrOp(None, [self.value.missing(), self.length.missing()])


class RightOp(Expression):
    has_simple_form = True

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        if isinstance(term, Mapping):
            self.value, self.length = term.items()[0]
        else:
            self.value, self.length = term

    def to_ruby(self, not_null=False, boolean=False):
        test_v = self.value.missing().to_ruby(boolean=True)
        test_l = self.length.missing().to_ruby(boolean=True)
        v = self.value.to_ruby(not_null=True)
        l = self.length.to_ruby(not_null=True)

        expr = "((" + test_v + ") || (" + test_l + ")) ? null : (" + v + ".substring(min("+v+".length(), max(0, (" + v + ").length() - (" + l + "))).intValue()))"
        return expr

    def to_python(self, not_null=False, boolean=False):
        v = self.value.to_python()
        l = self.length.to_python()
        return "None if " + v + " == None or " + l + " == None else " + v + "[max(0, len(" + v + ")-(" + l + ")):]"

    def to_dict(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            return {"right": {self.value.var: convert.json2value(self.length.json)}}
        else:
            return {"right": [self.value.to_dict(), self.length.to_dict()]}

    def vars(self):
        return self.value.vars() | self.length.vars()

    def map(self, map_):
        return RightOp("right", [self.value.map(map_), self.length.map(map_)])

    def missing(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            return MissingOp(None, self.value)
        else:
            return OrOp(None, [self.value.missing(), self.length.missing()])

class NotRightOp(Expression):
    has_simple_form = True

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        if isinstance(term, Mapping):
            self.value, self.length = term.items()[0]
        else:
            self.value, self.length = term

    def to_ruby(self, not_null=False, boolean=False):
        test_v = self.value.missing().to_ruby(boolean=True)
        test_l = self.length.missing().to_ruby(boolean=True)
        v = self.value.to_ruby(not_null=True)
        l = self.length.to_ruby(not_null=True)

        expr = "((" + test_v + ") || (" + test_l + ")) ? null : (" + v + ".substring(0, min("+v+".length(), max(0, (" + v + ").length() - (" + l + "))).intValue()))"
        return expr

    def to_python(self, not_null=False, boolean=False):
        v = self.value.to_python()
        l = self.length.to_python()
        return "None if " + v + " == None or " + l + " == None else " + v + "[0:max(0, len("+v+")-(" + l + "))]"

    def to_dict(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            return {"not_right": {self.value.var: convert.json2value(self.length.json)}}
        else:
            return {"not_right": [self.value.to_dict(), self.length.to_dict()]}

    def vars(self):
        return self.value.vars() | self.length.vars()

    def map(self, map_):
        return NotRightOp(None, [self.value.map(map_), self.length.map(map_)])

    def missing(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            return MissingOp(None, self.value)
        else:
            return OrOp(None, [self.value.missing(), self.length.missing()])


class InOp(Expression):
    has_simple_form = True

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        self.field, self.values = term

    def to_ruby(self, not_null=False, boolean=False):
        return self.values.to_ruby() + ".contains(" + self.field.to_ruby() + ")"

    def to_python(self, not_null=False, boolean=False):
        return self.field.to_python() + " in " + self.values.to_python()

    def to_sql(self, schema, not_null=False, boolean=False):
        if not isinstance(self.values, Literal):
            Log.error("Not supported")
        var = self.field.to_sql(schema)
        return " OR ".join("(" + var + "==" + sql_quote(v) + ")" for v in convert.json2value(self.values))

    def to_esfilter(self):
        if isinstance(self.field, Variable):
            return {"terms": {self.field.var: convert.json2value(self.values.json)}}
        else:
            return {"script": self.to_ruby()}

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

    def __new__(cls, op, term, *args):
        Expression.__new__(cls, *args)
        field, comparisons = term  # comparisons IS A Literal()
        return AndOp("and", [operators[op](op, [field, Literal(None, value)]) for op, value in convert.json2value(comparisons.json).items()])

    def __init__(self, op, term):
        Log.error("Should never happen!")


class WhenOp(Expression):
    def __init__(self, op, term, **clauses):
        Expression.__init__(self, op, [term])
        self.when = term
        self.then = coalesce(clauses.get("then"), NullOp())
        self.els_ = coalesce(clauses.get("else"), NullOp())

    def to_ruby(self, not_null=False, boolean=False):
        return "(" + self.when.to_ruby(boolean=True) + ") ? (" + self.then.to_ruby(not_null=not_null) + ") : (" + self.els_.to_ruby(not_null=not_null) + ")"

    def to_python(self, not_null=False, boolean=False):
        return "(" + self.when.to_python(boolean=True) + ") ? (" + self.then.to_python(not_null=not_null) + ") : (" + self.els_.to_python(not_null=not_null) + ")"

    def to_sql(self, schema, not_null=False, boolean=False):
        when = self.when.to_sql(boolean=True)
        then = self.then.to_sql(not_null=not_null)
        els_ = self.els_.to_sql(not_null=not_null)
        output = {}
        for t in "bsn":
            if then[t] == None:
                if els_[t] == None:
                    pass
                else:
                    output[t] = "CASE WHEN " + when.b + " THEN NULL ELSE " + els_[t] + " END"
            else:
                if els_[t] == None:
                    output[t] = "CASE WHEN " + when.b + " THEN " + then[t] + " END"
                else:
                    output[t] = "CASE WHEN " + when.b + " THEN " + then[t] + " ELSE " + els_[t] + " END"
        return output

    def to_esfilter(self):
        return {"or": [
            {"and": [
                self.when.to_esfilter(),
                self.then.to_esfilter()
            ]},
            {"and": [
                {"not": self.when.to_esfilter()},
                self.els_.to_esfilter()
            ]}
        ]}
        # return {"script": {"script": self.to_ruby()}}

    def to_dict(self):
        return {"when": self.when.to_dict(), "then": self.then.to_dict() if self.then else None, "else": self.els_.to_dict() if self.els_ else None}

    def vars(self):
        return self.when.vars() | self.then.vars() | self.els_.vars()

    def map(self, map_):
        return WhenOp("when", self.when.map(map_), **{"then": self.then.map(map_), "else": self.els_.map(map_)})

    def missing(self):
        if self.then.missing() or self.els_.missing():
            return WhenOp("when", self.when, **{"then": self.then.missing(), "else": self.els_.missing()})
        else:
            return FalseOp()


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

    def to_ruby(self, not_null=False, boolean=False):
        acc = self.whens[-1].to_ruby()
        for w in reversed(self.whens[0:-1]):
            acc = "(" + w.when.to_ruby(boolean=True) + ") ? (" + w.then.to_ruby() + ") : (" + acc + ")"
        return acc

    def to_python(self, not_null=False, boolean=False):
        acc = self.whens[-1].to_python()
        for w in reversed(self.whens[0:-1]):
            acc = "(" + w.when.to_python(boolean=True) + ") ? (" + w.then.to_python() + ") : (" + acc + ")"
        return acc

    def to_sql(self, schema, not_null=False, boolean=False):
        output = {}
        for t in "bsn":  # EXPENSIVE LOOP to_sql() RUN 3 TIMES
            acc = " ELSE " + self.whens[-1].to_sql(schema)[t] + " END"
            for w in reversed(self.whens[0:-1]):
                acc = " WHEN " + w.when.to_sql(boolean=True).b + " THEN " + w.then.to_sql(schema)[t] + acc
            output[t]="CASE" + acc
        return output

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
        return MissingOp("missing", self)


USE_BOOL_MUST = True

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

        Log.unexpected("programmer error", cause=e)


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

        if coalesce(esfilter["and"], esfilter.bool.must):
            terms = coalesce(esfilter["and"], esfilter.bool.must)
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
    "add": MultiOp,
    "and": AndOp,
    "case": CaseOp,
    "coalesce": CoalesceOp,
    "contains": ContainsOp,
    "count": CountOp,
    "date": DateOp,
    "div": DivOp,
    "divide": DivOp,
    "eq": EqOp,
    "exists": ExistsOp,
    "exp": BinaryOp,
    "floor": FloorOp,
    "gt": InequalityOp,
    "gte": InequalityOp,
    "in": InOp,
    "instr": ContainsOp,
    "left": LeftOp,
    "length": LengthOp,
    "literal": Literal,
    "lt": InequalityOp,
    "lte": InequalityOp,
    "match_all": TrueOp,
    "minus": BinaryOp,
    "missing": MissingOp,
    "mod": BinaryOp,
    "mul": MultiOp,
    "mult": MultiOp,
    "multiply": MultiOp,
    "ne": NeOp,
    "neq": NeOp,
    "not": NotOp,
    "not_left": NotLeftOp,
    "not_right": NotRightOp,
    "null": NullOp,
    "number": NumberOp,
    "or": OrOp,
    "prefix": PrefixOp,
    "range": RangeOp,
    "regex": RegExpOp,
    "regexp": RegExpOp,
    "right": RightOp,
    "script": ScriptOp,
    "string": StringOp,
    "sub": BinaryOp,
    "subtract": BinaryOp,
    "sum": MultiOp,
    "term": EqOp,
    "terms": InOp,
    "tuple": TupleOp,
    "when": WhenOp,
}


def sql_quote(value):
    if value == Null:
        return "NULL"
    elif value is True:
        return "0"
    elif value is False:
        return "1"
    elif isinstance(value, unicode):
        return "'" + value.replace("'", "''") + "'"
    else:
        return unicode(value)


json_type_to_sql_type = {
    "string": "s",
    "number": "n",
    "object": "j",
    "boolean": "b",
    "nested": "j"
}

sql_type_to_json_type = {
    "s": "string",
    "n": "number",
    "j": "object",
    "b": "boolean"
}


def wrap_nested_path(nested_path):
    return listwrap(nested_path) + ["."]


def unwrap_nested_path(nested_path):
    if unwrap(nested_path)[-1] == ".":
        nested_path = nested_path[:-1]

    return unwraplist(nested_path)
