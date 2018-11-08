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

import operator
import re
from collections import Mapping
from decimal import Decimal

import mo_json
from jx_base import first
from jx_base.queries import is_variable_name, get_property_name
from mo_dots import coalesce, wrap, Null, split_field
from mo_future import text_type, utf8_json_encoder, get_function_name, zip_longest
from mo_json import scrub, IS_NULL, OBJECT, BOOLEAN, NUMBER, INTEGER, STRING, python_type_to_json_type
from mo_json.typed_encoder import inserter_type_to_json_type
from mo_logs import Log, Except
from mo_math import Math, MAX, MIN, UNION
from mo_times.dates import Date, unicode2Date

ALLOW_SCRIPTING = False
EMPTY_DICT = {}


def extend(cls):
    """
    DECORATOR TO ADD METHODS TO CLASSES
    :param cls: THE CLASS TO ADD THE METHOD TO
    :return:
    """

    def extender(func):
        setattr(cls, get_function_name(func), func)
        return func

    return extender


def last(values):
    if len(values):
        return values[-1]
    else:
        return Null


def simplified(func):
    def mark_as_simple(self):
        if self.simplified:
            return self

        output = func(self)
        output.simplified = True
        return output

    func_name = get_function_name(func)
    mark_as_simple.__name__ = func_name
    return mark_as_simple


def jx_expression(expr, schema=None):
    # UPDATE THE VARIABLE WITH THIER KNOWN TYPES
    output = _jx_expression(expr)
    if not schema:
        return output
    for v in output.vars():
        leaves = schema.leaves(v.var)
        if len(leaves) == 0:
            v.data_type = IS_NULL
        if len(leaves) == 1:
            v.data_type = first(leaves).jx_type
    return output


def _jx_expression(expr):
    """
    WRAP A JSON EXPRESSION WITH OBJECT REPRESENTATION
    """
    if isinstance(expr, Expression):
        Log.error("Expecting JSON, not expression")

    if expr is None:
        return TRUE
    elif expr in (True, False, None) or expr == None or isinstance(expr, (float, int, Decimal, Date)):
        return Literal(expr)
    elif isinstance(expr, text_type):
        return Variable(expr)
    elif isinstance(expr, (list, tuple)):
        return TupleOp(list(map(jx_expression, expr)))  # FORMALIZE

    expr = wrap(expr)
    try:
        items = expr.items()
    except Exception as e:
        Log.error("programmer error expr = {{value|quote}}", value=expr, cause=e)

    for item in items:
        op, term = item
        class_ = operators.get(op)
        if class_:
            return class_.define(expr)
    else:
        if not items:
            return NULL
        raise Log.error("{{operator|quote}} is not a known operator", operator=op)


class Expression(object):
    data_type = OBJECT
    has_simple_form = False

    def __init__(self, terms):
        self.simplified = False
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

    @classmethod
    def define(cls, expr):
        """
        GENERAL SUPPORT FOR BUILDING EXPRESSIONS FROM JSON EXPRESSIONS
        OVERRIDE THIS IF AN OPERATOR EXPECTS COMPLICATED PARAMETERS
        :param expr: Data representing a JSON Expression
        :return: Python parse tree
        """

        try:
            items = expr.items()
        except Exception as e:
            Log.error("programmer error expr = {{value|quote}}", value=expr, cause=e)

        for item in items:
            op, term = item
            class_ = operators.get(op)
            if class_:
                clauses = {k: jx_expression(v) for k, v in expr.items() if k != op}
                break
        else:
            if not items:
                return NULL
            raise Log.error("{{operator|quote}} is not a known operator", operator=op)

        if term == None:
            return class_([], **clauses)
        elif isinstance(term, list):
            terms = list(map(_jx_expression, term))
            return class_(terms, **clauses)
        elif isinstance(term, Mapping):
            items = term.items()
            if class_.has_simple_form:
                if len(items) == 1:
                    k, v = items[0]
                    return class_([Variable(k), Literal(v)], **clauses)
                else:
                    return class_({k: Literal(v) for k, v in items}, **clauses)
            else:
                return class_(jx_expression(term), **clauses)
        else:
            if op in ["literal", "date", "offset"]:
                return class_(term, **clauses)
            else:
                return class_(_jx_expression(term), **clauses)

    @property
    def name(self):
        return self.__class__.__name__

    @property
    def many(self):
        """
        :return: True IF THE EXPRESSION RETURNS A MULTIVALUE (WHICH IS NOT A LIST OR A TUPLE)
        """
        return False

    def __data__(self):
        raise NotImplementedError

    def vars(self):
        raise Log.error("{{type}} has no `vars` method", type=self.__class__.__name__)

    def map(self, map):
        raise Log.error("{{type}} has no `map` method", type=self.__class__.__name__)

    def missing(self):
        """
        THERE IS PLENTY OF OPPORTUNITY TO SIMPLIFY missing EXPRESSIONS
        OVERRIDE THIS METHOD TO SIMPLIFY
        :return:
        """
        if self.type == BOOLEAN:
            Log.error("programmer error")
        return MissingOp(self)

    def exists(self):
        """
        THERE IS PLENTY OF OPPORTUNITY TO SIMPLIFY exists EXPRESSIONS
        OVERRIDE THIS METHOD TO SIMPLIFY
        :return:
        """
        return NotOp(self.missing())

    def is_true(self):
        """
        :return: True, IF THIS EXPRESSION ALWAYS RETURNS BOOLEAN true
        """
        return FALSE  # GOOD DEFAULT ASSUMPTION

    def is_false(self):
        """
        :return: True, IF THIS EXPRESSION ALWAYS RETURNS BOOLEAN false
        """
        return FALSE  # GOOD DEFAULT ASSUMPTION

    def partial_eval(self):
        """
        ATTEMPT TO SIMPLIFY THE EXPRESSION:
        PREFERABLY RETURNING A LITERAL, BUT MAYBE A SIMPLER EXPRESSION, OR self IF NOT POSSIBLE
        """
        return self

    @property
    def type(self):
        return self.data_type

    def __eq__(self, other):
        self_class = self.__class__
        Log.note("this is slow on {{type}}", type=text_type(self_class.__name__))
        if other is None:
            return False
        if not isinstance(other, self_class):
            return False
        return self.__data__() == other.__data__()


class Variable(Expression):

    def __init__(self, var):
        """
        :param var:  DOT DELIMITED PATH INTO A DOCUMENT

        """
        Expression.__init__(self, None)
        self.var = get_property_name(var)
        jx_type = inserter_type_to_json_type.get(last(split_field(var)))
        if jx_type:
            self.data_type = jx_type

    def __call__(self, row, rownum=None, rows=None):
        path = split_field(self.var)
        for p in path:
            row = row.get(p)
            if row is None:
                return None
        if isinstance(row, list) and len(row) == 1:
            return row[0]
        return row

    def __data__(self):
        return self.var

    @property
    def many(self):
        return True

    def vars(self):
        return {self}

    def map(self, map_):
        return Variable(coalesce(map_.get(self.var), self.var))

    def __hash__(self):
        return self.var.__hash__()

    def __eq__(self, other):
        if isinstance(other, Variable):
            return self.var == other.var
        elif isinstance(other, text_type):
            return self.var == other
        return False

    def __unicode__(self):
        return self.var

    def __str__(self):
        return str(self.var)

    def missing(self):
        if self.var == "_id":
            return FALSE
        else:
            return MissingOp(self)


IDENTITY = Variable(".")


class OffsetOp(Expression):
    """
    OFFSET INDEX INTO A TUPLE
    """

    def __init__(self, var):
        Expression.__init__(self, None)
        if not Math.is_integer(var):
            Log.error("Expecting an integer")
        self.var = var

    def __call__(self, row, rownum=None, rows=None):
        try:
            return row[self.var]
        except Exception:
            return None

    def __data__(self):
        return {"offset": self.var}

    def vars(self):
        return {}

    def __hash__(self):
        return self.var.__hash__()

    def __eq__(self, other):
        return self.var == other

    def __unicode__(self):
        return text_type(self.var)

    def __str__(self):
        return str(self.var)


class RowsOp(Expression):
    has_simple_form = True

    def __init__(self, term):
        Expression.__init__(self, term)
        self.var, self.offset = term
        if isinstance(self.var, Variable):
            if isinstance(self.var, Variable) and not any(self.var.var.startswith(p) for p in ["row.", "rows.", "rownum"]):  # VARIABLES ARE INTERPRETED LITERALLY
                self.var = Literal(self.var.var)
            else:
                Log.error("can not handle")
        else:
            Log.error("can not handle")

    def __data__(self):
        if isinstance(self.var, Literal) and isinstance(self.offset, Literal):
            return {"rows": {self.var.json, self.offset.value}}
        else:
            return {"rows": [self.var.__data__(), self.offset.__data__()]}

    def vars(self):
        return self.var.vars() | self.offset.vars() | {"rows", "rownum"}

    def map(self, map_):
        return RowsOp([self.var.map(map_), self.offset.map(map_)])


class GetOp(Expression):
    has_simple_form = True

    def __init__(self, term):
        Expression.__init__(self, term)
        self.var, self.offset = term

    def __data__(self):
        if isinstance(self.var, Literal) and isinstance(self.offset, Literal):
            return {"get": {self.var.json, self.offset.value}}
        else:
            return {"get": [self.var.__data__(), self.offset.__data__()]}

    def vars(self):
        return self.var.vars() | self.offset.vars()

    def map(self, map_):
        return GetOp([self.var.map(map_), self.offset.map(map_)])


class SelectOp(Expression):
    has_simple_form = True

    def __init__(self, terms):
        self.terms = terms

    @classmethod
    def define(cls, expr):
        term = expr.select
        terms = []
        if not isinstance(term, list):
            raise Log.error("Expecting a list")
        for t in term:
            if isinstance(t, text_type):
                if not is_variable_name(t):
                    Log.error("expecting {{value}} a simple dot-delimited path name", value=t)
                terms.append({"name": t, "value": jx_expression(t)})
            elif t.name == None:
                if t.value == None:
                    Log.error("expecting select parameters to have name and value properties")
                elif isinstance(t.value, text_type):
                    if not is_variable_name(t):
                        Log.error("expecting {{value}} a simple dot-delimited path name", value=t.value)
                    else:
                        terms.append({"name": t.value, "value": jx_expression(t.value)})
                else:
                    Log.error("expecting a name property")
            else:
                terms.append({"name": t.name, "value": jx_expression(t.value)})
        return SelectOp(terms)

    def __data__(self):
        return {"select": [
            {
                "name": t.name.__data__(),
                "value": t.value.__data__()
            }
            for t in self.terms
        ]}

    def vars(self):
        return UNION(t.value for t in self.terms)

    def map(self, map_):
        return SelectOp([
            {"name": t.name, "value": t.value.map(map_)}
            for t in self.terms
        ])


class ScriptOp(Expression):
    """
    ONLY FOR WHEN YOU TRUST THE SCRIPT SOURCE
    """

    def __init__(self, script, data_type=OBJECT):
        Expression.__init__(self, None)
        if not isinstance(script, text_type):
            Log.error("expecting text of a script")
        self.simplified = True
        self.script = script
        self.data_type = data_type

    @classmethod
    def define(cls, expr):
        if ALLOW_SCRIPTING:
            Log.warning("Scripting has been activated:  This has known security holes!!\nscript = {{script|quote}}", script=expr.script.term)
            return ScriptOp(expr.script)
        else:
            Log.error("scripting is disabled")

    def vars(self):
        return set()

    def map(self, map_):
        return self

    def __unicode__(self):
        return self.script

    def __str__(self):
        return str(self.script)


_json_encoder = utf8_json_encoder


def value2json(value):
    try:
        scrubbed = scrub(value, scrub_number=float)
        return text_type(_json_encoder(scrubbed))
    except Exception as e:
        e = Except.wrap(e)
        Log.warning("problem serializing {{type}}", type=text_type(repr(value)), cause=e)
        raise e


class Literal(Expression):
    """
    A literal JSON document
    """

    def __new__(cls, term):
        if term == None:
            return NULL
        if term is True:
            return TRUE
        if term is False:
            return FALSE
        if isinstance(term, Mapping) and term.date:
            # SPECIAL CASE
            return DateOp(term.date)
        return object.__new__(cls)

    def __init__(self, term):
        Expression.__init__(self, None)
        self.simplified = True
        self.term = term

    @classmethod
    def define(cls, expr):
        return Literal(expr.literal)

    def __nonzero__(self):
        return True

    def __eq__(self, other):
        if other == None:
            if self.term == None:
                return True
            else:
                return False
        elif self.term == None:
            return False

        if isinstance(other, Literal):
            return (self.term == other.term) or (self.json == other.json)

    def __data__(self):
        return {"literal": self.value}

    @property
    def value(self):
        return self.term

    @property
    def json(self):
        if self.term == "":
            self._json = '""'
        else:
            self._json = value2json(self.term)

        return self._json

    def vars(self):
        return set()

    def map(self, map_):
        return self

    def missing(self):
        if self.term in [None, Null]:
            return TRUE
        if self.value == '':
            return TRUE
        return FALSE

    def __call__(self, row=None, rownum=None, rows=None):
        return self.value

    def __unicode__(self):
        return self._json

    def __str__(self):
        return str(self._json)

    @property
    def type(self):
        return python_type_to_json_type[self.term.__class__]

    def partial_eval(self):
        return self


ZERO = Literal(0)
ONE = Literal(1)


class NullOp(Literal):
    """
    FOR USE WHEN EVERYTHING IS EXPECTED TO BE AN Expression
    USE IT TO EXPECT A NULL VALUE IN assertAlmostEqual
    """
    data_type = OBJECT

    @classmethod
    def define(cls, expr):
        return NULL

    def __new__(cls, *args, **kwargs):
        return object.__new__(cls, *args, **kwargs)

    def __init__(self, op=None, term=None):
        Literal.__init__(self, None)

    def __nonzero__(self):
        return False

    def __eq__(self, other):
        return other == None

    def __gt__(self, other):
        return False

    def __lt__(self, other):
        return False

    def __ge__(self, other):
        if other == None:
            return True
        return False

    def __le__(self, other):
        if other == None:
            return True
        return False

    def __data__(self):
        return {"null": {}}

    def vars(self):
        return set()

    def map(self, map_):
        return self

    def missing(self):
        return TRUE

    def exists(self):
        return FALSE

    def __call__(self, row=None, rownum=None, rows=None):
        return Null

    def __unicode__(self):
        return "null"

    def __str__(self):
        return b"null"

    def __data__(self):
        return None


NULL = NullOp()


class TrueOp(Literal):
    data_type = BOOLEAN

    def __new__(cls, *args, **kwargs):
        return object.__new__(cls, *args, **kwargs)

    def __init__(self, op=None, term=None):
        Literal.__init__(self, True)

    @classmethod
    def define(cls, expr):
        return TRUE

    def __nonzero__(self):
        return True

    def __eq__(self, other):
        return (other is TRUE) or (other is True)

    def __data__(self):
        return True

    def vars(self):
        return set()

    def map(self, map_):
        return self

    def missing(self):
        return FALSE

    def is_true(self):
        return TRUE

    def is_false(self):
        return FALSE

    def __call__(self, row=None, rownum=None, rows=None):
        return True

    def __unicode__(self):
        return "true"

    def __str__(self):
        return b"true"


TRUE = TrueOp()


class FalseOp(Literal):
    data_type = BOOLEAN

    def __new__(cls, *args, **kwargs):
        return object.__new__(cls, *args, **kwargs)

    def __init__(self, op=None, term=None):
        Literal.__init__(self, False)

    @classmethod
    def define(cls, expr):
        return FALSE

    def __nonzero__(self):
        return False

    def __eq__(self, other):
        return (other is FALSE) or (other is False)

    def __data__(self):
        return False

    def vars(self):
        return set()

    def map(self, map_):
        return self

    def missing(self):
        return FALSE

    def is_true(self):
        return FALSE

    def is_false(self):
        return TRUE

    def __call__(self, row=None, rownum=None, rows=None):
        return False

    def __unicode__(self):
        return "false"

    def __str__(self):
        return b"false"


FALSE = FalseOp()


class DateOp(Literal):
    date_type = NUMBER

    def __init__(self, term):
        if hasattr(self, "date"):
            return
        if isinstance(term, text_type):
            self.date = term
        else:
            self.date = coalesce(term.literal, term)
        v = unicode2Date(self.date)
        if isinstance(v, Date):
            Literal.__init__(self, v.unix)
        else:
            Literal.__init__(self, v.seconds)

    @classmethod
    def define(cls, expr):
        return DateOp(expr.date)

    def __data__(self):
        return {"date": self.date}

    def __call__(self, row=None, rownum=None, rows=None):
        return Date(self.date)


class TupleOp(Expression):
    date_type = OBJECT

    def __init__(self, terms):
        Expression.__init__(self, terms)
        if terms == None:
            self.terms = []
        elif isinstance(terms, list):
            self.terms = terms
        else:
            self.terms = [terms]

    def __data__(self):
        return {"tuple": [t.__data__() for t in self.terms]}

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output

    def map(self, map_):
        return TupleOp([t.map(map_) for t in self.terms])

    def missing(self):
        return FALSE


class LeavesOp(Expression):
    date_type = OBJECT

    def __init__(self, term, prefix=None):
        Expression.__init__(self, term)
        self.term = term
        self.prefix = prefix

    def __data__(self):
        if self.prefix:
            return {"leaves": self.term.__data__(), "prefix": self.prefix}
        else:
            return {"leaves": self.term.__data__()}

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return LeavesOp(self.term.map(map_))

    def missing(self):
        return FALSE


class BaseBinaryOp(Expression):
    has_simple_form = True
    data_type = NUMBER
    op = None

    def __init__(self, terms, default=NULL):
        Expression.__init__(self, terms)
        self.lhs, self.rhs = terms
        self.default = default

    @property
    def name(self):
        return self.op

    def __data__(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {self.op: {self.lhs.var, self.rhs.value}, "default": self.default}
        else:
            return {self.op: [self.lhs.__data__(), self.rhs.__data__()], "default": self.default}

    def vars(self):
        return self.lhs.vars() | self.rhs.vars() | self.default.vars()

    def map(self, map_):
        return self.__class__([self.lhs.map(map_), self.rhs.map(map_)], default=self.default.map(map_))

    def missing(self):
        if self.default.exists():
            return FALSE
        else:
            return OrOp([self.lhs.missing(), self.rhs.missing()])

    def partial_eval(self):
        lhs = self.lhs.partial_eval()
        rhs = self.rhs.partial_eval()
        default = self.default.partial_eval()
        if isinstance(lhs, Literal) and isinstance(rhs, Literal):
            return Literal(builtin_ops[self.op](lhs.value, rhs.value))
        return self.__class__([lhs, rhs], default=default)


class SubOp(BaseBinaryOp):
    op = "sub"


class ExpOp(BaseBinaryOp):
    op = "exp"


class ModOp(BaseBinaryOp):
    op = "mod"


class DivOp(BaseBinaryOp):
    op = "div"

    def missing(self):
        return AndOp([
            self.default.missing(),
            OrOp([self.lhs.missing(), self.rhs.missing(), EqOp([self.rhs, ZERO])])
        ]).partial_eval()

    def partial_eval(self):
        default = self.default.partial_eval()
        rhs = self.rhs.partial_eval()
        if rhs is ZERO:
            return default
        lhs = self.lhs.partial_eval()
        if isinstance(lhs, Literal) and isinstance(rhs, Literal):
            return Literal(builtin_ops[self.op](lhs.value, rhs.value))
        return self.__class__([lhs, rhs], default=default)


class BaseInequalityOp(Expression):
    has_simple_form = True
    data_type = BOOLEAN
    op = None

    def __init__(self, terms):
        Expression.__init__(self, terms)
        self.lhs, self.rhs = terms

    @property
    def name(self):
        return self.op

    def __data__(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {self.op: {self.lhs.var, self.rhs.value}}
        else:
            return {self.op: [self.lhs.__data__(), self.rhs.__data__()]}

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.op == other.op and self.lhs == other.lhs and self.rhs == other.rhs

    def vars(self):
        return self.lhs.vars() | self.rhs.vars()

    def map(self, map_):
        return self.__class__([self.lhs.map(map_), self.rhs.map(map_)])

    def missing(self):
        return FALSE

    @simplified
    def partial_eval(self):
        lhs = self.lhs.partial_eval()
        rhs = self.rhs.partial_eval()

        if isinstance(lhs, Literal) and isinstance(rhs, Literal):
            return Literal(builtin_ops[self.op](lhs, rhs))

        return self.__class__([lhs, rhs])


class GtOp(BaseInequalityOp):
    op = "gt"


class GteOp(BaseInequalityOp):
    op = "gte"


class LtOp(BaseInequalityOp):
    op = "lt"


class LteOp(BaseInequalityOp):
    op = "lte"


class FloorOp(Expression):
    has_simple_form = True
    data_type = NUMBER

    def __init__(self, terms, default=NULL):
        Expression.__init__(self, terms)
        if len(terms) == 1:
            self.lhs = terms[0]
            self.rhs = ONE
        else:
            self.lhs, self.rhs = terms
        self.default = default

    def __data__(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {"floor": {self.lhs.var, self.rhs.value}, "default": self.default}
        else:
            return {"floor": [self.lhs.__data__(), self.rhs.__data__()], "default": self.default}

    def vars(self):
        return self.lhs.vars() | self.rhs.vars() | self.default.vars()

    def map(self, map_):
        return FloorOp([self.lhs.map(map_), self.rhs.map(map_)], default=self.default.map(map_))

    def missing(self):
        if self.default.exists():
            return FALSE
        else:
            return OrOp([self.lhs.missing(), self.rhs.missing(), EqOp([self.rhs, ZERO])])


class EqOp(Expression):
    has_simple_form = True
    data_type = BOOLEAN

    def __new__(cls, terms):
        if isinstance(terms, list):
            return object.__new__(cls)

        items = terms.items()
        if len(items) == 1:
            if isinstance(items[0][1], list):
                return InOp(items[0])
            else:
                return EqOp(items[0])
        else:
            acc = []
            for lhs, rhs in items:
                if rhs.json.startswith("["):
                    acc.append(InOp([Variable(lhs), rhs]))
                else:
                    acc.append(EqOp([Variable(lhs), rhs]))
            return AndOp(acc)

    def __init__(self, terms):
        Expression.__init__(self, terms)
        self.lhs, self.rhs = terms

    def __data__(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {"eq": {self.lhs.var, self.rhs.value}}
        else:
            return {"eq": [self.lhs.__data__(), self.rhs.__data__()]}

    def __eq__(self, other):
        if isinstance(other, EqOp):
            return self.lhs == other.lhs and self.rhs == other.rhs
        return False

    def vars(self):
        return self.lhs.vars() | self.rhs.vars()

    def map(self, map_):
        return EqOp([self.lhs.map(map_), self.rhs.map(map_)])

    def missing(self):
        return FALSE

    def exists(self):
        return TRUE

    @simplified
    def partial_eval(self):
        lhs = self.lhs.partial_eval()
        rhs = self.rhs.partial_eval()

        if isinstance(lhs, Literal) and isinstance(rhs, Literal):
            return TRUE if builtin_ops["eq"](lhs.value, rhs.value) else FALSE
        else:
            return CaseOp(
                [
                    WhenOp(lhs.missing(), **{"then": rhs.missing()}),
                    WhenOp(rhs.missing(), **{"then": FALSE}),
                    BasicEqOp([lhs, rhs])
                ]
            ).partial_eval()


class NeOp(Expression):
    has_simple_form = True
    data_type = BOOLEAN

    def __init__(self, terms):
        Expression.__init__(self, terms)
        if isinstance(terms, (list, tuple)):
            self.lhs, self.rhs = terms
        elif isinstance(terms, Mapping):
            self.rhs, self.lhs = terms.items()[0]
        else:
            Log.error("logic error")

    def __data__(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {"ne": {self.lhs.var, self.rhs.value}}
        else:
            return {"ne": [self.lhs.__data__(), self.rhs.__data__()]}

    def vars(self):
        return self.lhs.vars() | self.rhs.vars()

    def map(self, map_):
        return NeOp([self.lhs.map(map_), self.rhs.map(map_)])

    def missing(self):
        return FALSE  # USING THE decisive EQUAILTY https://github.com/mozilla/jx-sqlite/blob/master/docs/Logical%20Equality.md#definitions

    @simplified
    def partial_eval(self):
        output = NotOp(EqOp([self.lhs, self.rhs])).partial_eval()
        return output


class NotOp(Expression):
    data_type = BOOLEAN

    def __init__(self, term):
        Expression.__init__(self, term)
        self.term = term

    def __data__(self):
        return {"not": self.term.__data__()}

    def __eq__(self, other):
        if not isinstance(other, NotOp):
            return False
        return self.term == other.term

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return NotOp(self.term.map(map_))

    def missing(self):
        return self.term.missing()

    @simplified
    def partial_eval(self):
        def inverse(term):
            if term is TRUE:
                return FALSE
            elif term is FALSE:
                return TRUE
            elif isinstance(term, NullOp):
                return TRUE
            elif isinstance(term, Literal):
                Log.error("`not` operator expects a Boolean term")
            elif isinstance(term, WhenOp):
                output = WhenOp(
                    term.when,
                    **{"then": inverse(term.then), "else": inverse(term.els_)}
                ).partial_eval()
            elif isinstance(term, CaseOp):
                output = CaseOp(
                    [
                        WhenOp(w.when, **{"then": inverse(w.then)}) if isinstance(w, WhenOp) else inverse(w)
                        for w in term.whens
                    ]
                ).partial_eval()
            elif isinstance(term, AndOp):
                output = OrOp([inverse(t) for t in term.terms]).partial_eval()
            elif isinstance(term, OrOp):
                output = AndOp([inverse(t) for t in term.terms]).partial_eval()
            elif isinstance(term, MissingOp):
                output = NotOp(term.expr.missing())
            elif isinstance(term, ExistsOp):
                output = term.field.missing().partial_eval()
            elif isinstance(term, NotOp):
                output = term.term.partial_eval()
            elif isinstance(term, NeOp):
                output = EqOp([term.lhs, term.rhs]).partial_eval()
            elif isinstance(term, (BasicIndexOfOp, BasicSubstringOp)):
                return FALSE
            else:
                output = NotOp(term)

            return output

        output = inverse(self.term.partial_eval())
        return output


class AndOp(Expression):
    data_type = BOOLEAN

    def __init__(self, terms):
        Expression.__init__(self, terms)
        if terms == None:
            self.terms = []
        elif isinstance(terms, list):
            self.terms = terms
        else:
            self.terms = [terms]

    def __data__(self):
        return {"and": [t.__data__() for t in self.terms]}

    def __eq__(self, other):
        if isinstance(other, AndOp):
            return all(a == b for a, b in zip_longest(self.terms, other.terms))
        return False

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output

    def map(self, map_):
        return AndOp([t.map(map_) for t in self.terms])

    def missing(self):
        return FALSE

    @simplified
    def partial_eval(self):
        or_terms = [[]]  # LIST OF TUPLES FOR or-ing and and-ing
        for i, t in enumerate(self.terms):
            simple = BooleanOp(t).partial_eval()
            if simple is TRUE:
                continue
            elif simple is FALSE:
                return FALSE
            elif isinstance(simple, AndOp):
                for and_terms in or_terms:
                    and_terms.extend([tt for tt in simple.terms if tt not in and_terms])
                continue
            elif isinstance(simple, OrOp):
                or_terms = [
                    and_terms + [o]
                    for o in simple.terms
                    for and_terms in or_terms
                ]
                continue
            elif simple.type != BOOLEAN:
                Log.error("expecting boolean value")

            for and_terms in list(or_terms):
                if NotOp(simple).partial_eval() in and_terms:
                    or_terms.remove(and_terms)
                elif simple not in and_terms:
                    and_terms.append(simple)

        if len(or_terms) == 0:
            return FALSE
        elif len(or_terms) == 1:
            and_terms = or_terms[0]
            if len(and_terms) == 0:
                return TRUE
            elif len(and_terms) == 1:
                return and_terms[0]
            else:
                return AndOp(and_terms)

        return OrOp([
            AndOp(and_terms) if len(and_terms) > 1 else and_terms[0]
            for and_terms in or_terms
        ])


class OrOp(Expression):
    data_type = BOOLEAN

    def __init__(self, terms):
        Expression.__init__(self, terms)
        self.terms = terms

    def __data__(self):
        return {"or": [t.__data__() for t in self.terms]}

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output

    def map(self, map_):
        return OrOp([t.map(map_) for t in self.terms])

    def missing(self):
        return FALSE

    def __call__(self, row=None, rownum=None, rows=None):
        return any(t(row, rownum, rows) for t in self.terms)

    def __eq__(self, other):
        if not isinstance(other, OrOp):
            return False
        if len(self.terms) != len(other.terms):
            return False
        return all(t == u for t, u in zip(self.terms, other.terms))

    @simplified
    def partial_eval(self):
        terms = []
        ands = []
        for t in self.terms:
            simple = t.partial_eval()
            if simple is TRUE:
                return TRUE
            elif simple in (FALSE, NULL):
                pass
            elif isinstance(simple, OrOp):
                terms.extend(tt for tt in simple.terms if tt not in terms)
            elif isinstance(simple, AndOp):
                ands.append(simple)
            elif simple.type != BOOLEAN:
                Log.error("expecting boolean value")
            elif simple not in terms:
                terms.append(simple)

        if ands:  # REMOVE TERMS THAT ARE MORE RESTRICTIVE THAN OTHERS
            for a in ands:
                for tt in a.terms:
                    if tt in terms:
                        break
                else:
                    terms.append(a)

        if len(terms) == 0:
            return FALSE
        if len(terms) == 1:
            return terms[0]
        return OrOp(terms)


class LengthOp(Expression):
    data_type = INTEGER

    def __init__(self, term):
        Expression.__init__(self, [term])
        self.term = term

    def __eq__(self, other):
        if isinstance(other, LengthOp):
            return self.term == other.term

    def __data__(self):
        return {"length": self.term.__data__()}

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return LengthOp(self.term.map(map_))

    def missing(self):
        return self.term.missing()

    @simplified
    def partial_eval(self):
        term = self.term.partial_eval()
        if isinstance(term, Literal):
            if isinstance(term.value, text_type):
                return Literal(len(term.value))
            else:
                return NULL
        else:
            return LengthOp(term)


class FirstOp(Expression):
    def __init__(self, term):
        Expression.__init__(self, [term])
        self.term = term
        self.data_type = self.term.type

    def __data__(self):
        return {"first": self.term.__data__()}

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return LastOp(self.term.map(map_))

    def missing(self):
        return self.term.missing()

    @simplified
    def partial_eval(self):
        term = self.term.partial_eval()
        if isinstance(self.term, FirstOp):
            return term
        elif term.type != OBJECT and not term.many:
            return term
        elif term is NULL:
            return term
        elif isinstance(term, Literal):
            Log.error("not handled yet")
        else:
            return FirstOp(term)


class LastOp(Expression):
    def __init__(self, term):
        Expression.__init__(self, [term])
        self.term = term
        self.data_type = self.term.type

    def __data__(self):
        return {"last": self.term.__data__()}

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return LastOp(self.term.map(map_))

    def missing(self):
        return self.term.missing()

    @simplified
    def partial_eval(self):
        term = self.term.partial_eval()
        if isinstance(self.term, LastOp):
            return term
        elif term.type != OBJECT and not term.many:
            return term
        elif term is NULL:
            return term
        elif isinstance(term, Literal):
            if isinstance(term, list):
                if len(term) > 0:
                    return term[-1]
                return NULL
            return term
        else:
            return LastOp(term)


class BooleanOp(Expression):
    data_type = BOOLEAN

    def __init__(self, term):
        Expression.__init__(self, [term])
        self.term = term

    def __data__(self):
        return {"boolean": self.term.__data__()}

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return BooleanOp(self.term.map(map_))

    def missing(self):
        return self.term.missing()

    @simplified
    def partial_eval(self):
        term = self.term.partial_eval()
        if term is TRUE:
            return TRUE
        elif term in (FALSE, NULL):
            return FALSE
        elif term.type == BOOLEAN:
            return term

        is_missing = NotOp(term.missing()).partial_eval()
        return is_missing


class IsBooleanOp(Expression):
    data_type = BOOLEAN

    def __init__(self, term):
        Expression.__init__(self, [term])
        self.term = term

    def __data__(self):
        return {"is_boolean": self.term.__data__()}

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return IsBooleanOp(self.term.map(map_))

    def missing(self):
        return FALSE


class IntegerOp(Expression):
    data_type = INTEGER

    def __init__(self, term):
        Expression.__init__(self, [term])
        self.term = term

    def __data__(self):
        return {"integer": self.term.__data__()}

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return IntegerOp(self.term.map(map_))

    def missing(self):
        return self.term.missing()

    @simplified
    def partial_eval(self):
        term = FirstOp(self.term).partial_eval()
        if isinstance(term, CoalesceOp):
            return CoalesceOp([IntegerOp(t) for t in term.terms])
        if term.type == INTEGER:
            return term
        return IntegerOp(term)


class IsIntegerOp(Expression):
    data_type = BOOLEAN

    def __init__(self, term):
        Expression.__init__(self, [term])
        self.term = term

    def __data__(self):
        return {"is_integer": self.term.__data__()}

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return IsIntegerOp(self.term.map(map_))

    def missing(self):
        return FALSE


class NumberOp(Expression):
    data_type = NUMBER

    def __init__(self, term):
        Expression.__init__(self, [term])
        self.term = term

    def __data__(self):
        return {"number": self.term.__data__()}

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return NumberOp(self.term.map(map_))

    def missing(self):
        return self.term.missing()

    @simplified
    def partial_eval(self):
        term = FirstOp(self.term).partial_eval()
        if isinstance(term, CoalesceOp):
            return CoalesceOp([NumberOp(t) for t in term.terms])
        return self


class IsNumberOp(Expression):
    data_type = BOOLEAN

    def __init__(self, term):
        Expression.__init__(self, [term])
        self.term = term

    def __data__(self):
        return {"is_number": self.term.__data__()}

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return IsNumberOp(self.term.map(map_))

    def missing(self):
        return FALSE

    @simplified
    def partial_eval(self):
        term = self.term.partial_eval()

        if isinstance(term, NullOp):
            return FALSE
        elif term.type in (INTEGER, NUMBER):
            return TRUE
        elif term.type == OBJECT:
            return self
        else:
            return FALSE


class StringOp(Expression):
    data_type = STRING

    def __init__(self, term):
        Expression.__init__(self, [term])
        self.term = term

    def __data__(self):
        return {"string": self.term.__data__()}

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return StringOp(self.term.map(map_))

    def missing(self):
        return self.term.missing()

    @simplified
    def partial_eval(self):
        term = FirstOp(self.term).partial_eval()
        if isinstance(term, CoalesceOp):
            return CoalesceOp([StringOp(t).partial_eval() for t in term.terms])
        elif isinstance(term, Literal):
            if term.type == STRING:
                return term
            else:
                return Literal(mo_json.value2json(term.value))
        return self


class IsStringOp(Expression):
    data_type = BOOLEAN

    def __init__(self, term):
        Expression.__init__(self, [term])
        self.term = term

    def __data__(self):
        return {"is_string": self.term.__data__()}

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return IsStringOp(self.term.map(map_))

    def missing(self):
        return FALSE


class CountOp(Expression):
    has_simple_form = False
    data_type = INTEGER

    def __init__(self, terms, **clauses):
        Expression.__init__(self, terms)
        if isinstance(terms, list):
            # SHORTCUT: ASSUME AN ARRAY OF IS A TUPLE
            self.terms = TupleOp(terms)
        else:
            self.terms = terms

    def __data__(self):
        return {"count": self.terms.__data__()}

    def vars(self):
        return self.terms.vars()

    def map(self, map_):
        return CountOp(self.terms.map(map_))

    def missing(self):
        return FALSE

    def exists(self):
        return TrueOp


class MaxOp(Expression):
    data_type = NUMBER

    def __init__(self, terms):
        Expression.__init__(self, terms)
        if terms == None:
            self.terms = []
        elif isinstance(terms, list):
            self.terms = terms
        else:
            self.terms = [terms]

    def __data__(self):
        return {"max": [t.__data__() for t in self.terms]}

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output

    def map(self, map_):
        return MaxOp([t.map(map_) for t in self.terms])

    def missing(self):
        return FALSE

    @simplified
    def partial_eval(self):
        maximum = None
        terms = []
        for t in self.terms:
            simple = t.partial_eval()
            if isinstance(simple, NullOp):
                pass
            elif isinstance(simple, Literal):
                maximum = MAX([maximum, simple.value])
            else:
                terms.append(simple)
        if len(terms) == 0:
            if maximum == None:
                return NULL
            else:
                return Literal(maximum)
        else:
            if maximum == None:
                output = MaxOp(terms)
            else:
                output = MaxOp([Literal(maximum)] + terms)

        return output


class MinOp(Expression):
    data_type = NUMBER

    def __init__(self, terms):
        Expression.__init__(self, terms)
        if terms == None:
            self.terms = []
        elif isinstance(terms, list):
            self.terms = terms
        else:
            self.terms = [terms]

    def __data__(self):
        return {"min": [t.__data__() for t in self.terms]}

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output

    def map(self, map_):
        return MinOp([t.map(map_) for t in self.terms])

    def missing(self):
        return FALSE

    @simplified
    def partial_eval(self):
        minimum = None
        terms = []
        for t in self.terms:
            simple = t.partial_eval()
            if isinstance(simple, NullOp):
                pass
            elif isinstance(simple, Literal):
                minimum = MIN([minimum, simple.value])
            else:
                terms.append(simple)
        if len(terms) == 0:
            if minimum == None:
                return NULL
            else:
                return Literal(minimum)
        else:
            if minimum == None:
                output = MinOp(terms)
            else:
                output = MinOp([Literal(minimum)] + terms)

        return output


_jx_identity = {
    "add": ZERO,
    "mul": ONE
}


class BaseMultiOp(Expression):
    has_simple_form = True
    data_type = NUMBER
    op = None

    def __init__(self, terms, **clauses):
        Expression.__init__(self, terms)
        self.terms = terms
        self.default = coalesce(clauses.get("default"), NULL)
        self.nulls = coalesce(clauses.get("nulls"), FALSE)  # nulls==True WILL HAVE OP RETURN null ONLY IF ALL OPERANDS ARE null

    def __data__(self):
        return {self.op: [t.__data__() for t in self.terms], "default": self.default, "nulls": self.nulls}

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output

    def map(self, map_):
        return self.__class__([t.map(map_) for t in self.terms], **{"default": self.default, "nulls": self.nulls})

    def missing(self):
        if self.nulls:
            if isinstance(self.default, NullOp):
                return AndOp([t.missing() for t in self.terms])
            else:
                return TRUE
        else:
            if isinstance(self.default, NullOp):
                return OrOp([t.missing() for t in self.terms])
            else:
                return FALSE

    def exists(self):
        if self.nulls:
            return OrOp([t.exists() for t in self.terms])
        else:
            return AndOp([t.exists() for t in self.terms])

    @simplified
    def partial_eval(self):
        acc = None
        terms = []
        for t in self.terms:
            simple = t.partial_eval()
            if isinstance(simple, NullOp):
                pass
            elif isinstance(simple, Literal):
                if acc is None:
                    acc = simple.value
                else:
                    acc = builtin_ops[self.op](acc, simple.value)
            else:
                terms.append(simple)
        if len(terms) == 0:
            if acc == None:
                return self.default.partial_eval()
            else:
                return Literal(acc)
        elif self.nulls:
            # DECISIVE
            if acc is not None:
                terms.append(Literal(acc))

            output = WhenOp(
                AndOp([t.missing() for t in terms]),
                **{
                    "then": self.default,
                    "else": operators["basic." + self.op]([
                        CoalesceOp([t, _jx_identity[self.op]])
                        for t in terms
                    ])
                }
            ).partial_eval()
        else:
            # CONSERVATIVE
            if acc is not None:
                terms.append(Literal(acc))

            output = WhenOp(
                OrOp([t.missing() for t in terms]),
                **{
                    "then": self.default,
                    "else": operators["basic." + self.op](terms)
                }
            ).partial_eval()

        return output


class AddOp(BaseMultiOp):
    op = "add"


class MulOp(BaseMultiOp):
    op = "mul"


class RegExpOp(Expression):
    has_simple_form = True
    data_type = BOOLEAN

    def __init__(self, terms):
        Expression.__init__(self, terms)
        self.var, self.pattern = terms

    def __data__(self):
        return {"regexp": {self.var.var: self.pattern}}

    def vars(self):
        return {self.var}

    def map(self, map_):
        return RegExpOp([self.var.map(map_), self.pattern])

    def missing(self):
        return FALSE

    def exists(self):
        return TRUE


class CoalesceOp(Expression):
    has_simple_form = True

    def __init__(self, terms):
        Expression.__init__(self, terms)
        self.terms = terms

    def __data__(self):
        return {"coalesce": [t.__data__() for t in self.terms]}

    def __eq__(self, other):
        if isinstance(other, CoalesceOp):
            if len(self.terms) == len(other.terms):
                return all(s == o for s, o in zip(self.terms, other.terms))
        return False

    def missing(self):
        # RETURN true FOR RECORDS THE WOULD RETURN NULL
        return AndOp([v.missing() for v in self.terms])

    def vars(self):
        output = set()
        for v in self.terms:
            output |= v.vars()
        return output

    def map(self, map_):
        return CoalesceOp([v.map(map_) for v in self.terms])

    @simplified
    def partial_eval(self):
        terms = []
        for t in self.terms:
            simple = FirstOp(t).partial_eval()
            if simple is NULL:
                pass
            elif isinstance(simple, Literal):
                terms.append(simple)
                break
            else:
                terms.append(simple)

        if len(terms) == 0:
            return NULL
        elif len(terms) == 1:
            return terms[0]
        else:
            return CoalesceOp(terms)


class MissingOp(Expression):
    data_type = BOOLEAN

    def __init__(self, term):
        Expression.__init__(self, term)
        self.expr = term

    def __data__(self):
        return {"missing": self.expr.__data__()}

    def __eq__(self, other):
        if not isinstance(other, MissingOp):
            return False
        else:
            return self.expr == other.expr

    def vars(self):
        return self.expr.vars()

    def map(self, map_):
        return MissingOp(self.expr.map(map_))

    def missing(self):
        return FALSE

    def exists(self):
        return TRUE

    @simplified
    def partial_eval(self):
        output = self.expr.partial_eval().missing()
        if isinstance(output, MissingOp):
            return output
        else:
            return output.partial_eval()


class ExistsOp(Expression):
    data_type = BOOLEAN

    def __init__(self, term):
        Expression.__init__(self, [term])
        self.field = term

    def __data__(self):
        return {"exists": self.field.__data__()}

    def vars(self):
        return self.field.vars()

    def map(self, map_):
        return ExistsOp(self.field.map(map_))

    def missing(self):
        return FALSE

    def exists(self):
        return TRUE

    @simplified
    def partial_eval(self):
        return NotOp(self.field.missing()).partial_eval()


class PrefixOp(Expression):
    has_simple_form = True
    data_type = BOOLEAN

    def __init__(self, term):
        Expression.__init__(self, term)
        if not term:
            self.expr = None
            self.prefix = None
        elif isinstance(term, Mapping):
            self.expr, self.prefix = term.items()[0]
        else:
            self.expr, self.prefix = term

    def __data__(self):
        if not self.expr:
            return {"prefix": {}}
        elif isinstance(self.expr, Variable) and isinstance(self.prefix, Literal):
            return {"prefix": {self.expr.var: self.prefix.value}}
        else:
            return {"prefix": [self.expr.__data__(), self.prefix.__data__()]}

    def vars(self):
        if not self.expr:
            return set()
        return self.expr.vars() | self.prefix.vars()

    def map(self, map_):
        if not self.expr:
            return self
        else:
            return PrefixOp([self.expr.map(map_), self.prefix.map(map_)])

    def missing(self):
        return FALSE

    def partial_eval(self):
        if not self.expr:
            return TRUE

        return WhenOp(
            AndOp([self.expr.exists(), self.prefix.exists()]),
            **{"then": BasicStartsWithOp([self.expr, self.prefix]), "else": FALSE}
        ).partial_eval()


class SuffixOp(Expression):
    has_simple_form = True
    data_type = BOOLEAN

    def __init__(self, term):
        Expression.__init__(self, term)
        if not term:
            self.expr = self.suffix = None
        elif isinstance(term, Mapping):
            self.expr, self.suffix = term.items()[0]
        else:
            self.expr, self.suffix = term

    def __data__(self):
        if self.expr is None:
            return {"suffix": {}}
        elif isinstance(self.expr, Variable) and isinstance(self.suffix, Literal):
            return {"suffix": {self.expr.var: self.suffix.value}}
        else:
            return {"suffix": [self.expr.__data__(), self.suffix.__data__()]}

    def missing(self):
        """
        THERE IS PLENTY OF OPPORTUNITY TO SIMPLIFY missing EXPRESSIONS
        OVERRIDE THIS METHOD TO SIMPLIFY
        :return:
        """
        return FALSE

    def vars(self):
        if self.expr is None:
            return set()
        return self.expr.vars() | self.suffix.vars()

    def map(self, map_):
        if self.expr is None:
            return TRUE
        else:
            return SuffixOp([self.expr.map(map_), self.suffix.map(map_)])

    def partial_eval(self):
        if self.expr is None:
            return TRUE
        if not isinstance(self.suffix, Literal) and self.suffix.type == STRING:
            Log.error("can only hanlde literal suffix ")

        return WhenOp(
            AndOp([self.expr.exists(), self.suffix.exists()]),
            **{"then": RegExpOp([self.expr, Literal(".*" + re.escape(self.suffix.value))]), "else": FALSE}
        ).partial_eval()


class ConcatOp(Expression):
    has_simple_form = True
    data_type = STRING

    def __init__(self, term, **clauses):
        Expression.__init__(self, term)
        if isinstance(term, Mapping):
            self.terms = term.items()[0]
        else:
            self.terms = term
        self.separator = clauses.get("separator", Literal(""))
        self.default = clauses.get("default", NULL)
        if not isinstance(self.separator, Literal):
            Log.error("Expecting a literal separator")

    @classmethod
    def define(cls, expr):
        term = expr.concat
        if isinstance(term, Mapping):
            k, v = term.items()[0]
            terms = [Variable(k), Literal(v)]
        else:
            terms = map(jx_expression, term)

        return ConcatOp(
            terms,
            **{k: Literal(v) for k, v in expr.items() if k in ["default", "separator"]}
        )

    def __data__(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            output = {"concat": {self.terms[0].var: self.terms[2].value}}
        else:
            output = {"concat": [t.__data__() for t in self.terms]}
        if self.separator.json != '""':
            output["separator"] = self.terms[2].value
        return output

    def vars(self):
        if not self.terms:
            return set()
        return set.union(*(t.vars() for t in self.terms))

    def map(self, map_):
        return ConcatOp([t.map(map_) for t in self.terms], separator=self.separator)

    def missing(self):
        return AndOp([t.missing() for t in self.terms] + [self.default.missing()]).partial_eval()


class UnixOp(Expression):
    """
    FOR USING ON DATABASES WHICH HAVE A DATE COLUMNS: CONVERT TO UNIX
    """
    has_simple_form = True
    data_type = NUMBER

    def __init__(self, term):
        Expression.__init__(self, term)
        self.value = term

    def vars(self):
        return self.value.vars()

    def map(self, map_):
        return UnixOp(self.value.map(map_))

    def missing(self):
        return self.value.missing()


class FromUnixOp(Expression):
    """
    FOR USING ON DATABASES WHICH HAVE A DATE COLUMNS: CONVERT TO UNIX
    """
    data_type = NUMBER

    def __init__(self, term):
        Expression.__init__(self, term)
        self.value = term

    def vars(self):
        return self.value.vars()

    def map(self, map_):
        return FromUnixOp(self.value.map(map_))

    def missing(self):
        return self.value.missing()


class LeftOp(Expression):
    has_simple_form = True
    data_type = STRING

    def __init__(self, term):
        Expression.__init__(self, term)
        if isinstance(term, Mapping):
            self.value, self.length = term.items()[0]
        else:
            self.value, self.length = term

    def __data__(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            return {"left": {self.value.var: self.length.value}}
        else:
            return {"left": [self.value.__data__(), self.length.__data__()]}

    def vars(self):
        return self.value.vars() | self.length.vars()

    def map(self, map_):
        return LeftOp([self.value.map(map_), self.length.map(map_)])

    def missing(self):
        return OrOp([self.value.missing(), self.length.missing()]).partial_eval()

    @simplified
    def partial_eval(self):
        value = self.value.partial_eval()
        length = self.length.partial_eval()
        max_length = LengthOp(value)

        return WhenOp(
            self.missing(),
            **{
                "else": BasicSubstringOp([
                    value,
                    ZERO,
                    MaxOp([ZERO, MinOp([length, max_length])])
                ])
            }
        ).partial_eval()


class NotLeftOp(Expression):
    has_simple_form = True
    data_type = STRING

    def __init__(self, term):
        Expression.__init__(self, term)
        if isinstance(term, Mapping):
            self.value, self.length = term.items()[0]
        else:
            self.value, self.length = term

    def __data__(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            return {"not_left": {self.value.var: self.length.value}}
        else:
            return {"not_left": [self.value.__data__(), self.length.__data__()]}

    def vars(self):
        return self.value.vars() | self.length.vars()

    def map(self, map_):
        return NotLeftOp([self.value.map(map_), self.length.map(map_)])

    def missing(self):
        return OrOp([self.value.missing(), self.length.missing()])

    @simplified
    def partial_eval(self):
        value = self.value.partial_eval()
        length = self.length.partial_eval()
        max_length = LengthOp(value)

        return WhenOp(
            self.missing(),
            **{
                "else": BasicSubstringOp([
                    value,
                    MaxOp([ZERO, MinOp([length, max_length])]),
                    max_length
                ])
            }
        ).partial_eval()


class RightOp(Expression):
    has_simple_form = True
    data_type = STRING

    def __init__(self, term):
        Expression.__init__(self, term)
        if isinstance(term, Mapping):
            self.value, self.length = term.items()[0]
        else:
            self.value, self.length = term

    def __data__(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            return {"right": {self.value.var: self.length.value}}
        else:
            return {"right": [self.value.__data__(), self.length.__data__()]}

    def vars(self):
        return self.value.vars() | self.length.vars()

    def map(self, map_):
        return RightOp([self.value.map(map_), self.length.map(map_)])

    def missing(self):
        return OrOp([self.value.missing(), self.length.missing()])

    @simplified
    def partial_eval(self):
        value = self.value.partial_eval()
        length = self.length.partial_eval()
        max_length = LengthOp(value)

        return WhenOp(
            self.missing(),
            **{
                "else": BasicSubstringOp([
                    value,
                    MaxOp([ZERO, MinOp([max_length, SubOp([max_length, length])])]),
                    max_length
                ])
            }
        ).partial_eval()


class NotRightOp(Expression):
    has_simple_form = True
    data_type = STRING

    def __init__(self, term):
        Expression.__init__(self, term)
        if isinstance(term, Mapping):
            self.value, self.length = term.items()[0]
        else:
            self.value, self.length = term

    def __data__(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            return {"not_right": {self.value.var: self.length.value}}
        else:
            return {"not_right": [self.value.__data__(), self.length.__data__()]}

    def vars(self):
        return self.value.vars() | self.length.vars()

    def map(self, map_):
        return NotRightOp([self.value.map(map_), self.length.map(map_)])

    def missing(self):
        return OrOp([self.value.missing(), self.length.missing()])

    @simplified
    def partial_eval(self):
        value = self.value.partial_eval()
        length = self.length.partial_eval()
        max_length = LengthOp(value)

        return WhenOp(
            self.missing(),
            **{
                "else": BasicSubstringOp([
                    value,
                    ZERO,
                    MaxOp([ZERO, MinOp([max_length, SubOp([max_length, length])])])
                ])
            }
        ).partial_eval()


class FindOp(Expression):
    """
    RETURN INDEX OF find IN value, ELSE RETURN null
    """
    has_simple_form = True
    data_type = INTEGER

    def __init__(self, term, **kwargs):
        Expression.__init__(self, term)
        self.value, self.find = term
        self.default = kwargs.get("default", NULL)
        self.start = kwargs.get("start", ZERO).partial_eval()
        if self.start is NULL:
            self.start = ZERO

    def __data__(self):
        if isinstance(self.value, Variable) and isinstance(self.find, Literal):
            output = {
                "find": {self.value.var, self.find.value},
                "start": self.start.__data__()
            }
        else:
            output = {
                "find": [self.value.__data__(), self.find.__data__()],
                "start": self.start.__data__()
            }
        if self.default:
            output["default"] = self.default.__data__()
        return output

    def vars(self):
        return self.value.vars() | self.find.vars() | self.default.vars() | self.start.vars()

    def map(self, map_):
        return FindOp(
            [self.value.map(map_), self.find.map(map_)],
            start=self.start.map(map_),
            default=self.default.map(map_)
        )

    def missing(self):
        output = AndOp([
            self.default.missing(),
            OrOp([
                self.value.missing(),
                self.find.missing(),
                EqOp([BasicIndexOfOp([
                    self.value,
                    self.find,
                    self.start
                ]), Literal(-1)])
            ])
        ]).partial_eval()
        return output

    def exists(self):
        return TRUE

    @simplified
    def partial_eval(self):
        index = BasicIndexOfOp([
            self.value,
            self.find,
            self.start
        ]).partial_eval()

        output = WhenOp(
            OrOp([
                self.value.missing(),
                self.find.missing(),
                BasicEqOp([index, Literal(-1)])
            ]),
            **{"then": self.default, "else": index}
        ).partial_eval()
        return output


class SplitOp(Expression):
    has_simple_form = True

    def __init__(self, term, **kwargs):
        Expression.__init__(self, term)
        self.value, self.find = term

    def __data__(self):
        if isinstance(self.value, Variable) and isinstance(self.find, Literal):
            return {"split": {self.value.var, self.find.value}}
        else:
            return {"split": [self.value.__data__(), self.find.__data__()]}

    def vars(self):
        return self.value.vars() | self.find.vars() | self.default.vars() | self.start.vars()

    def map(self, map_):
        return FindOp(
            [self.value.map(map_), self.find.map(map_)],
            start=self.start.map(map_),
            default=self.default.map(map_)
        )

    def missing(self):
        v = self.value.to_es_script(not_null=True)
        find = self.find.to_es_script(not_null=True)
        index = v + ".indexOf(" + find + ", " + self.start.to_es_script() + ")"

        return AndOp([
            self.default.missing(),
            OrOp([
                self.value.missing(),
                self.find.missing(),
                EqOp([ScriptOp(index), Literal(-1)])
            ])
        ])

    def exists(self):
        return TRUE


class BetweenOp(Expression):
    data_type = STRING

    def __init__(self, value, prefix, suffix, default=NULL, start=NULL):
        Expression.__init__(self, [])
        self.value = value
        self.prefix = prefix
        self.suffix = suffix
        self.default = default
        self.start = start
        if isinstance(self.prefix, Literal) and isinstance(self.suffix, Literal):
            pass
        else:
            Log.error("Expecting literal prefix and suffix only")

    @classmethod
    def define(cls, expr):
        term = expr.between
        if isinstance(term, list):
            return BetweenOp(
                value=jx_expression(term[0]),
                prefix=jx_expression(term[1]),
                suffix=jx_expression(term[2]),
                default=jx_expression(expr.default),
                start=jx_expression(expr.start)
            )
        elif isinstance(term, Mapping):
            var, vals = term.items()[0]
            if isinstance(vals, list) and len(vals) == 2:
                return BetweenOp(
                    value=Variable(var),
                    prefix=Literal(vals[0]),
                    suffix=Literal(vals[1]),
                    default=jx_expression(expr.default),
                    start=jx_expression(expr.start)
                )
            else:
                Log.error("`between` parameters are expected to be in {var: [prefix, suffix]} form")
        else:
            Log.error("`between` parameters are expected to be in {var: [prefix, suffix]} form")

    def vars(self):
        return self.value.vars() | self.prefix.vars() | self.suffix.vars() | self.default.vars() | self.start.vars()

    def map(self, map_):
        return BetweenOp(
            self.value.map(map_),
            self.prefix.map(map_),
            self.suffix.map(map_),
            default=self.default.map(map_),
            start=self.start.map(map_)
        )

    def __data__(self):
        if isinstance(self.value, Variable) and isinstance(self.prefix, Literal) and isinstance(self.suffix, Literal):
            output = wrap({"between": {self.value.var: [self.prefix.value, self.suffix.value]}})
        else:
            output = wrap({"between": [self.value.__data__(), self.prefix.__data__(), self.suffix.__data__()]})
        if self.start:
            output.start = self.start.__data__()
        if self.default:
            output.default = self.default.__data__()
        return output

    @simplified
    def partial_eval(self):
        value = self.value.partial_eval()

        start_index = CaseOp([
                WhenOp(self.prefix.missing(), **{"then": ZERO}),
                WhenOp(IsNumberOp(self.prefix), **{"then": MaxOp([ZERO, self.prefix])}),
                FindOp([value, self.prefix], start=self.start)
            ]
        ).partial_eval()

        len_prefix = CaseOp([
                WhenOp(self.prefix.missing(), **{"then": ZERO}),
                WhenOp(IsNumberOp(self.prefix), **{"then": ZERO}),
                LengthOp(self.prefix)
            ]
        ).partial_eval()

        end_index = CaseOp(
            [
                WhenOp(start_index.missing(), **{"then": NULL}),
                WhenOp(self.suffix.missing(), **{"then": LengthOp(value)}),
                WhenOp(IsNumberOp(self.suffix), **{"then": MinOp([self.suffix, LengthOp(value)])}),
                FindOp([value, self.suffix], start=AddOp([start_index, len_prefix]))
            ]
        ).partial_eval()

        start_index = AddOp([start_index, len_prefix]).partial_eval()
        substring = BasicSubstringOp([value, start_index, end_index]).partial_eval()

        between = WhenOp(
            end_index.missing(),
            **{
                "then": self.default,
                "else": substring
            }
        ).partial_eval()

        return between


class InOp(Expression):
    has_simple_form = True
    data_type = BOOLEAN

    def __new__(cls, terms):
        if isinstance(terms[0], Variable) and isinstance(terms[1], Literal):
            name, value = terms
            if not isinstance(value.value, (list, tuple)):
                return EqOp([name, Literal([value.value])])
        return object.__new__(cls)

    def __init__(self, term):
        Expression.__init__(self, term)
        self.value, self.superset = term

    def __data__(self):
        if isinstance(self.value, Variable) and isinstance(self.superset, Literal):
            return {"in": {self.value.var: self.superset.value}}
        else:
            return {"in": [self.value.__data__(), self.superset.__data__()]}

    def __eq__(self, other):
        if isinstance(other, InOp):
            return self.value == other.value and self.superset == other.superset
        return False

    def vars(self):
        return self.value.vars()

    def map(self, map_):
        return InOp([self.value.map(map_), self.superset.map(map_)])

    @simplified
    def partial_eval(self):
        value = self.value.partial_eval()
        superset = self.superset.partial_eval()
        if superset is NULL:
            return FALSE
        elif isinstance(value, Literal) and isinstance(superset, Literal):
            return Literal(self())
        else:
            return self

    def __call__(self):
        return self.value() in self.superset()

    def missing(self):
        return FALSE


class RangeOp(Expression):
    has_simple_form = True
    data_type = BOOLEAN

    def __new__(cls, term, *args):
        Expression.__new__(cls, *args)
        field, comparisons = term  # comparisons IS A Literal()
        return AndOp([operators[op](op, [field, Literal(value)]) for op, value in comparisons.value.items()])

    def __init__(self, term):
        Log.error("Should never happen!")


class WhenOp(Expression):
    def __init__(self, term, **clauses):
        Expression.__init__(self, [term])

        self.when = term
        self.then = coalesce(clauses.get("then"), NULL)
        self.els_ = coalesce(clauses.get("else"), NULL)

        if self.then is NULL:
            self.data_type = self.els_.type
        elif self.els_ is NULL:
            self.data_type = self.then.type
        elif self.then.type == self.els_.type:
            self.data_type = self.then.type
        elif self.then.type in (INTEGER, NUMBER) and self.els_.type in (INTEGER, NUMBER):
            self.data_type = NUMBER
        else:
            self.data_type = OBJECT

    def __data__(self):
        return {"when": self.when.__data__(), "then": self.then.__data__() if self.then else None, "else": self.els_.__data__() if self.els_ else None}

    def vars(self):
        return self.when.vars() | self.then.vars() | self.els_.vars()

    def map(self, map_):
        return WhenOp(self.when.map(map_), **{"then": self.then.map(map_), "else": self.els_.map(map_)})

    def missing(self):
        return OrOp([
            AndOp([self.when, self.then.missing()]),
            AndOp([NotOp(self.when), self.els_.missing()])
        ]).partial_eval()

    @simplified
    def partial_eval(self):
        when = BooleanOp(self.when).partial_eval()

        if when is TRUE:
            return self.then.partial_eval()
        elif when in [FALSE, NULL]:
            return self.els_.partial_eval()
        elif isinstance(when, Literal):
            Log.error("Expecting `when` clause to return a Boolean, or `null`")

        then = self.then.partial_eval()
        els_ = self.els_.partial_eval()

        if then is TRUE:
            if els_ is FALSE:
                return when
            elif els_ is TRUE:
                return TRUE
        elif then is FALSE:
            if els_ is FALSE:
                return FALSE
            elif els_ is TRUE:
                return NotOp(when).partial_eval()

        return WhenOp(when, **{"then": then, "else": els_})


class CaseOp(Expression):
    def __init__(self, terms, **clauses):
        if not isinstance(terms, (list, tuple)):
            Log.error("case expression requires a list of `when` sub-clauses")
        Expression.__init__(self, terms)
        if len(terms) == 0:
            Log.error("Expecting at least one clause")

        for w in terms[:-1]:
            if not isinstance(w, WhenOp) or w.els_:
                Log.error("case expression does not allow `else` clause in `when` sub-clause")
        self.whens = terms

    def __data__(self):
        return {"case": [w.__data__() for w in self.whens]}

    def __eq__(self, other):
        if isinstance(other, CaseOp):
            return all(s == o for s, o in zip(self.whens, other.whens))

    def vars(self):
        output = set()
        for w in self.whens:
            output |= w.vars()
        return output

    def map(self, map_):
        return CaseOp([w.map(map_) for w in self.whens])

    def missing(self):
        m = self.whens[-1].missing()
        for w in reversed(self.whens[0:-1]):
            when = w.when.partial_eval()
            if when is FALSE:
                pass
            elif when is TRUE:
                m = w.then.partial_eval().missing()
            else:
                m = OrOp([AndOp([when, w.then.partial_eval().missing()]), m])
        return m.partial_eval()

    @simplified
    def partial_eval(self):
        whens = []
        for w in self.whens[:-1]:
            when = w.when.partial_eval()
            if when is TRUE:
                whens.append(w.then.partial_eval())
                break
            elif when is FALSE:
                pass
            else:
                whens.append(WhenOp(when, **{"then": w.then.partial_eval()}))
        else:
            whens.append(self.whens[-1].partial_eval())

        if len(whens) == 1:
            return whens[0]
        else:
            return CaseOp(whens)

    @property
    def type(self):
        types = set(w.then.type if isinstance(w, WhenOp) else w.type for w in self.whens)
        if len(types) > 1:
            return OBJECT
        else:
            return first(types)


class UnionOp(Expression):

    def __init__(self, terms):
        Expression.__init__(self, terms)
        if terms == None:
            self.terms = []
        elif isinstance(terms, list):
            self.terms = terms
        else:
            self.terms = [terms]

    def __data__(self):
        return {"union": [t.__data__() for t in self.terms]}

    @property
    def type(self):
        return merge_types(t.type for t in self.terms)

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output

    def map(self, map_):
        return UnionOp([t.map(map_) for t in self.terms])

    def missing(self):
        return FALSE

    @simplified
    def partial_eval(self):
        minimum = None
        terms = []
        for t in self.terms:
            simple = t.partial_eval()
            if isinstance(simple, NullOp):
                pass
            elif isinstance(simple, Literal):
                minimum = MIN([minimum, simple.value])
            else:
                terms.append(simple)
        if len(terms) == 0:
            if minimum == None:
                return NULL
            else:
                return Literal(minimum)
        else:
            if minimum == None:
                output = UnionOp(terms)
            else:
                output = UnionOp([Literal(minimum)] + terms)

        return output


class EsNestedOp(Expression):
    data_type = BOOLEAN
    has_simple_form = False

    def __init__(self, terms):
        Expression.__init__(self, terms)
        self.path, self.query = terms

    @simplified
    def partial_eval(self):
        if self.path.var == '.':
            return self.query.partial_eval()
        return EsNestedOp("es.nested", [self.path, self.query.partial_eval()])

    def __data__(self):
        return {"es.nested": {self.path.var: self.query.__data__()}}

    def __eq__(self, other):
        if isinstance(other, EsNestedOp):
            return self.path.var == other.path.var and self.query == other.query
        return False


class BasicStartsWithOp(Expression):
    """
    PLACEHOLDER FOR BASIC value.startsWith(find, start) (CAN NOT DEAL WITH NULLS)
    """
    data_type = BOOLEAN

    def __init__(self, params):
        Expression.__init__(self, params)
        self.value, self.prefix = params

    def __data__(self):
        return {"basic.startsWith": [self.value.__data__(), self.prefix.__data__()]}

    def __eq__(self, other):
        if isinstance(other, BasicStartsWithOp):
            return self.value == other.value and self.prefix == other.prefix

    def vars(self):
        return self.value.vars() | self.prefix.vars()

    def missing(self):
        return FALSE

    @simplified
    def partial_eval(self):
        return BasicStartsWithOp([
            StringOp(self.value).partial_eval(),
            StringOp(self.prefix).partial_eval(),
        ])


class BasicIndexOfOp(Expression):
    """
    PLACEHOLDER FOR BASIC value.indexOf(find, start) (CAN NOT DEAL WITH NULLS)
    """
    data_type = INTEGER

    def __init__(self, params):
        Expression.__init__(self, params)
        self.value, self.find, self.start = params

    def __data__(self):
        return {"basic.indexOf": [self.value.__data__(), self.find.__data__(), self.start.__data__()]}

    def vars(self):
        return self.value.vars() | self.find.vars() | self.start.vars()

    def missing(self):
        return FALSE

    @simplified
    def partial_eval(self):
        start = IntegerOp(MaxOp([ZERO, self.start])).partial_eval()
        return BasicIndexOfOp([
            StringOp(self.value).partial_eval(),
            StringOp(self.find).partial_eval(),
            start
        ])


class SqlEqOp(Expression):
    """
    PLACEHOLDER FOR BASIC `==` OPERATOR (CAN NOT DEAL WITH NULLS)
    """
    data_type = BOOLEAN

    def __init__(self, terms):
        self.lhs, self.rhs = terms

    def __data__(self):
        return {"sql.eq": [self.lhs.__data__(), self.rhs.__data__()]}

    def missing(self):
        return FALSE

    def __eq__(self, other):
        if not isinstance(other, EqOp):
            return False
        return self.lhs == other.lhs and self.rhs == other.rhs


class SqlInstrOp(Expression):
    data_type = INTEGER

    def __init__(self, params):
        Expression.__init__(self, params)
        self.value, self.find = params

    def __data__(self):
        return {"sql.instr": [self.value.__data__(), self.find.__data__()]}

    def vars(self):
        return self.value.vars() | self.find.vars()

    def missing(self):
        return FALSE


class SqlSubstrOp(Expression):
    data_type = INTEGER

    def __init__(self, params):
        Expression.__init__(self, params)
        self.value, self.start, self.length = params

    def __data__(self):
        return {"sql.substr": [self.value.__data__(), self.start.__data__(), self.length.__data__()]}

    def vars(self):
        return self.value.vars() | self.start.vars() | self.length.vars()

    def missing(self):
        return FALSE


class BasicEqOp(Expression):
    """
    PLACEHOLDER FOR BASIC `==` OPERATOR (CAN NOT DEAL WITH NULLS)
    """
    data_type = BOOLEAN

    def __init__(self, terms):
        self.lhs, self.rhs = terms

    def __data__(self):
        return {"basic.eq": [self.lhs.__data__(), self.rhs.__data__()]}

    def missing(self):
        return FALSE

    def __eq__(self, other):
        if not isinstance(other, EqOp):
            return False
        return self.lhs == other.lhs and self.rhs == other.rhs


class BasicMultiOp(Expression):
    """
    PLACEHOLDER FOR BASIC OPERATOR (CAN NOT DEAL WITH NULLS)
    """
    data_type = NUMBER
    op = None

    def __init__(self, terms):
        Expression.__init__(self, terms)
        self.terms = terms

    def vars(self):
        output = set()
        for t in self.terms:
            output.update(t.vars())
        return output

    def map(self, map):
        return self.__class__([t.map(map) for t in self.terms])

    def __data__(self):
        return {self.op: [t.__data__() for t in self.terms]}

    def missing(self):
        return FALSE

    @simplified
    def partial_eval(self):
        acc = None
        terms = []
        for t in self.terms:
            simple = t.partial_eval()
            if isinstance(simple, NullOp):
                pass
            elif isinstance(simple, Literal):
                if acc is None:
                    acc = simple.value
                else:
                    acc = builtin_ops[self.op](acc, simple.value)
            else:
                terms.append(simple)
        if len(terms) == 0:
            if acc == None:
                return self.default.partial_eval()
            else:
                return Literal(acc)
        else:
            if acc is not None:
                terms.append(Literal(acc))

            return self.__class__(terms)


class BasicAddOp(BasicMultiOp):
    op = "basic.add"


class BasicMulOp(BasicMultiOp):
    op = "basic.mul"


class BasicSubstringOp(Expression):
    """
    PLACEHOLDER FOR BASIC value.substring(start, end) (CAN NOT DEAL WITH NULLS)
    """
    data_type = STRING

    def __init__(self, terms):
        self.value, self.start, self.end = terms

    def __data__(self):
        return {"basic.substring": [self.value.__data__(), self.start.__data__(), self.end.__data__()]}

    def missing(self):
        return FALSE


def merge_types(jx_types):
    """
    :param jx_types: ITERABLE OF jx TYPES
    :return: ONE TYPE TO RULE THEM ALL
    """
    return _merge_types[max(_merge_score[t] for t in jx_types)]


_merge_score = {
    IS_NULL: 0,
    BOOLEAN: 1,
    INTEGER: 2,
    NUMBER: 3,
    STRING: 4,
    OBJECT: 5
}
_merge_types = {v: k for k, v in _merge_score.items()}

operators = {
    "add": AddOp,
    "and": AndOp,
    "basic.add": BasicAddOp,
    "basic.mul": BasicMulOp,
    "between": BetweenOp,
    "case": CaseOp,
    "coalesce": CoalesceOp,
    "concat": ConcatOp,
    "count": CountOp,
    "date": DateOp,
    "div": DivOp,
    "divide": DivOp,
    "eq": EqOp,
    "exists": ExistsOp,
    "exp": ExpOp,
    "find": FindOp,
    "first": FirstOp,
    "floor": FloorOp,
    "from_unix": FromUnixOp,
    "get": GetOp,
    "gt": GtOp,
    "gte": GteOp,
    "in": InOp,
    "instr": FindOp,
    "is_number": IsNumberOp,
    "is_string": IsStringOp,
    "last": LastOp,
    "left": LeftOp,
    "length": LengthOp,
    "literal": Literal,
    "lt": LtOp,
    "lte": LteOp,
    "match_all": TrueOp,
    "max": MaxOp,
    "minus": SubOp,
    "missing": MissingOp,
    "mod": ModOp,
    "mul": MulOp,
    "mult": MulOp,
    "multiply": MulOp,
    "ne": NeOp,
    "neq": NeOp,
    "not": NotOp,
    "not_left": NotLeftOp,
    "not_right": NotRightOp,
    "null": NullOp,
    "number": NumberOp,
    "offset": OffsetOp,
    "or": OrOp,
    "postfix": SuffixOp,
    "prefix": PrefixOp,
    "range": RangeOp,
    "regex": RegExpOp,
    "regexp": RegExpOp,
    "right": RightOp,
    "rows": RowsOp,
    "script": ScriptOp,
    "select": SelectOp,
    "split": SplitOp,
    "string": StringOp,
    "suffix": SuffixOp,
    "sub": SubOp,
    "subtract": SubOp,
    "sum": AddOp,
    "term": EqOp,
    "terms": InOp,
    "tuple": TupleOp,
    "union": UnionOp,
    "unix": UnixOp,
    "when": WhenOp,
}

builtin_ops = {
    "ne": operator.ne,
    "eq": operator.eq,
    "gte": operator.ge,
    "gt": operator.gt,
    "lte": operator.le,
    "lt": operator.lt,
    "add": operator.add,
    "sub": operator.sub,
    "mul": operator.mul,
    "max": lambda *v: max(v),
    "min": lambda *v: min(v)
}
