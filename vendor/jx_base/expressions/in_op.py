# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

from jx_base.expressions.eq_op import EqOp
from jx_base.expressions.expression import Expression
from jx_base.expressions.false_op import FALSE
from jx_base.expressions.literal import Literal
from jx_base.expressions.literal import is_literal
from jx_base.expressions.null_op import NULL
from jx_base.expressions.variable import Variable
from jx_base.language import is_op
from mo_dots import is_many
from mo_imports import export
from mo_json import BOOLEAN


class InOp(Expression):
    has_simple_form = True
    data_type = BOOLEAN

    def __new__(cls, terms):
        if is_op(terms[0], Variable) and is_op(terms[1], Literal):
            name, value = terms
            if not is_many(value.value):
                return cls.lang[EqOp([name, Literal([value.value])])]
        return object.__new__(cls)

    def __init__(self, term):
        Expression.__init__(self, term)
        self.value, self.superset = term

    def __data__(self):
        if is_op(self.value, Variable) and is_literal(self.superset):
            return {"in": {self.value.var: self.superset.value}}
        else:
            return {"in": [self.value.__data__(), self.superset.__data__()]}

    def __eq__(self, other):
        if is_op(other, InOp):
            return self.value == other.value and self.superset == other.superset
        return False

    def vars(self):
        return self.value.vars()

    def map(self, map_):
        return self.lang[InOp([self.value.map(map_), self.superset.map(map_)])]

    def partial_eval(self, lang):
        value = self.value.partial_eval(lang)
        superset = self.superset.partial_eval(lang)
        if superset is NULL:
            return FALSE
        elif value is NULL:
            return FALSE
        elif is_literal(value) and is_literal(superset):
            return self.lang[Literal(self())]
        else:
            return self.lang[InOp([value, superset])]

    def __call__(self):
        return self.value() in self.superset()

    def missing(self, lang):
        return FALSE


export("jx_base.expressions.eq_op", InOp)
