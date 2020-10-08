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

from jx_base.expressions._utils import builtin_ops
from jx_base.expressions.expression import Expression
from jx_base.expressions.false_op import FALSE
from jx_base.expressions.literal import Literal
from jx_base.expressions.literal import is_literal
from jx_base.expressions.null_op import NULL
from jx_base.expressions.or_op import OrOp
from jx_base.expressions.variable import Variable
from jx_base.language import is_op
from mo_json import NUMBER


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
        if is_op(self.lhs, Variable) and is_literal(self.rhs):
            return {self.op: {self.lhs.var, self.rhs.value}, "default": self.default}
        else:
            return {
                self.op: [self.lhs.__data__(), self.rhs.__data__()],
                "default": self.default,
            }

    def vars(self):
        return self.lhs.vars() | self.rhs.vars() | self.default.vars()

    def map(self, map_):
        return self.__class__(
            [self.lhs.map(map_), self.rhs.map(map_)], default=self.default.map(map_)
        )

    def missing(self, lang):
        if self.default.exists():
            return FALSE
        else:
            return self.lang[OrOp([self.lhs.missing(lang), self.rhs.missing(lang)])]

    def partial_eval(self, lang):
        lhs = self.lhs.partial_eval(lang)
        rhs = self.rhs.partial_eval(lang)
        default = self.default.partial_eval(lang)
        if is_literal(lhs) and is_literal(rhs):
            if lhs is NULL or rhs is NULL:
                return NULL
            return Literal(builtin_ops[self.op](lhs.value, rhs.value))
        return self.__class__([lhs, rhs], default=default)
