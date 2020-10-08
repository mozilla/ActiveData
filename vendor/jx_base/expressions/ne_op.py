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

from jx_base.expressions.and_op import AndOp
from jx_base.expressions.basic_eq_op import BasicEqOp
from jx_base.expressions.expression import Expression
from jx_base.expressions.false_op import FALSE
from jx_base.expressions.literal import is_literal
from jx_base.expressions.nested_op import NestedOp
from jx_base.expressions.not_op import NotOp
from jx_base.expressions.or_op import OrOp
from jx_base.expressions.variable import Variable, IDENTITY
from jx_base.language import is_op
from mo_dots import is_data, is_sequence
from mo_json import BOOLEAN
from mo_logs import Log


class NeOp(Expression):
    has_simple_form = True
    data_type = BOOLEAN

    def __init__(self, terms):
        Expression.__init__(self, terms)
        if is_sequence(terms):
            self.lhs, self.rhs = terms
        elif is_data(terms):
            self.rhs, self.lhs = terms.items()[0]
        else:
            Log.error("logic error")

    def __data__(self):
        if is_op(self.lhs, Variable) and is_literal(self.rhs):
            return {"ne": {self.lhs.var, self.rhs.value}}
        else:
            return {"ne": [self.lhs.__data__(), self.rhs.__data__()]}

    def vars(self):
        return self.lhs.vars() | self.rhs.vars()

    def map(self, map_):
        return self.lang[NeOp([self.lhs.map(map_), self.rhs.map(map_)])]

    def missing(self, lang):
        return FALSE  # USING THE decisive EQUALITY https://github.com/mozilla/jx-sqlite/blob/master/docs/Logical%20Equality.md#definitions

    def invert(self, lang):
        return self.lang[OrOp([
            self.lhs.missing(lang),
            self.rhs.missing(lang),
            BasicEqOp([self.lhs, self.rhs]),
        ])].partial_eval(lang)

    def partial_eval(self, lang):
        lhs = (self.lhs).partial_eval(lang)
        rhs = (self.rhs).partial_eval(lang)

        if is_op(lhs, NestedOp):
            return self.lang[NestedOp(
                path=lhs.path.partial_eval(lang),
                select=IDENTITY,
                where=AndOp([lhs.where, NeOp([lhs.select, rhs])]).partial_eval(lang),
                sort=lhs.sort.partial_eval(lang),
                limit=lhs.limit.partial_eval(lang),
            )].partial_eval(lang)

        output = self.lang[AndOp([
            lhs.exists(),
            rhs.exists(),
            NotOp(BasicEqOp([lhs, rhs])),
        ])].partial_eval(lang)
        return output
