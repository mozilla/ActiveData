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
from jx_base.expressions.boolean_op import BooleanOp
from jx_base.expressions.expression import Expression
from jx_base.expressions.false_op import FALSE
from jx_base.expressions.literal import Literal
from jx_base.expressions.not_op import NotOp
from jx_base.expressions.null_op import NULL
from jx_base.expressions.or_op import OrOp
from jx_base.expressions.true_op import TRUE
from jx_base.language import is_op
from mo_dots import coalesce
from mo_imports import export
from mo_json import OBJECT, same_json_type, merge_json_type
from mo_logs import Log


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
        elif same_json_type(self.then.type, self.els_.type):
            self.data_type = merge_json_type(self.then.type, self.els_.type)
        else:
            self.data_type = OBJECT

    def __data__(self):
        return {
            "when": self.when.__data__(),
            "then": None if self.then is NULL else self.then.__data__(),
            "else": None if self.els_ is NULL else self.els_.__data__(),
        }

    def vars(self):
        return self.when.vars() | self.then.vars() | self.els_.vars()

    def map(self, map_):
        return self.lang[WhenOp(
            self.when.map(map_),
            **{"then": self.then.map(map_), "else": self.els_.map(map_)}
        )]

    def missing(self, lang):
        return self.lang[OrOp([
            AndOp([self.when, self.then.missing(lang)]),
            AndOp([NotOp(self.when), self.els_.missing(lang)]),
        ])].partial_eval(lang)

    def invert(self, lang):
        return self.lang[OrOp([
            AndOp([self.when, self.then.invert(lang)]),
            AndOp([NotOp(self.when), self.els_.invert(lang)]),
        ])].partial_eval(lang)

    def partial_eval(self, lang):
        when = (BooleanOp(self.when)).partial_eval(lang)

        if when is TRUE:
            return (self.then).partial_eval(lang)
        elif when in [FALSE, NULL]:
            return (self.els_).partial_eval(lang)
        elif is_op(when, Literal):
            Log.error("Expecting `when` clause to return a Boolean, or `null`")

        then = (self.then).partial_eval(lang)
        els_ = (self.els_).partial_eval(lang)

        if then is TRUE:
            if els_ is FALSE:
                return when
            elif els_ is TRUE:
                return TRUE
        elif then is FALSE:
            if els_ is FALSE:
                return FALSE
            elif els_ is TRUE:
                return (NotOp(when)).partial_eval(lang)

        return self.lang[WhenOp(when, **{"then": then, "else": els_})]


export("jx_base.expressions.first_op", WhenOp)
export("jx_base.expressions.eq_op", WhenOp)
