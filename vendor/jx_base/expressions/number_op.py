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

from jx_base.expressions.case_op import CaseOp
from jx_base.expressions.coalesce_op import CoalesceOp
from jx_base.expressions.expression import Expression
from jx_base.expressions.false_op import FALSE
from jx_base.expressions.first_op import FirstOp
from jx_base.expressions.literal import Literal, ZERO, ONE
from jx_base.expressions.literal import is_literal
from jx_base.expressions.null_op import NULL
from jx_base.expressions.true_op import TRUE
from jx_base.expressions.when_op import WhenOp
from jx_base.language import is_op
from mo_future import text
from mo_json import NUMBER
from mo_logs import Log
from mo_times import Date


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
        return self.lang[NumberOp(self.term.map(map_))]

    def missing(self, lang):
        return self.term.missing(lang)

    def partial_eval(self, lang):
        term = FirstOp(self.term).partial_eval(lang)

        if is_literal(term):
            if term is NULL:
                return NULL
            elif term is FALSE:
                return ZERO
            elif term is TRUE:
                return ONE

            v = term.value
            if isinstance(v, (text, Date)):
                return self.lang[Literal(float(v))]
            elif isinstance(v, (int, float)):
                return term
            else:
                Log.error("can not convert {{value|json}} to number", value=term.value)
        elif is_op(term, CaseOp):  # REWRITING
            return self.lang[CaseOp(
                [WhenOp(t.when, **{"then": NumberOp(t.then)}) for t in term.whens[:-1]]
                + [NumberOp(term.whens[-1])]
            )].partial_eval(lang)
        elif is_op(term, WhenOp):  # REWRITING
            return self.lang[WhenOp(
                term.when, **{"then": NumberOp(term.then), "else": NumberOp(term.els_)}
            )].partial_eval(lang)
        elif is_op(term, CoalesceOp):
            return self.lang[CoalesceOp([NumberOp(t) for t in term.terms])]
        return self.lang[NumberOp(term)]
