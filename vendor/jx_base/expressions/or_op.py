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
from jx_base.expressions.expression import Expression
from jx_base.expressions.false_op import FALSE
from jx_base.expressions.true_op import TRUE
from jx_base.language import is_op
from mo_imports import export
from mo_json import BOOLEAN


class OrOp(Expression):
    data_type = BOOLEAN
    zero = FALSE  # ADD THIS TO terms FOR NO EEFECT

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
        return self.lang[OrOp([t.map(map_) for t in self.terms])]

    def missing(self, lang):
        return FALSE

    def invert(self, lang):
        return AndOp([t.invert(lang) for t in self.terms]).partial_eval(lang)

    def __call__(self, row=None, rownum=None, rows=None):
        return any(t(row, rownum, rows) for t in self.terms)

    def __eq__(self, other):
        if not is_op(other, OrOp):
            return False
        if len(self.terms) != len(other.terms):
            return False
        return all(t == u for t, u in zip(self.terms, other.terms))

    def partial_eval(self, lang):
        terms = []
        ands = []
        for t in self.terms:
            simple = (t).partial_eval(lang)
            if simple.type != BOOLEAN:
                simple = simple.exists()

            if simple is TRUE:
                return TRUE
            elif simple is FALSE:
                continue
            elif is_op(simple, OrOp):
                terms.extend([tt for tt in simple.terms if tt not in terms])
            elif is_op(simple, AndOp):
                ands.append(simple)
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
        return self.lang[OrOp(terms)]


export("jx_base.expressions.and_op", OrOp)
