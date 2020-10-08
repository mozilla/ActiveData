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

from jx_base.expressions._utils import merge_types
from jx_base.expressions.expression import Expression
from jx_base.expressions.false_op import FALSE
from jx_base.expressions.literal import Literal
from jx_base.expressions.null_op import NULL
from jx_base.language import is_op
from mo_dots import is_many
from mo_math import MIN


class UnionOp(Expression):
    def __init__(self, terms):
        Expression.__init__(self, terms)
        if terms == None:
            self.terms = []
        elif is_many(terms):
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
        return self.lang[UnionOp([t.map(map_) for t in self.terms])]

    def missing(self, lang):
        return FALSE

    def partial_eval(self, lang):
        minimum = None
        terms = []
        for t in self.terms:
            simple = t.partial_eval(lang)
            if simple is NULL:
                pass
            elif is_op(simple, Literal):
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
                output = self.lang[UnionOp(terms)]
            else:
                output = self.lang[UnionOp([Literal(minimum)] + terms)]

        return output
