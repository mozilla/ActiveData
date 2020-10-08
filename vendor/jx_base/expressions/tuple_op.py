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

from jx_base.expressions.expression import Expression
from jx_base.expressions.false_op import FALSE
from jx_base.expressions.literal import Literal
from jx_base.expressions.literal import is_literal
from mo_dots import is_many
from mo_imports import export
from mo_json import OBJECT


class TupleOp(Expression):
    date_type = OBJECT

    def __init__(self, terms):
        Expression.__init__(self, terms)
        if terms == None:
            self.terms = []
        elif is_many(terms):
            self.terms = terms
        else:
            self.terms = [terms]

    def __iter__(self):
        return self.terms.__iter__()

    def __data__(self):
        return {"tuple": [t.__data__() for t in self.terms]}

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output

    def map(self, map_):
        return self.lang[TupleOp([t.map(map_) for t in self.terms])]

    def missing(self, lang):
        return FALSE

    def __call__(self):
        return tuple(t() for t in self.terms)

    def partial_eval(self, lang):
        if all(is_literal(t) for t in self.terms):
            return self.lang[Literal([t.value for t in self.terms])]

        return self


export("jx_base.expressions._utils", TupleOp)
