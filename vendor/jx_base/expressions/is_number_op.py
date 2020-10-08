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
from jx_base.expressions.null_op import NULL
from jx_base.expressions.true_op import TRUE
from mo_json import BOOLEAN, OBJECT, NUMBER_TYPES


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
        return self.lang[IsNumberOp(self.term.map(map_))]

    def missing(self, lang):
        return FALSE

    def partial_eval(self, lang):
        term = self.term.partial_eval(lang)

        if term is NULL:
            return FALSE
        elif term.type in NUMBER_TYPES:
            return TRUE
        elif term.type == OBJECT:
            return self
        else:
            return FALSE
