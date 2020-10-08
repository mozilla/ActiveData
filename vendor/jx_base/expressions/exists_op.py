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

from jx_base.expressions._utils import TRUE
from jx_base.expressions.expression import Expression
from jx_base.expressions.false_op import FALSE
from mo_imports import expect
from mo_json import BOOLEAN

NotOp = expect("NotOp")


class ExistsOp(Expression):
    data_type = BOOLEAN

    def __init__(self, term):
        Expression.__init__(self, [term])
        self.expr = term

    def __data__(self):
        return {"exists": self.expr.__data__()}

    def vars(self):
        return self.expr.vars()

    def map(self, map_):
        return self.lang[ExistsOp(self.expr.map(map_))]

    def missing(self, lang):
        return FALSE

    def invert(self, lang):
        return self.lang[self.expr].missing(lang)

    def exists(self):
        return TRUE

    def partial_eval(self, lang):
        return (NotOp(self.expr.missing(lang))).partial_eval(lang)
