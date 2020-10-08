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
from jx_base.expressions.not_op import NotOp
from jx_base.expressions.true_op import TRUE
from jx_base.language import is_op
from mo_imports import export
from mo_json import BOOLEAN


class MissingOp(Expression):
    data_type = BOOLEAN

    def __init__(self, term):
        Expression.__init__(self, term)
        self.expr = term

    def __data__(self):
        return {"missing": self.expr.__data__()}

    def __eq__(self, other):
        if not is_op(other, MissingOp):
            return False
        else:
            return self.expr == other.expr

    def vars(self):
        return self.expr.vars()

    def map(self, map_):
        return self.lang[MissingOp(self.expr.map(map_))]

    def missing(self, lang):
        return FALSE

    def invert(self, lang):
        output = self.expr.missing(lang)
        if is_op(output, MissingOp):
            # break call cycle
            return self.lang[NotOp(output)]
        else:
            return self.lang[output.invert(lang)]

    def exists(self):
        return TRUE

    def partial_eval(self, lang):
        output = self.expr.partial_eval(lang).missing(lang)
        if is_op(output, MissingOp):
            return output
        else:
            return output.partial_eval(lang)


export("jx_base.expressions.expression", MissingOp)
