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
from jx_base.language import is_op
from mo_json import NUMBER


class AbsOp(Expression):
    data_type = NUMBER

    def __init__(self, term):
        Expression.__init__(self, term)
        self.term = term

    def __data__(self):
        return {"abs": self.term.__data__()}

    def __eq__(self, other):
        if not is_op(other, AbsOp):
            return False
        return self.term == other.term

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return self.lang[AbsOp(self.term.map(map_))]

    def missing(self, lang):
        return self.term.missing(lang)

    def partial_eval(self, lang):
        return AbsOp(self.term.partial_eval(lang))
