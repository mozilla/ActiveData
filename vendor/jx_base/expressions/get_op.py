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
from jx_base.expressions.literal import is_literal


class GetOp(Expression):
    has_simple_form = True

    def __init__(self, term):
        Expression.__init__(self, term)
        self.var = term[0]
        self.offsets = term[1:]

    def __data__(self):
        if is_literal(self.var) and len(self.offsets) == 1 and is_literal(self.offset):
            return {"get": {self.var.json, self.offsets[0].value}}
        else:
            return {"get": [self.var.__data__()] + [o.__data__() for o in self.offsets]}

    def vars(self):
        output = self.var.vars()
        for o in self.offsets:
            output |= o.vars()
        return output

    def map(self, map_):
        return self.lang[GetOp(
            [self.var.map(map_)] + [o.map(map_) for o in self.offsets]
        )]
