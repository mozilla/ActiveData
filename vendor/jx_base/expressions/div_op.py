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

from jx_base.expressions._utils import builtin_ops
from jx_base.expressions.and_op import AndOp
from jx_base.expressions.base_binary_op import BaseBinaryOp
from jx_base.expressions.eq_op import EqOp
from jx_base.expressions.literal import Literal, ZERO, is_literal
from jx_base.expressions.or_op import OrOp


class DivOp(BaseBinaryOp):
    op = "div"

    def missing(self, lang):
        return self.lang[AndOp([
            self.default.missing(lang),
            OrOp([
                self.lhs.missing(lang),
                self.rhs.missing(lang),
                EqOp([self.rhs, ZERO]),
            ]),
        ])].partial_eval(lang)

    def partial_eval(self, lang):
        default = self.default.partial_eval(lang)
        rhs = self.rhs.partial_eval(lang)
        if rhs is ZERO:
            return default
        lhs = self.lhs.partial_eval(lang)
        if is_literal(lhs) and is_literal(rhs):
            return Literal(builtin_ops[self.op](lhs.value, rhs.value))
        return self.__class__([lhs, rhs], default=default)
