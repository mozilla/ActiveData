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
from jx_base.expressions.string_op import StringOp
from jx_base.language import is_op
from mo_json import BOOLEAN


class BasicStartsWithOp(Expression):
    """
    PLACEHOLDER FOR BASIC value.startsWith(find, start) (CAN NOT DEAL WITH NULLS)
    """

    data_type = BOOLEAN

    def __init__(self, params):
        Expression.__init__(self, params)
        self.value, self.prefix = params

    def __data__(self):
        return {"basic.startsWith": [self.value.__data__(), self.prefix.__data__()]}

    def __eq__(self, other):
        if is_op(other, BasicStartsWithOp):
            return self.value == other.value and self.prefix == other.prefix

    def vars(self):
        return self.value.vars() | self.prefix.vars()

    def map(self, map_):
        return self.lang.BasicStartsWithOp([
            self.value.map(map_),
            self.prefix.map(map_),
        ])

    def missing(self, lang):
        return FALSE

    def partial_eval(self, lang):
        return self.lang[BasicStartsWithOp([
            StringOp(self.value).partial_eval(lang),
            StringOp(self.prefix).partial_eval(lang),
        ])]
