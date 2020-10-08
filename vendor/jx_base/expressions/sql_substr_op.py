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
from mo_json import INTEGER


class SqlSubstrOp(Expression):
    data_type = INTEGER

    def __init__(self, params):
        Expression.__init__(self, params)
        self.value, self.start, self.length = params

    def __data__(self):
        return {"sql.substr": [
            self.value.__data__(),
            self.start.__data__(),
            self.length.__data__(),
        ]}

    def vars(self):
        return self.value.vars() | self.start.vars() | self.length.vars()

    def missing(self, lang):
        return FALSE
