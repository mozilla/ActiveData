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

from jx_base.expressions import RegExpOp as RegExpOp_, Variable as Variable_, is_literal
from jx_base.language import is_op
from jx_elasticsearch.es52.expressions.false_op import MATCH_NONE
from mo_future import first
from mo_logs import Log


class RegExpOp(RegExpOp_):
    def to_es(self, schema):
        if is_literal(self.pattern) and is_op(self.var, Variable_):
            cols = schema.leaves(self.var.var)
            if len(cols) == 0:
                return MATCH_NONE
            elif len(cols) == 1:
                return {"regexp": {first(cols).es_column: self.pattern.value}}
            else:
                Log.error("regex on not supported ")
        else:
            Log.error("regex only accepts a variable and literal pattern")
