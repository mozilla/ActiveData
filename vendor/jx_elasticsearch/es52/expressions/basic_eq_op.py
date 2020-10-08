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

from jx_base.expressions import (
    BasicEqOp as BasicEqOp_,
    Variable as Variable_,
    is_literal,
    NestedOp,
    IDENTITY,
    AndOp,
)
from jx_base.language import is_op
from mo_dots import is_many
from mo_future import first


class BasicEqOp(BasicEqOp_):
    def partial_eval(self, lang):
        if is_op(self.lhs, NestedOp):
            return lang.NestedOp(
                path=self.lhs.frum.partial_eval(lang),
                select=IDENTITY,
                where=AndOp([
                    self.lhs.where,
                    BasicEqOp([self.lhs.select, self.rhs]),
                ]).partial_eval(lang),
                sort=self.lhs.sort.partial_eval(lang),
                limit=self.limit.partial_eval(lang),
            )
        return lang.BasicEqOp([
            self.lhs.partial_eval(lang),
            self.rhs.partial_eval(lang),
        ])

    def to_es(self, schema):
        if is_op(self.lhs, Variable_) and is_literal(self.rhs):
            lhs = self.lhs.var
            cols = schema.leaves(lhs)
            if cols:
                lhs = first(cols).es_column
            rhs = self.rhs.value
            if is_many(rhs):
                if len(rhs) == 1:
                    return {"term": {lhs: first(rhs)}}
                else:
                    return {"terms": {lhs: rhs}}
            else:
                return {"term": {lhs: rhs}}
        else:
            return (self).to_es_script(schema).to_es(schema)
