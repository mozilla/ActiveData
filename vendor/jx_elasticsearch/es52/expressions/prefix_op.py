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
    FALSE,
    NULL,
    PrefixOp as PrefixOp_,
    StringOp as StringOp_,
    TRUE,
    Variable as Variable_,
    is_literal,
    simplified,
)
from jx_base.language import is_op
from jx_elasticsearch.es52.expressions.false_op import MATCH_NONE
from jx_elasticsearch.es52.expressions.true_op import MATCH_ALL
from jx_elasticsearch.es52.painless import StringOp as PainlessStringOp, PrefixOp as PainlessPrefixOp
from mo_future import first


class PrefixOp(PrefixOp_):
    @simplified
    def partial_eval(self):
        expr = PainlessStringOp(self.expr).partial_eval()
        prefix = PainlessStringOp(self.prefix).partial_eval()

        if prefix is NULL:
            return TRUE
        if expr is NULL:
            return FALSE

        return PrefixOp([expr, prefix])

    def to_esfilter(self, schema):
        if is_literal(self.prefix) and not self.prefix.value:
            return MATCH_ALL

        expr = self.expr

        if expr is NULL:
            return MATCH_NONE
        elif not expr:
            return MATCH_ALL

        if is_op(expr, StringOp_):
            expr = expr.term

        if is_op(expr, Variable_) and is_literal(self.prefix):
            col = first(schema.leaves(expr.var))
            if not col:
                return MATCH_NONE
            return {"prefix": {col.es_column: self.prefix.value}}
        else:
            return PainlessPrefixOp.to_es_script(self, schema).to_esfilter(schema)
