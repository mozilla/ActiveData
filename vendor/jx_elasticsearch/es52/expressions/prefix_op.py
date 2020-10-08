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
)
from jx_base.language import is_op
from jx_elasticsearch.es52.expressions import es_or
from jx_elasticsearch.es52.expressions.false_op import MATCH_NONE
from jx_elasticsearch.es52.expressions.true_op import MATCH_ALL
from mo_json import STRING, INTERNAL
from mo_logs import Log


class PrefixOp(PrefixOp_):
    def partial_eval(self, lang):
        expr = self.expr.partial_eval(lang)
        prefix = self.prefix.partial_eval(lang)

        if prefix is NULL:
            return TRUE
        if expr is NULL:
            return FALSE

        return PrefixOp(expr, prefix)

    def to_es(self, schema):
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
            cols = schema.values(expr.var, exclude_type=INTERNAL)
            if not cols:
                return MATCH_NONE
            acc = []
            for col in cols:
                if col.jx_type == STRING:
                    acc.append({"prefix": {col.es_column: self.prefix.value}})
                else:
                    Log.error(
                        'do not know how to {"prefix":{{column|quote}}} of type'
                        " {{type}}",
                        column=col.name,
                        type=col.jx_type,
                    )
            if len(acc) == 0:
                return MATCH_NONE
            elif len(acc) == 1:
                return acc[0]
            else:
                return es_or(acc)
        else:
            return (self).to_es_script(schema).to_es(schema)
