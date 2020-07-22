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
    MissingOp as MissingOp_,
    NotOp as NotOp_,
    Variable as Variable_,
)
from jx_base.language import is_op
from jx_elasticsearch.es52.expressions.utils import ES52
from jx_elasticsearch.es52.expressions.false_op import MATCH_NONE
from jx_elasticsearch.es52.expressions.or_op import es_or
from mo_dots import dict_to_data
from mo_future import first
from mo_future.exports import export
from mo_json import STRUCT


class NotOp(NotOp_):
    def to_es(self, schema):
        if is_op(self.term, MissingOp_) and is_op(self.term.expr, Variable_):
            # PREVENT RECURSIVE LOOP
            v = self.term.expr.var
            cols = schema.values(v, STRUCT)
            if len(cols) == 0:
                return MATCH_NONE
            elif len(cols) == 1:
                return {"exists": {"field": first(cols).es_column}}
            else:
                return es_or([{"exists": {"field": c.es_column}} for c in cols])
        else:
            operand = ES52[self.term].to_es(schema)
            return es_not(operand)


def es_not(term):
    return dict_to_data({"bool": {"must_not": term}})


export("jx_elasticsearch.es52.expressions.or_op", es_not)
export("jx_elasticsearch.es52.expressions.or_op", NotOp)
