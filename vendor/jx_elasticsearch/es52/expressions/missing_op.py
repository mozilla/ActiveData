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

from jx_base.expressions import MissingOp as MissingOp_, Variable as Variable_
from jx_base.language import is_op
from jx_elasticsearch.es52.expressions.and_op import es_and
from jx_elasticsearch.es52.expressions.true_op import MATCH_ALL
from jx_elasticsearch.es52.painless import MissingOp as PainlessMissingOp
from mo_future import first


class MissingOp(MissingOp_):
    def to_esfilter(self, schema):
        if is_op(self.expr, Variable_):
            cols = schema.leaves(self.expr.var)
            if not cols:
                return MATCH_ALL
            elif len(cols) == 1:
                return es_missing(first(cols).es_column)
            else:
                return es_and([es_missing(c.es_column) for c in cols])
        else:
            return PainlessMissingOp.to_es_script(self, schema).to_esfilter(schema)

def es_missing(term):
    return {"bool": {"must_not": {"exists": {"field": term}}}}

