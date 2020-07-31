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
    CoalesceOp as CoalesceOp_,
    FirstOp as FirstOp_,
    Variable as Variable_,
)
from jx_base.language import is_op
from jx_elasticsearch.es52.painless import Painless
from jx_elasticsearch.es52.painless.null_op import null_script
from jx_elasticsearch.es52.painless.es_script import EsScript

CoalesceOp, Variable = [None] * 2


class FirstOp(FirstOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        if is_op(self.term, Variable_):
            columns = schema.values(self.term.var)
            if len(columns) == 0:
                return null_script
            elif len(columns) == 1:
                return self.term.to_es_script(schema, many=False)
            # else:
            #     return CoalesceOp(
            #         [
            #             FirstOp(Variable(c.es_column))
            #             for c in columns
            #         ]
            #     ).to_es_script(schema)

        term = Painless[self.term].to_es_script(schema)

        if is_op(term.frum, CoalesceOp_):
            return CoalesceOp([
                FirstOp(t.partial_eval().to_es_script(schema)) for t in term.frum.terms
            ]).to_es_script(schema)

        if term.many:
            return EsScript(
                miss=term.miss,
                type=term.type,
                expr="(" + term.expr + ")[0]",
                frum=term.frum,
                schema=schema,
            ).to_es_script(schema)
        else:
            return term
