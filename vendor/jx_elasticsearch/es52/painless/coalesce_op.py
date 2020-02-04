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

from jx_base.expressions import CoalesceOp as CoalesceOp_, FALSE, NULL, TRUE
from jx_elasticsearch.es52.painless import first_op
from jx_elasticsearch.es52.painless.and_op import AndOp
from jx_elasticsearch.es52.painless.es_script import EsScript
from jx_elasticsearch.es52.painless.first_op import FirstOp
from jx_elasticsearch.es52.painless.not_op import NotOp
from mo_json import INTEGER, NUMBER, OBJECT, NUMBER_TYPES


class CoalesceOp(CoalesceOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        if not self.terms:
            return NULL.to_es_script(schema)
        # acc.miss WILL SAY IF THIS COALESCE RETURNS NULL,
        # acc.expr WILL ASSUMED TO BE A VALUE, SO THE LAST TERM IS ASSUMED NOT NULL
        v = self.terms[-1]
        acc = FirstOp(v).partial_eval().to_es_script(schema)
        for v in reversed(self.terms[:-1]):
            m = v.missing().partial_eval()
            e = NotOp(m).partial_eval().to_es_script(schema)
            r = FirstOp(v).partial_eval().to_es_script(schema)

            if r.miss is TRUE:
                continue
            elif r.miss is FALSE:
                acc = r
                continue
            elif acc.type == r.type or acc.miss is TRUE:
                new_type = r.type
            elif acc.type in NUMBER_TYPES and r.type in NUMBER_TYPES:
                new_type = NUMBER
            else:
                new_type = OBJECT

            acc = EsScript(
                miss=AndOp([acc.miss, m]).partial_eval(),
                type=new_type,
                expr="(" + e.expr + ") ? (" + r.expr + ") : (" + acc.expr + ")",
                frum=self,
                schema=schema,
            )
        return acc


first_op.CoalesceOp = CoalesceOp
