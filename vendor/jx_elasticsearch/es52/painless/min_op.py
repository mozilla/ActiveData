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

from jx_base.expressions import MinOp as MinOp_
from jx_elasticsearch.es52.painless.and_op import AndOp
from jx_elasticsearch.es52.painless.es_script import EsScript
from jx_elasticsearch.es52.painless.number_op import NumberOp
from mo_json import NUMBER


class MinOp(MinOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        acc = NumberOp(self.terms[-1]).partial_eval().to_es_script(schema).expr
        for t in reversed(self.terms[0:-1]):
            acc = (
                "Math.min("
                + NumberOp(t).partial_eval().to_es_script(schema).expr
                + " , "
                + acc
                + ")"
            )
        return EsScript(
            miss=AndOp([t.missing() for t in self.terms]),
            type=NUMBER,
            expr=acc,
            frum=self,
            schema=schema,
        )
