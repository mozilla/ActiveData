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

from jx_base.expressions import DivOp as DivOp_, ZERO
from jx_elasticsearch.es52.painless.eq_op import EqOp
from jx_elasticsearch.es52.painless.es_script import EsScript
from jx_elasticsearch.es52.painless.number_op import NumberOp
from jx_elasticsearch.es52.painless.or_op import OrOp
from jx_elasticsearch.es52.painless.when_op import WhenOp
from mo_json import NUMBER


class DivOp(DivOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        lhs = NumberOp(self.lhs).partial_eval()
        rhs = NumberOp(self.rhs).partial_eval()
        script = (
            "("
            + lhs.to_es_script(schema).expr
            + ") / ("
            + rhs.to_es_script(schema).expr
            + ")"
        )

        output = WhenOp(
            OrOp([lhs.missing(), rhs.missing(), EqOp([rhs, ZERO])]),
            **{
                "then": self.default,
                "else": EsScript(type=NUMBER, expr=script, frum=self, schema=schema),
            }
        ).partial_eval().to_es_script(schema)

        return output
