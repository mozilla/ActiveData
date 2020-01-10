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

from jx_base.expressions import FALSE, FloorOp as FloorOp_, ONE, ZERO
from jx_elasticsearch.es52.painless.eq_op import EqOp
from jx_elasticsearch.es52.painless.es_script import EsScript
from jx_elasticsearch.es52.painless.first_op import FirstOp
from jx_elasticsearch.es52.painless.or_op import OrOp
from jx_elasticsearch.es52.painless.when_op import WhenOp
from mo_json import NUMBER


class FloorOp(FloorOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        lhs = FirstOp(self.lhs).partial_eval()
        rhs = FirstOp(self.rhs).partial_eval()

        if rhs == ONE:
            script = "(int)Math.floor(" + lhs.to_es_script(schema).expr + ")"
        else:
            rhs = rhs.to_es_script(schema)
            script = (
                "Math.floor(("
                + lhs.to_es_script(schema).expr
                + ") / ("
                + rhs.expr
                + "))*("
                + rhs.expr
                + ")"
            )

        output = (
            WhenOp(
                OrOp([lhs.missing(), rhs.missing(), EqOp([self.rhs, ZERO])]),
                **{
                    "then": self.default,
                    "else": EsScript(
                        type=NUMBER, expr=script, frum=self, miss=FALSE, schema=schema
                    ),
                }
            )
            .partial_eval()
            .to_es_script(schema)
        )
        return output
