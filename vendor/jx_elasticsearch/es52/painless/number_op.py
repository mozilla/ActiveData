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

from jx_base.expressions import CoalesceOp as CoalesceOp_, NumberOp as NumberOp_
from jx_base.language import is_op
from jx_elasticsearch.es52.painless import _utils
from jx_elasticsearch.es52.painless.literal import Literal
from jx_elasticsearch.es52.painless.null_op import null_script
from jx_elasticsearch.es52.painless.false_op import false_script
from jx_elasticsearch.es52.painless.true_op import true_script
from jx_elasticsearch.es52.painless.coalesce_op import CoalesceOp
from jx_elasticsearch.es52.painless.es_script import EsScript
from jx_elasticsearch.es52.painless.first_op import FirstOp
from mo_json import BOOLEAN, INTEGER, NUMBER, OBJECT, STRING


class NumberOp(NumberOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        term = FirstOp(self.term).partial_eval()

        value = term.to_es_script(schema)

        if is_op(value.frum, CoalesceOp_):
            return CoalesceOp([
                NumberOp(t).partial_eval().to_es_script(schema)
                for t in value.frum.terms
            ]).to_es_script(schema)

        if value is null_script:
            return Literal(0).to_es_script(schema)
        if value is false_script:
            return Literal(0).to_es_script(schema)
        if value is true_script:
            return Literal(1).to_es_script(schema)
        elif value.type == BOOLEAN:
            return EsScript(
                miss=term.missing().partial_eval(),
                type=NUMBER,
                expr="(" + value.expr + ") ? 1 : 0",
                frum=self,
                schema=schema,
            )
        elif value.type == INTEGER:
            return EsScript(
                miss=term.missing().partial_eval(),
                type=NUMBER,
                expr=value.expr,
                frum=self,
                schema=schema,
            )
        elif value.type == NUMBER:
            return EsScript(
                miss=term.missing().partial_eval(),
                type=NUMBER,
                expr=value.expr,
                frum=self,
                schema=schema,
            )
        elif value.type == STRING:
            return EsScript(
                miss=term.missing().partial_eval(),
                type=NUMBER,
                expr="Double.parseDouble(" + value.expr + ")",
                frum=self,
                schema=schema,
            )
        elif value.type == OBJECT:
            return EsScript(
                miss=term.missing().partial_eval(),
                type=NUMBER,
                expr="(("
                + value.expr
                + ") instanceof String) ? Double.parseDouble("
                + value.expr
                + ") : ("
                + value.expr
                + ")",
                frum=self,
                schema=schema,
            )


_utils.NumberOp = NumberOp
