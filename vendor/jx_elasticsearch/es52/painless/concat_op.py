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

from jx_base.expressions import ConcatOp as ConcatOp_, NULL
from jx_elasticsearch.es52.painless.es_script import EsScript
from jx_elasticsearch.es52.painless.length_op import LengthOp
from jx_elasticsearch.es52.painless.literal import Literal
from jx_elasticsearch.es52.painless.string_op import StringOp
from jx_elasticsearch.es52.painless.when_op import WhenOp
from mo_json import STRING


class ConcatOp(ConcatOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        if len(self.terms) == 0:
            return self.default.to_es_script(schema)

        acc = []
        separator = StringOp(self.separator).partial_eval()
        sep = separator.to_es_script(schema).expr
        for t in self.terms:
            val = WhenOp(
                t.missing(),
                **{
                    "then": Literal(""),
                    "else": EsScript(
                        type=STRING,
                        expr=sep
                        + "+"
                        + StringOp(t).partial_eval().to_es_script(schema).expr,
                        frum=t,
                        schema=schema,
                    )
                    # "else": ConcatOp([sep, t])
                }
            )
            acc.append("(" + val.partial_eval().to_es_script(schema).expr + ")")
        expr_ = (
            "("
            + "+".join(acc)
            + ").substring("
            + LengthOp(separator).to_es_script(schema).expr
            + ")"
        )

        if self.default is NULL:
            return EsScript(
                miss=self.missing(), type=STRING, expr=expr_, frum=self, schema=schema
            )
        else:
            return EsScript(
                miss=self.missing(),
                type=STRING,
                expr="(("
                + expr_
                + ").length==0) ? ("
                + self.default.to_es_script(schema).expr
                + ") : ("
                + expr_
                + ")",
                frum=self,
            )
