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

from jx_base.expressions import NotLeftOp as NotLeftOp_
from jx_elasticsearch.es52.painless.es_script import EsScript
from jx_elasticsearch.es52.painless.number_op import NumberOp
from jx_elasticsearch.es52.painless.or_op import OrOp
from jx_elasticsearch.es52.painless.string_op import StringOp
from mo_json import STRING


class NotLeftOp(NotLeftOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        v = StringOp(self.value).partial_eval().to_es_script(schema).expr
        l = NumberOp(self.length).partial_eval().to_es_script(schema).expr

        expr = (
            "("
            + v
            + ").substring((int)Math.max(0, (int)Math.min("
            + v
            + ".length(), "
            + l
            + ")))"
        )
        return EsScript(
            miss=OrOp([self.value.missing(), self.length.missing()]),
            type=STRING,
            expr=expr,
            frum=self,
            schema=schema,
        )
