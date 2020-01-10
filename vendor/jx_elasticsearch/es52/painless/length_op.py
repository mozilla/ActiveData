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

from jx_base.expressions import LengthOp as LengthOp_
from jx_elasticsearch.es52.painless.es_script import EsScript
from jx_elasticsearch.es52.painless.string_op import StringOp
from mo_json import INTEGER


class LengthOp(LengthOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        value = StringOp(self.term).to_es_script(schema)
        missing = self.term.missing().partial_eval()
        return EsScript(
            miss=missing,
            type=INTEGER,
            expr="(" + value.expr + ").length()",
            frum=self,
            schema=schema,
        )
