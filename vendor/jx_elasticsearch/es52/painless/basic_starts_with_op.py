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

from jx_base.expressions import BasicStartsWithOp as BasicStartsWithOp_, FALSE
from jx_elasticsearch.es52.painless.false_op import false_script
from jx_elasticsearch.es52.painless._utils import (
    Painless,
    empty_string_script,
)
from jx_elasticsearch.es52.painless.es_script import EsScript
from jx_elasticsearch.es52.painless.first_op import FirstOp
from mo_json import BOOLEAN


class BasicStartsWithOp(BasicStartsWithOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        expr = Painless[FirstOp(self.value)].partial_eval().to_es_script(schema)
        if expr is empty_string_script:
            return false_script

        prefix = Painless[self.prefix].to_es_script(schema).partial_eval()
        return EsScript(
            miss=FALSE,
            type=BOOLEAN,
            expr="(" + expr.expr + ").startsWith(" + prefix.expr + ")",
            frum=self,
            schema=schema,
        )
