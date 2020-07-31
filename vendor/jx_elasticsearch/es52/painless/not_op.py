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

from jx_base.expressions import NotOp as NotOp_
from jx_elasticsearch.es52.painless._utils import Painless
from jx_elasticsearch.es52.painless.es_script import EsScript
from jx_elasticsearch.es52.painless.null_op import null_script
from jx_elasticsearch.es52.painless.false_op import false_script
from jx_elasticsearch.es52.painless.true_op import true_script
from mo_json import BOOLEAN


class NotOp(NotOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        value = Painless[self.term].partial_eval().to_es_script(schema)

        if value is false_script:
            return true_script
        elif value is true_script:
            return false_script
        elif value is null_script:
            return null_script

        return EsScript(
            type=BOOLEAN, expr="!(" + value.expr + ")", frum=self, schema=schema,
        )
