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

from jx_base.expressions import BasicIndexOfOp as BasicIndexOfOp_, FALSE
from jx_elasticsearch.es52.painless.es_script import EsScript
from jx_elasticsearch.es52.painless.integer_op import IntegerOp
from jx_elasticsearch.es52.painless.string_op import StringOp
from mo_json import INTEGER


class BasicIndexOfOp(BasicIndexOfOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        v = StringOp(self.value).to_es_script(schema).expr
        find = StringOp(self.find).to_es_script(schema).expr
        start = IntegerOp(self.start).to_es_script(schema).expr

        return EsScript(
            miss=FALSE,
            type=INTEGER,
            expr="(" + v + ").indexOf(" + find + ", " + start + ")",
            frum=self,
            schema=schema,
        )
