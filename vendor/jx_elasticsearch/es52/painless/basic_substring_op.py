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

from jx_base.expressions import BasicSubstringOp as BasicSubstringOp_, FALSE
from jx_elasticsearch.es52.painless.es_script import EsScript
from jx_elasticsearch.es52.painless.integer_op import IntegerOp
from jx_elasticsearch.es52.painless.string_op import StringOp
from mo_json import STRING


class BasicSubstringOp(BasicSubstringOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        v = StringOp(self.value).partial_eval().to_es_script(schema).expr
        start = IntegerOp(self.start).partial_eval().to_es_script(schema).expr
        end = IntegerOp(self.end).partial_eval().to_es_script(schema).expr

        return EsScript(
            miss=FALSE,
            type=STRING,
            expr="(" + v + ").substring(" + start + ", " + end + ")",
            frum=self,
            schema=schema,
        )
