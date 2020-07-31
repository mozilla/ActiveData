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

from jx_base.expressions import BooleanOp as BooleanOp_, FALSE
from jx_elasticsearch.es52.painless.es_script import EsScript
from jx_elasticsearch.es52.painless.not_op import NotOp
from jx_elasticsearch.es52.painless.when_op import WhenOp
from mo_json import BOOLEAN


class BooleanOp(BooleanOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        try:
            value = self.lang[self.term].to_es_script(schema)
        except Exception as e:
            raise e
        if value.many:
            return BooleanOp(EsScript(
                miss=value.miss,
                type=value.type,
                expr="(" + value.expr + ")[0]",
                frum=value.frum,
                schema=schema,
            )).to_es_script(schema)
        elif value.type == BOOLEAN:
            miss = value.miss
            value.miss = FALSE
            return WhenOp(
                miss, **{"then": FALSE, "else": value}
            ).partial_eval().to_es_script(schema)
        else:
            return NotOp(value.miss).partial_eval().to_es_script(schema)
