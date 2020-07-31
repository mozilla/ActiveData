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

from jx_base.expressions import NeOp as NeOp_
from jx_elasticsearch.es52.painless.basic_eq_op import BasicEqOp
from jx_elasticsearch.es52.painless.case_op import CaseOp
from jx_elasticsearch.es52.painless.not_op import NotOp
from jx_elasticsearch.es52.painless.when_op import WhenOp


class NeOp(NeOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        return CaseOp([
            WhenOp(self.lhs.missing(), **{"then": NotOp(self.rhs.missing())}),
            WhenOp(self.rhs.missing(), **{"then": NotOp(self.lhs.missing())}),
            NotOp(BasicEqOp([self.lhs, self.rhs])),
        ]).partial_eval().to_es_script(schema)
