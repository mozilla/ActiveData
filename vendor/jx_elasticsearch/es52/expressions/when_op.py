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

from jx_base.expressions import WhenOp as WhenOp_
from jx_elasticsearch.es52.expressions.and_op import AndOp
from jx_elasticsearch.es52.expressions.boolean_op import BooleanOp
from jx_elasticsearch.es52.expressions.not_op import NotOp
from jx_elasticsearch.es52.expressions.or_op import OrOp


class WhenOp(WhenOp_):
    def to_es(self, schema):
        output = OrOp([
            AndOp([self.when, BooleanOp(self.then)]),
            AndOp([NotOp(self.when), BooleanOp(self.els_)]),
        ]).partial_eval()

        return output.to_es(schema)
