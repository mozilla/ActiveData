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

from jx_base.expressions import FindOp as FindOp_, simplified
from jx_elasticsearch.es52.painless.and_op import AndOp
from jx_elasticsearch.es52.painless.basic_eq_op import BasicEqOp
from jx_elasticsearch.es52.painless.basic_index_of_op import BasicIndexOfOp
from jx_elasticsearch.es52.painless.eq_op import EqOp
from jx_elasticsearch.es52.painless.literal import Literal
from jx_elasticsearch.es52.painless.or_op import OrOp
from jx_elasticsearch.es52.painless.when_op import WhenOp


class FindOp(FindOp_):
    @simplified
    def partial_eval(self):
        index = self.lang[BasicIndexOfOp([
            self.value,
            self.find,
            self.start,
        ])].partial_eval()

        output = self.lang[WhenOp(
            OrOp([
                self.value.missing(),
                self.find.missing(),
                BasicEqOp([index, Literal(-1)]),
            ]),
            **{"then": self.default, "else": index}
        )].partial_eval()
        return output

    def missing(self):
        output = AndOp([
            self.default.missing(),
            OrOp([
                self.value.missing(),
                self.find.missing(),
                EqOp([
                    BasicIndexOfOp([self.value, self.find, self.start]),
                    Literal(-1),
                ]),
            ]),
        ]).partial_eval()
        return output
