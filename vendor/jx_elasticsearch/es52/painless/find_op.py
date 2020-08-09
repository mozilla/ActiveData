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

from jx_base.expressions import FindOp as FindOp_
from jx_elasticsearch.es52.painless.and_op import AndOp
from jx_elasticsearch.es52.painless.basic_eq_op import BasicEqOp
from jx_elasticsearch.es52.painless.basic_index_of_op import BasicIndexOfOp
from jx_elasticsearch.es52.painless.eq_op import EqOp
from jx_elasticsearch.es52.painless.literal import Literal
from jx_elasticsearch.es52.painless.or_op import OrOp
from jx_elasticsearch.es52.painless.when_op import WhenOp


class FindOp(FindOp_):
    def partial_eval(self, lang):
        index = self.lang[BasicIndexOfOp([
            self.value,
            self.find,
            self.start,
        ])].partial_eval(lang)

        output = self.lang[WhenOp(
            OrOp([
                self.value.missing(lang),
                self.find.missing(lang),
                BasicEqOp([index, Literal(-1)]),
            ]),
            **{"then": self.default, "else": index}
        )].partial_eval(lang)
        return output

    def missing(self, lang):
        output = AndOp([
            self.default.missing(lang),
            OrOp([
                self.value.missing(lang),
                self.find.missing(lang),
                EqOp([
                    BasicIndexOfOp([self.value, self.find, self.start]),
                    Literal(-1),
                ]),
            ]),
        ]).partial_eval(lang)
        return output
