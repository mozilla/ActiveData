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

from jx_base.expressions._utils import simplified, TRUE

from mo_logs import Log

from mo_dots import startswith_field

from jx_base.expressions.expression import Expression
from jx_base.expressions.or_op import OrOp
from jx_base.language import is_op
from jx_base.expressions.outer_join_op import OuterJoinOp
from mo_json import BOOLEAN


class InnerJoinOp(Expression):
    data_type = BOOLEAN
    has_simple_form = False

    __slots__ = ["frum", "nests"]

    def __init__(self, frum, nests):
        Expression.__init__(self, nests)
        self.frum = frum
        self.nests = nests
        last = "."
        for n in reversed(nests):
            path = n.path.var
            if not startswith_field(path, last):
                Log.error("Expecting nests to be reverse nested order")
            last = path

    def __data__(self):
        return {
            "innerjoin": {
                "from": self.frum.__data__(),
                "nests": [n.__data__() for n in self.nests],
            }
        }

    def __eq__(self, other):
        return (
            is_op(other, OuterJoinOp)
            and self.frum == other.frum
            and self.nests == other.nests
        )

    def vars(self):
        return (
            self.frum.vars()
            | self.nests.vars()
            | self.where.vars()
            | self.sort.vars()
            | self.limit.vars()
        )

    def map(self, mapping):
        return InnerJoinOp(frum=self.frum.map(mapping), nests=self.nests.map(mapping),)

    def invert(self):
        return self.missing()

    def missing(self):
        if not self.nests:
            return TRUE

        return OrOp(
            [self.frum.missing()] + [n.missing() for n in self.nests]
        ).partial_eval()

    @property
    def many(self):
        return True

    @simplified
    def partial_eval(self):
        return self.lang[InnerJoinOp(
            frum=self.frum,
            nests=[n.partial_eval() for n in self.nests],
        )]
