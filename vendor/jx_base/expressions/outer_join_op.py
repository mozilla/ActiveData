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

from jx_base.expressions.null_op import NULL

from jx_base.expressions.false_op import FALSE

from jx_base.expressions._utils import simplified
from jx_base.expressions.expression import Expression
from jx_base.expressions.or_op import OrOp
from jx_base.language import is_op
from mo_dots import startswith_field
from mo_json import BOOLEAN
from mo_logs import Log
from mo_math import UNION


class OuterJoinOp(Expression):
    data_type = BOOLEAN
    has_simple_form = False

    __slots__ = ["frum", "nests"]

    def __init__(self, frum, nests):
        """
        A SEQUENCE OF NESTED (OUTER) JOINS FOR A QUERY
        :param frum: THE TABLE OF DOCUMENTS
        :param nests: LIST OF OUTER JOINS (deepest first)
        """
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
        return {"outerjoin": {
            "from": self.frum.__data__(),
            "nests": [n.__data__() for n in self.nests],
        }}

    def __eq__(self, other):
        return (
            is_op(other, OuterJoinOp)
            and self.frum == other.frum
            and self.nests == other.nests
        )

    def vars(self):
        return UNION(
            [self.frum.vars(), self.where.vars(), self.sort.vars(), self.limit.vars()]
            + [n.vars() for n in self.nests.vars()]
        )

    def map(self, mapping):
        return OuterJoinOp(frum=self.frum.map(mapping), nests=self.nests.map(mapping))

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
        nests = []
        for n in self.nests:
            n = n.partial_eval()
            if n.where is FALSE:
                nests = []  # ALL DEEPER IS NOTHING
            else:
                nests.append(n)

        if nests:
            return self.lang[OuterJoinOp(
                frum=self.frum.partial_eval(), nests=nests
            )]
        else:
            return NULL

