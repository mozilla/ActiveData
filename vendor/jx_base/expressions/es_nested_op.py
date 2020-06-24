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

from jx_base.expressions._utils import simplified
from jx_base.expressions.eq_op import EqOp
from jx_base.expressions.expression import Expression, Variable
from jx_base.expressions.literal import ZERO
from jx_base.expressions.not_op import NotOp
from jx_base.expressions.null_op import NULL
from jx_base.expressions.or_op import OrOp
from jx_base.expressions.true_op import TRUE
from jx_base.expressions.variable import IDENTITY
from jx_base.language import is_op
from mo_dots import Null
from mo_json import BOOLEAN


class EsNestedOp(Expression):
    data_type = BOOLEAN
    has_simple_form = False

    __slots__ = ["frum", "select", "where", "sort", "limit"]

    def __init__(self, frum, select=IDENTITY, where=TRUE, sort=Null, limit=NULL):
        Expression.__init__(self, [frum, select, where, sort, limit])
        self.frum = frum
        self.select = select
        self.where = where
        self.sort = sort
        self.limit = limit

    @simplified
    def partial_eval(self):
        if self.missing() is TRUE:
            return NULL

        return self.lang[
            EsNestedOp(
                self.frum.partial_eval(),
                self.select.partial_eval(),
                self.where.partial_eval(),
                self.sort.partial_eval(),
                self.limit.partial_eval()
            )
        ]

    def __data__(self):
        return {
            "es.nested": {
                "from": self.frum.__data__(),
                "select": self.select.__data__(),
                "where": self.where.__data__(),
                "sort": self.sort.__data__(),
                "limit": self.limit.__data__(),
            }
        }

    def __eq__(self, other):
        return (
            is_op(other, EsNestedOp)
            and self.frum == other.frum
            and self.select == other.select
            and self.where == other.where
            and self.sort == other.sort
            and self.limit == other.limit
        )

    def vars(self):
        return (
            self.frum.vars()
            | self.select.vars()
            | self.where.vars()
            | self.sort.vars()
            | self.limit.vars()
        )

    def map(self, mapping):
        return EsNestedOp(
            frum=self.frum.map(mapping),
            select=self.select.map(mapping),
            where=self.where.map(mapping),
            sort=self.sort.map(mapping),
            limit=self.limit.map(mapping),
        )

    def invert(self):
        return self.missing()

    def missing(self):
        return OrOp([
            NotOp(self.where),
            self.frum.missing(),
            self.select.missing(),
            EqOp([self.limit,  ZERO])
         ])

    @property
    def many(self):
        return True

