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

from jx_base.expressions.false_op import FALSE
from jx_base.expressions.literal import Literal
from mo_imports import export
from mo_json import BOOLEAN


class TrueOp(Literal):
    data_type = BOOLEAN

    def __new__(cls, *args, **kwargs):
        return object.__new__(cls, *args, **kwargs)

    def __init__(self, op=None, term=None):
        Literal.__init__(self, True)

    @classmethod
    def define(cls, expr):
        return TRUE

    def __nonzero__(self):
        return True

    def __eq__(self, other):
        return (other is TRUE) or (other is True)

    def __data__(self):
        return True

    def vars(self):
        return set()

    def map(self, map_):
        return self

    def missing(self, lang):
        return FALSE

    def invert(self, lang):
        return FALSE

    def __call__(self, row=None, rownum=None, rows=None):
        return True

    def __unicode__(self):
        return "true"

    def __str__(self):
        return b"true"

    def __bool__(self):
        return True

    def __nonzero__(self):
        return True


TRUE = TrueOp()


export("jx_base.expressions.literal", TRUE)
export("jx_base.expressions.false_op", TRUE)
export("jx_base.expressions._utils", TRUE)
export("jx_base.expressions.expression", TRUE)
