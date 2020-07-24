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

from jx_base.expressions.literal import Literal
from mo_imports import export, expect
from mo_json import BOOLEAN

TRUE, = expect("TRUE")


class FalseOp(Literal):
    data_type = BOOLEAN

    def __new__(cls, *args, **kwargs):
        return object.__new__(cls, *args, **kwargs)

    def __init__(self, op=None, term=None):
        Literal.__init__(self, False)

    @classmethod
    def define(cls, expr):
        return FALSE

    def __nonzero__(self):
        return False

    def __eq__(self, other):
        return (other is FALSE) or (other is False)

    def __data__(self):
        return False

    def vars(self):
        return set()

    def map(self, map_):
        return self

    def missing(self):
        return FALSE

    def invert(self):
        return TRUE

    def __call__(self, row=None, rownum=None, rows=None):
        return False

    def __unicode__(self):
        return "false"

    def __str__(self):
        return b"false"

    def __bool__(self):
        return False


FALSE = FalseOp()

export("jx_base.expressions._utils", FALSE)
export("jx_base.expressions.expression", FALSE)
export("jx_base.expressions.literal", FALSE)
