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

from jx_base.expressions.expression import Expression
from jx_base.expressions.false_op import FALSE
from jx_base.expressions.variable import IDENTITY, Variable
from mo_future import PY2
from mo_json import OBJECT

default_select = ({"name": ".", "value": IDENTITY},)


class ESSelectOp(Expression):
    data_type = OBJECT
    has_simple_form = False

    def __init__(self, path="."):
        Expression.__init__(self, [])
        self.path = path
        self.source_path = None
        self.fields = []
        self.scripts = {}

    def __data__(self):
        output = [{"name": f, "value": Variable(f)} for f in self.fields]
        if self.source_path:
            output = [default_select]
        for n, e in self.scripts.items():
            output.append({"name": n, "value": e})
        return output

    def vars(self):
        output = set(Variable(f) for f in self.fields)
        for e in self.scripts.values():
            output |= e.vars()
        return output

    def map(self, mapping):
        return self
        # Log.error("not supported")

    def invert(self, lang):
        return FALSE

    def missing(self, lang):
        return FALSE

    def __bool__(self):
        return True if self.source_path or self.fields or self.scripts else False


if PY2:
    ESSelectOp.__nonzero__ = ESSelectOp.__bool__
