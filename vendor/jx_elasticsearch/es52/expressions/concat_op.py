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

from jx_base.expressions import ConcatOp as ConcatOp_, Variable as Variable_, is_literal
from jx_base.language import is_op
from pyLibrary.convert import string2regexp


class ConcatOp(ConcatOp_):
    def to_esfilter(self, schema):
        if is_op(self.value, Variable_) and is_literal(self.find):
            return {
                "regexp": {self.value.var: ".*" + string2regexp(self.find.value) + ".*"}
            }
        else:
            return self.to_es_script(schema).script(schema).to_esfilter(schema)
