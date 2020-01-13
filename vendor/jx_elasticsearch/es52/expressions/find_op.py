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

import re

from jx_base.expressions import (
    FindOp as FindOp_,
    NULL,
    Variable as Variable_,
    is_literal,
    simplified,
)
from jx_base.language import is_op
from jx_elasticsearch.es52.painless import Painless
from jx_elasticsearch.es52.expressions._utils import ES52
from jx_elasticsearch.es52.expressions.not_op import NotOp
from mo_json import STRING

BooleanOp = None

class FindOp(FindOp_):
    def to_esfilter(self, schema):
        if (
            is_op(self.value, Variable_)
            and is_literal(self.find)
            and self.default is NULL
            and is_literal(self.start)
            and self.start.value == 0
        ):
            columns = [c for c in schema.leaves(self.value.var) if c.jx_type == STRING]
            if len(columns) == 1:
                return {
                    "regexp": {
                        columns[0].es_column: ".*" + re.escape(self.find.value) + ".*"
                    }
                }
        # CONVERT TO SCRIPT, SIMPLIFY, AND THEN BACK TO FILTER
        self.simplified = False
        return ES52[Painless[self].partial_eval()].to_esfilter(schema)

    @simplified
    def partial_eval(self):
        value = self.value.partial_eval()
        find = self.find.partial_eval()
        default = self.default.partial_eval()
        start = self.start.partial_eval()

        return FindOp([value, find], default=default, start=start)

    def missing(self):
        return NotOp(self)

    def exists(self):
        return BooleanOp(self)
