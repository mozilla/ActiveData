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
    is_literal,
)
from jx_base.language import is_op
from jx_elasticsearch.es52.expressions.literal import Literal
from jx_elasticsearch.es52.expressions.variable import Variable
from jx_elasticsearch.es52.expressions.utils import ES52
from jx_elasticsearch.es52.expressions.not_op import NotOp
from jx_elasticsearch.es52.expressions.reg_exp_op import RegExpOp
from jx_elasticsearch.es52.painless import Painless
from mo_imports import export
from mo_json import STRING


class FindOp(FindOp_):
    def to_es(self, schema):
        if (
            is_op(self.value, Variable)
            and is_literal(self.find)
            and self.default is NULL
            and is_literal(self.start)
            and self.start.value == 0
        ):
            columns = [c for c in schema.leaves(self.value.var) if c.jx_type == STRING]
            if len(columns) == 1:
                return {"regexp": {
                    columns[0].es_column: ".*" + re.escape(self.find.value) + ".*"
                }}
        # CONVERT TO SCRIPT, SIMPLIFY, AND THEN BACK TO FILTER
        self.simplified = False
        return self.partial_eval(Painless).to_es(schema)

    def partial_eval(self, lang):
        value = self.value.partial_eval(lang)
        find = self.find.partial_eval(lang)
        default = self.default.partial_eval(lang)
        start = self.start.partial_eval(lang)

        return FindOp([value, find], default=default, start=start)

    def missing(self, lang):
        slim = self.partial_eval(lang)
        if (
            is_op(slim.value, Variable)
            and is_literal(slim.find)
            and slim.default is NULL
            and is_literal(slim.start)
            and slim.start.value == 0
        ):
            return NotOp(RegExpOp([
                slim.value,
                Literal(".*" + re.escape(slim.find.value) + ".*"),
            ]))
        return NotOp(self.partial_eval(Painless))

    def exists(self):
        return NotOp(self.missing(ES52)).partial_eval(ES52)


export("jx_elasticsearch.es52.expressions.boolean_op", FindOp)
