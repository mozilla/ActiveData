# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from jx_base.expressions import (
    BasicStartsWithOp as BasicStartsWithOp_,
    Variable as Variable_,
    is_literal,
)
from jx_base.language import is_op
from jx_elasticsearch.es52.painless import false_script
from jx_elasticsearch.es52.util import MATCH_ALL, MATCH_NONE
from mo_future import first
from jx_elasticsearch.es52.painless import BasicStartsWithOp as PainlessBasicStartsWithOp


class BasicStartsWithOp(BasicStartsWithOp_):
    def to_esfilter(self, schema):
        if not self.value:
            return MATCH_ALL
        elif is_op(self.value, Variable_) and is_literal(self.prefix):
            var = first(schema.leaves(self.value.var)).es_column
            return {"prefix": {var: self.prefix.value}}
        else:
            output = PainlessBasicStartsWithOp.to_es_script(self, schema)
            if output is false_script:
                return MATCH_NONE
            return output
