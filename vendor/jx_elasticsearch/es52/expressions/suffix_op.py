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

from jx_base.expressions import SuffixOp as SuffixOp_, Variable as Variable_, is_literal
from jx_base.language import is_op
from jx_elasticsearch.es52.expressions.true_op import MATCH_ALL
from mo_future import first
from pyLibrary.convert import string2regexp
from jx_elasticsearch.es52.painless import SuffixOp as PainlessSuffixOp


class SuffixOp(SuffixOp_):
    def to_es(self, schema):
        if not self.suffix:
            return MATCH_ALL
        elif is_op(self.expr, Variable_) and is_literal(self.suffix):
            var = first(schema.leaves(self.expr.var)).es_column
            return {"regexp": {var: ".*" + string2regexp(self.suffix.value)}}
        else:
            return PainlessSuffixOp.to_es_script(self, schema).to_es(schema)
