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

from jx_base.expressions import (
    MissingOp as MissingOp_,
    Variable as Variable_,
    is_literal,
)
from jx_base.language import is_op
from jx_elasticsearch.es52.painless.and_op import AndOp
from jx_elasticsearch.es52.painless.es_script import EsScript
from mo_json import BOOLEAN
from mo_logs.strings import quote


class MissingOp(MissingOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        if is_op(self.expr, Variable_):
            if self.expr.var == "_id":
                return EsScript(type=BOOLEAN, expr="false", frum=self, schema=schema)
            else:
                columns = schema.leaves(self.expr.var)
                return AndOp([
                    EsScript(
                        type=BOOLEAN,
                        expr="doc[" + quote(c.es_column) + "].empty",
                        frum=self,
                        schema=schema,
                    )
                    for c in columns
                ]).partial_eval().to_es_script(schema)
        elif is_literal(self.expr):
            return self.expr.missing().to_es_script(schema)
        else:
            return self.expr.missing().partial_eval().to_es_script(schema)
