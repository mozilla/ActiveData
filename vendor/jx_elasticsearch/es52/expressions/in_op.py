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

from jx_base.expressions import NULL, InOp as BaseInOp, is_literal, TupleOp
from jx_base.language import is_op
from jx_elasticsearch.es52.expressions.eq_op import EqOp
from jx_elasticsearch.es52.expressions.false_op import MATCH_NONE
from jx_elasticsearch.es52.expressions.nested_op import NestedOp
from jx_elasticsearch.es52.expressions.or_op import OrOp
from jx_elasticsearch.es52.expressions.utils import ES52
from jx_elasticsearch.es52.expressions.variable import Variable
from jx_elasticsearch.es52.painless import Painless, AndOp
from mo_dots import is_many
from mo_future import first
from mo_json import BOOLEAN
from pyLibrary.convert import value2boolean


class InOp(BaseInOp):
    def to_es(self, schema):
        value = self.value
        if is_op(value, Variable):
            var = value.var
            cols = schema.leaves(var)
            if not cols:
                return MATCH_NONE
            col = first(cols)
            var = col.es_column

            if is_literal(self.superset):
                if col.jx_type == BOOLEAN:
                    if is_literal(self.superset) and not is_many(self.superset.value):
                        return {"term": {var: value2boolean(self.superset.value)}}
                    else:
                        return {"terms": {var: list(map(
                            value2boolean, self.superset.value
                        ))}}
                else:
                    if is_literal(self.superset) and not is_many(self.superset.value):
                        return {"term": {var: self.superset.value}}
                    else:
                        return {"terms": {var: self.superset.value}}
            elif is_op(self.superset, TupleOp):
                return (
                    OrOp([EqOp([value, s]) for s in self.superset.terms])
                    .partial_eval()
                    .to_es(schema)
                )
        if (
            is_op(value, NestedOp)
            and is_literal(self.superset)
            and is_op(value.select, Variable)
        ):
            output = (
                ES52[NestedOp(
                    path=value.path,
                    select=NULL,
                    where=AndOp([value.where, InOp([value.select, self.superset])]),
                )]
                .exists()
                .partial_eval()
                .to_es(schema)
            )
            return output
        # THE HARD WAY
        return Painless[self].to_es_script(schema).to_es(schema)
