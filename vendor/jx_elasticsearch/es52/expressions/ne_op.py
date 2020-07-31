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

from jx_base.expressions import NeOp as NeOp_, Variable as Variable_, is_literal
from jx_base.language import is_op
from jx_elasticsearch.es52.expressions.not_op import es_not
from jx_elasticsearch.es52.expressions.script_op import ScriptOp
from jx_elasticsearch.es52.expressions.true_op import MATCH_ALL
from mo_future import first
from mo_logs import Log


class NeOp(NeOp_):
    def to_es(self, schema):
        if is_op(self.lhs, Variable_) and is_literal(self.rhs):
            columns = schema.values(self.lhs.var)
            if len(columns) == 0:
                return MATCH_ALL
            elif len(columns) == 1:
                return es_not({"term": {first(columns).es_column: self.rhs.value}})
            else:
                Log.error("column split to multiple, not handled")
        else:
            lhs = self.lhs.partial_eval().to_es_script(schema)
            rhs = self.rhs.partial_eval().to_es_script(schema)

            if lhs.many:
                if rhs.many:
                    return es_not(ScriptOp((
                        "("
                        + lhs.expr
                        + ").size()==("
                        + rhs.expr
                        + ").size() && "
                        + "("
                        + rhs.expr
                        + ").containsAll("
                        + lhs.expr
                        + ")"
                    )).to_es(schema))
                else:
                    return es_not(ScriptOp(
                        "(" + lhs.expr + ").contains(" + rhs.expr + ")"
                    ).to_es(schema))
            else:
                if rhs.many:
                    return es_not(ScriptOp(
                        "(" + rhs.expr + ").contains(" + lhs.expr + ")"
                    ).to_es(schema))
                else:
                    return es_not(ScriptOp(
                        "(" + lhs.expr + ") != (" + rhs.expr + ")"
                    ).to_es(schema))
