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

from jx_base.expressions.and_op import AndOp

from jx_base.expressions import (
    EqOp as EqOp_,
    FALSE,
    TRUE,
    Variable as Variable_,
    is_literal,
    simplified,
)
from jx_base.language import is_op
from jx_elasticsearch.es52.expressions.basic_eq_op import BasicEqOp
from jx_elasticsearch.es52.expressions.literal import Literal
from jx_elasticsearch.es52.expressions.utils import ES52
from jx_elasticsearch.es52.expressions.case_op import CaseOp
from jx_elasticsearch.es52.expressions.or_op import OrOp
from jx_elasticsearch.es52.expressions.when_op import WhenOp
from jx_elasticsearch.es52.util import pull_functions
from jx_python.jx import value_compare
from mo_dots import Data, is_container
from mo_future import first
from mo_imports import expect
from mo_json import BOOLEAN, python_type_to_json_type, NUMBER_TYPES, same_json_type, OBJECT
from mo_logs import Log
from pyLibrary.convert import string2boolean

NestedOp = expect("NestedOp")


class EqOp(EqOp_):
    @simplified
    def partial_eval(self):
        lhs = ES52[self.lhs].partial_eval()
        rhs = ES52[self.rhs].partial_eval()

        if is_literal(lhs):
            if is_literal(rhs):
                return FALSE if value_compare(lhs.value, rhs.value) else TRUE
            else:
                lhs, rhs = rhs, lhs  # FLIP SO WE CAN USE TERMS FILTER

        if is_literal(rhs) and same_json_type(lhs.type, BOOLEAN):
            # SPECIAL CASE true == "T"
            rhs = string2boolean(rhs.value)
            if rhs is None:
                return FALSE
            rhs = Literal(rhs)
            return EqOp([lhs, rhs])
        if lhs.type != OBJECT and rhs.type != OBJECT and not same_json_type(lhs.type, rhs.type):
            # OBJECT MEANS WE REALLY DO NOT KNOW THE TYPE
            return FALSE
        if is_op(lhs, NestedOp):
            return self.lang[NestedOp(
                path=lhs.frum, where=AndOp([lhs.where, EqOp([lhs.select, rhs])])
            )]

        return EqOp([lhs, rhs])

    def to_es(self, schema):
        if is_op(self.lhs, Variable_) and is_literal(self.rhs):
            rhs = self.rhs.value
            lhs = self.lhs.var
            cols = schema.leaves(lhs)
            if not cols:
                Log.warning(
                    "{{col}} does not exist while processing {{expr}}",
                    col=lhs,
                    expr=self.__data__(),
                )

            if is_container(rhs):
                if len(rhs) == 1:
                    rhs = rhs[0]
                else:
                    types = Data()  # MAP JSON TYPE TO LIST OF LITERALS
                    for r in rhs:
                        types[python_type_to_json_type[r.__class__]] += [r]
                    if len(types) == 1:
                        jx_type, values = first(types.items())
                        for c in cols:
                            if same_json_type(jx_type, c.jx_type):
                                return {"terms": {c.es_column: values}}
                        return FALSE.to_es(schema)
                    else:
                        return (
                            OrOp([
                                EqOp([self.lhs, values]) for t, values in types.items()
                            ])
                            .partial_eval()
                            .to_es(schema)
                        )

            for c in cols:
                if c.jx_type == BOOLEAN:
                    rhs = pull_functions[c.jx_type](rhs)
                rhs_type = python_type_to_json_type[rhs.__class__]
                if rhs_type == c.jx_type or (
                    rhs_type in NUMBER_TYPES and c.jx_type in NUMBER_TYPES
                ):
                    return {"term": {c.es_column: rhs}}
            return FALSE.to_es(schema)
        else:
            return ES52[
                CaseOp([
                    WhenOp(self.lhs.missing(), **{"then": self.rhs.missing()}),
                    WhenOp(self.rhs.missing(), **{"then": FALSE}),
                    BasicEqOp([self.lhs, self.rhs]),
                ]).partial_eval()
            ].to_es(schema)
