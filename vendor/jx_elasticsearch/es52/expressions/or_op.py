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

from jx_base.expressions import OrOp as OrOp_, TRUE, FALSE, simplified
from jx_base.language import is_op
from jx_elasticsearch.es52.expressions._utils import ES52
from mo_dots import dict_to_data, Null
from mo_json import BOOLEAN

NotOp, es_not, es_and, EsNestedOp = [Null] * 4  # IMPORTS


class OrOp(OrOp_):
    def to_esfilter(self, schema):

        if schema.snowflake.namespace.es_cluster.version.startswith("5."):
            # VERSION 5.2.x
            # WE REQUIRE EXIT-EARLY SEMANTICS, OTHERWISE EVERY EXPRESSION IS A SCRIPT EXPRESSION
            # {"bool":{"should"  :[a, b, c]}} RUNS IN PARALLEL
            # {"bool":{"must_not":[a, b, c]}} ALSO RUNS IN PARALLEL

            # OR(x) == NOT(AND(NOT(xi) for xi in x))
            output = es_not(
                es_and(
                    [NotOp(t).partial_eval().to_esfilter(schema) for t in self.terms]
                )
            )
            return output
        else:
            # VERSION 6.2+
            return es_or(
                [ES52[t].partial_eval().to_esfilter(schema) for t in self.terms]
            )

    @simplified
    def partial_eval(self):
        if not any(is_op(t, EsNestedOp) for t in self.terms):
            # CAN NOT FACTOR EsNestedOp
            return OrOp_.partial_eval(self)

        seen_nested = False
        terms = []
        for t in self.terms:
            if is_op(t, EsNestedOp):
                seen_nested = True
            simple = self.lang[t].partial_eval()
            if simple.type != BOOLEAN:
                simple = simple.exists()

            if simple is TRUE and not seen_nested:
                return TRUE
            elif simple is FALSE:
                continue
            elif is_op(simple, OrOp):
                for tt in simple.terms:
                    if is_op(tt, EsNestedOp):
                        seen_nested = True
                    if tt in terms:
                        continue
                    terms.append(tt)
            elif simple not in terms:
                terms.append(simple)

        if len(terms) == 0:
            return FALSE
        if len(terms) == 1:
            return terms[0]
        return self.lang[OrOp(terms)]


def es_or(terms):
    return dict_to_data({"bool": {"should": terms}})


# EXPORT
from jx_elasticsearch.es52.expressions import _utils

_utils.OrOp = OrOp
del _utils
