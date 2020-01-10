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

from jx_base.expressions import OrOp as OrOp_
from jx_elasticsearch.es52.expressions._utils import ES52
from jx_elasticsearch.es52.expressions.and_op import es_and
from mo_dots import wrap

NotOp, es_not = [None] * 2


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


def es_or(terms):
    return wrap({"bool": {"should": terms}})
