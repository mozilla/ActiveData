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

from jx_base.expressions import AndOp as AndOp_, TRUE, FALSE, simplified
from jx_base.language import is_op
from jx_elasticsearch.es52.expressions._utils import ES52
from jx_elasticsearch.es52.expressions.boolean_op import BooleanOp
from jx_elasticsearch.es52.expressions.not_op import NotOp
from jx_elasticsearch.es52.expressions.true_op import MATCH_ALL
from mo_dots import dict_to_data
from mo_json import BOOLEAN


EsNestedOp = None


class AndOp(AndOp_):
    def to_esfilter(self, schema):
        if not len(self.terms):
            return MATCH_ALL
        else:
            return es_and([ES52[t].to_esfilter(schema) for t in self.terms])

    @simplified
    def partial_eval(self):
        if any(is_op(o, EsNestedOp) for o in self.terms):
            # CAN NOT FACTOR EsNestedOp
            and_terms = []
            for i, t in enumerate(self.terms):
                simple = self.lang[BooleanOp(t)].partial_eval()
                if simple.type != BOOLEAN:
                    simple = simple.exists()

                if simple is TRUE:
                    continue
                elif simple is FALSE:
                    return FALSE
                elif is_op(simple, AndOp):
                    and_terms.extend([tt for tt in simple.terms if tt not in and_terms])
                    continue
                if NotOp(simple).partial_eval() in and_terms:
                    return FALSE
                elif simple not in and_terms:
                    and_terms.append(simple)

            if len(and_terms) == 0:
                return TRUE
            elif len(and_terms) == 1:
                return and_terms[0]
            else:
                return AndOp(and_terms)
        return AndOp_.partial_eval(self)


def es_and(terms):
    return dict_to_data({"bool": {"filter": terms}})


# EXPORT
from jx_elasticsearch.es52.expressions import or_op
or_op.es_and = es_and
del or_op

from jx_elasticsearch.es52.expressions import _utils
_utils.AndOp = AndOp
del _utils
