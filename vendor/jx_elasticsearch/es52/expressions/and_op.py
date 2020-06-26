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


def es_and(terms):
    return dict_to_data({"bool": {"filter": terms}})


# EXPORT
from jx_elasticsearch.es52.expressions import or_op
or_op.es_and = es_and
del or_op

from jx_elasticsearch.es52.expressions import _utils
_utils.AndOp = AndOp
del _utils
