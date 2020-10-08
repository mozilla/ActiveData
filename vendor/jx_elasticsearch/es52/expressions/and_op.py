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

from jx_base.expressions import AndOp as AndOp_
from jx_elasticsearch.es52.expressions.true_op import MATCH_ALL
from mo_dots import dict_to_data
from mo_imports import export


class AndOp(AndOp_):
    def to_es(self, schema):
        if not len(self.terms):
            return MATCH_ALL
        else:
            return es_and([t.to_es(schema) for t in self.terms])


def es_and(terms):
    return dict_to_data({"bool": {"filter": terms}})


export("jx_elasticsearch.es52.expressions.utils", AndOp)
export("jx_elasticsearch.es52.expressions.or_op", es_and)
