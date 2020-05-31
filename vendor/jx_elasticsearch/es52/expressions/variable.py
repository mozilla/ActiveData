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

from jx_base.expressions import Variable as Variable_
from jx_elasticsearch.es52.expressions.and_op import es_and
from jx_elasticsearch.es52.expressions.exists_op import es_exists
from jx_elasticsearch.es52.expressions.false_op import MATCH_NONE
from mo_future import first
from mo_json import BOOLEAN, STRUCT


class Variable(Variable_):
    def to_esfilter(self, schema):
        v = self.var
        cols = schema.values(v, STRUCT)
        if len(cols) == 0:
            return MATCH_NONE
        elif len(cols) == 1:
            c = first(cols)
            return (
                {"term": {c.es_column: True}}
                if c.es_type == BOOLEAN
                else es_exists(c.es_column)
            )
        else:
            return es_and(
                [
                    {"term": {c.es_column: True}}
                    if c.es_type == BOOLEAN
                    else es_exists(c.es_column)
                    for c in cols
                ]
            )
