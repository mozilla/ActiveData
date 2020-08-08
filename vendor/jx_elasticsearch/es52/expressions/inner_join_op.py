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

from jx_base.expressions import InnerJoinOp as InnerJoinOp_
from jx_elasticsearch.es52.expressions import es_and
from jx_elasticsearch.es52.expressions.utils import ES52


class InnerJoinOp(InnerJoinOp_):
    def to_es(self, schema):
        acc = None
        for nest in self.nests:
            es = ES52[nest].to_es(schema)
            if not acc:
                acc = es
            else:
                es.nested.query = es_and([es.nested.query, acc])
                acc = es

        return acc.nested.inner_hits | {"query": acc.nested.query}

