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

from jx_base.expressions import EsNestedOp as EsNestedOp_


class EsNestedOp(EsNestedOp_):
    def to_esfilter(self, schema):
        if self.path.var == ".":
            return self.select.to_es() | {"query": self.query.to_esfilter(schema), "from": 0}
        else:
            return {
                "nested": {
                    "path": self.path.var,
                    "query": self.query.to_esfilter(schema),
                    "inner_hits": (self.select.to_es() | {"size": 100000})
                    if self.select
                    else None,
                }
            }


# EXPORT
from jx_elasticsearch.es52.expressions import and_op
and_op.EsNestedOp = EsNestedOp
del and_op

from jx_elasticsearch.es52.expressions import or_op
or_op.EsNestedOp = EsNestedOp
del or_op
