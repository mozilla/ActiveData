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
from mo_dots import Data
from mo_future.exports import export


class InnerJoinOp(InnerJoinOp_):
    def to_esfilter(self, schema):
        selection = Data(
            _source=self.get_source,
            stored_fields=self.fields if not self.get_source else None,
            script_fields=self.scripts if self.scripts else None,
        )

        if self.frum.var == ".":
            return self.select.to_es() | {"query": self.where.to_esfilter(schema), "from": 0}
        else:
            return {
                "nested": {
                    "path": self.frum.var,
                    "query": self.where.to_esfilter(schema),
                    "inner_hits": (self.select.to_es() | {"size": 100000})
                    if self.select
                    else None,
                }
            }



export("jx_elasticsearch.es52.expressions.and_op", InnerJoinOp)
export("jx_elasticsearch.es52.expressions.or_op", InnerJoinOp)
export("jx_elasticsearch.es52.expressions._utils", InnerJoinOp)
export("jx_elasticsearch.es52.expressions.eq_op", InnerJoinOp)
