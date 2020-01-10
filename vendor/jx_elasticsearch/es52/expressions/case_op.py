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

from jx_base.expressions import CaseOp as CaseOp_
from jx_elasticsearch.es52.expressions.and_op import AndOp
from jx_elasticsearch.es52.expressions.or_op import OrOp
from mo_json import BOOLEAN
from mo_logs import Log


class CaseOp(CaseOp_):
    def to_esfilter(self, schema):
        if self.type == BOOLEAN:
            return (
                OrOp(
                    [AndOp([w.when, w.then]) for w in self.whens[:-1]] + self.whens[-1:]
                )
                .partial_eval()
                .to_esfilter(schema)
            )
        else:
            Log.error("do not know how to handle")
            return self.to_es_script(schema).script(schema).to_esfilter(schema)
