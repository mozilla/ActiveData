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

from jx_base.expressions import FALSE, TupleOp as TupleOp_
from jx_elasticsearch.es52.painless.es_script import EsScript
from jx_elasticsearch.es52.painless.first_op import FirstOp
from mo_future import text
from mo_json import OBJECT


class TupleOp(TupleOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        expr = (
            "new Object[]{"
            + ",".join(
                text(FirstOp(t).partial_eval().to_es_script(schema)) for t in self.terms
            )
            + "}"
        )
        return EsScript(type=OBJECT, expr=expr, many=FALSE, frum=self, schema=schema)
