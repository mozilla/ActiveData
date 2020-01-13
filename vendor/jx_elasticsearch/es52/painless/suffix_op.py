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

from jx_base.expressions import SuffixOp as SuffixOp_
from jx_elasticsearch.es52.painless.es_script import EsScript
from jx_elasticsearch.es52.painless.missing_op import MissingOp
from jx_elasticsearch.es52.painless.or_op import OrOp
from jx_elasticsearch.es52.painless.true_op import true_script


class SuffixOp(SuffixOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        if not self.suffix:
            return true_script
        else:
            return EsScript(
                miss=OrOp(
                    [MissingOp(self.expr), MissingOp(self.suffix)]
                ).partial_eval(),
                expr="("
                + self.expr.to_es_script(schema)
                + ").endsWith("
                + self.suffix.to_es_script(schema)
                + ")",
                frum=self,
                schema=schema,
            )
