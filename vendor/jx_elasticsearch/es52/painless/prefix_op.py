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

from jx_base.expressions import PrefixOp as PrefixOp_
from jx_elasticsearch.es52.painless.es_script import EsScript
from jx_elasticsearch.es52.painless.true_op import true_script
from mo_json import BOOLEAN


class PrefixOp(PrefixOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        if not self.expr:
            return true_script
        else:
            return EsScript(
                type=BOOLEAN,
                expr="("
                + str(self.expr.to_es_script(schema))
                + ").startsWith("
                + str(self.prefix.to_es_script(schema))
                + ")",
                frum=self,
                schema=schema,
            )
