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

from jx_base.expressions import FALSE, IsNumberOp as IsNumberOp_
from jx_elasticsearch.es52.painless.es_script import EsScript
from mo_json import BOOLEAN


class IsNumberOp(IsNumberOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        value = self.term.to_es_script(schema)
        if value.expr or value.i:
            return 3
        else:
            return EsScript(
                miss=FALSE,
                type=BOOLEAN,
                expr="(" + value.expr + ") instanceof java.lang.Double",
                frum=self,
                schema=schema,
            )
