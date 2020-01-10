# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from jx_base.expressions import AndOp as AndOp_, TRUE
from jx_elasticsearch.es52.painless import _utils
from jx_elasticsearch.es52.painless._utils import Painless
from jx_elasticsearch.es52.painless.es_script import EsScript
from mo_json import BOOLEAN


class AndOp(AndOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        if not self.terms:
            return TRUE.to_es_script()
        else:
            return EsScript(
                type=BOOLEAN,
                expr=" && ".join(
                    "(" + Painless[t].to_es_script(schema).expr + ")"
                    for t in self.terms
                ),
                frum=self,
                schema=schema,
            )


_utils.AndOp = AndOp
