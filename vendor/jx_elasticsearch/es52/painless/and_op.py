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

from jx_base.expressions import AndOp as AndOp_
from jx_elasticsearch.es52.painless import _utils
from jx_elasticsearch.es52.painless._utils import Painless
from jx_elasticsearch.es52.painless.es_script import EsScript
from jx_elasticsearch.es52.painless.true_op import true_script
from jx_elasticsearch.es52.painless.false_op import false_script
from mo_json import BOOLEAN


class AndOp(AndOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        ands = [Painless[t].to_es_script(schema) for t in self.terms]

        # TODO: WE SHOULD NOT BE SIMPLIFYING AT THIS POINT
        if all(a is true_script for a in ands):
            return true_script
        elif any(a is false_script for a in ands):
            return false_script

        return EsScript(
            type=BOOLEAN,
            expr=" && ".join("(" + a + ")" for a in ands),
            frum=self,
            schema=schema,
        )


_utils.AndOp = AndOp
