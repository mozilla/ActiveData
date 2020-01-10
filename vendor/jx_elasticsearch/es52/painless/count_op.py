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

from jx_base.expressions import CountOp as CountOp_, FALSE
from jx_elasticsearch.es52.painless._utils import Painless, _count_template
from jx_elasticsearch.es52.painless.es_script import EsScript
from mo_json import INTEGER
from mo_logs.strings import expand_template


class CountOp(CountOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        return EsScript(
            miss=FALSE,
            type=INTEGER,
            expr=expand_template(
                _count_template,
                {"expr": Painless[self.terms].partial_eval().to_es_script(schema).expr},
            ),
            frum=self,
            schema=schema,
        )
