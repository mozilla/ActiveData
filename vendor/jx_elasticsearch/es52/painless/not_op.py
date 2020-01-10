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

from jx_base.expressions import NotOp as NotOp_
from jx_elasticsearch.es52.painless._utils import Painless
from jx_elasticsearch.es52.painless.es_script import EsScript
from mo_json import BOOLEAN


class NotOp(NotOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        return EsScript(
            type=BOOLEAN,
            expr="!("
            + Painless[self.term].partial_eval().to_es_script(schema).expr
            + ")",
            frum=self,
            schema=schema,
        )
