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

from jx_base.expressions import IntegerOp as IntegerOp_
from jx_elasticsearch.es52.painless._utils import Painless
from jx_elasticsearch.es52.painless.es_script import EsScript
from mo_json import BOOLEAN, INTEGER, NUMBER, STRING


class IntegerOp(IntegerOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        value = Painless[self.term].to_es_script(schema)
        if value.many:
            return IntegerOp(
                EsScript(
                    miss=value.missing(),
                    type=value.type,
                    expr="(" + value.expr + ")[0]",
                    frum=value.frum,
                    schema=schema,
                )
            ).to_es_script(schema)
        elif value.type == BOOLEAN:
            return EsScript(
                miss=value.missing(),
                type=INTEGER,
                expr=value.expr + " ? 1 : 0",
                frum=self,
                schema=schema,
            )
        elif value.type == INTEGER:
            return value
        elif value.type == NUMBER:
            return EsScript(
                miss=value.missing(),
                type=INTEGER,
                expr="(int)(" + value.expr + ")",
                frum=self,
                schema=schema,
            )
        elif value.type == STRING:
            return EsScript(
                miss=value.missing(),
                type=INTEGER,
                expr="Integer.parseInt(" + value.expr + ")",
                frum=self,
                schema=schema,
            )
        else:
            return EsScript(
                miss=value.missing(),
                type=INTEGER,
                expr="(("
                + value.expr
                + ") instanceof String) ? Integer.parseInt("
                + value.expr
                + ") : (int)("
                + value.expr
                + ")",
                frum=self,
                schema=schema,
            )
