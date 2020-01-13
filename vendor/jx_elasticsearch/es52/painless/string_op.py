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

from jx_base.expressions import CoalesceOp as CoalesceOp_, StringOp as StringOp_, TRUE
from jx_base.language import is_op
from jx_elasticsearch.es52.painless._utils import empty_string_script, NUMBER_TO_STRING
from jx_elasticsearch.es52.painless.coalesce_op import CoalesceOp
from jx_elasticsearch.es52.painless.es_script import EsScript
from jx_elasticsearch.es52.painless.first_op import FirstOp
from mo_json import BOOLEAN, INTEGER, NUMBER, STRING
from mo_logs.strings import expand_template


class StringOp(StringOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        term = FirstOp(self.term).partial_eval()
        value = term.to_es_script(schema)

        if is_op(value.frum, CoalesceOp_):
            return CoalesceOp(
                [StringOp(t).partial_eval() for t in value.frum.terms]
            ).to_es_script(schema)

        if value.miss is TRUE:
            return empty_string_script
        elif value.type == BOOLEAN:
            return EsScript(
                miss=self.term.missing().partial_eval(),
                type=STRING,
                expr=value.expr + ' ? "T" : "F"',
                frum=self,
                schema=schema,
            )
        elif value.type == INTEGER:
            return EsScript(
                miss=self.term.missing().partial_eval(),
                type=STRING,
                expr="String.valueOf(" + value.expr + ")",
                frum=self,
                schema=schema,
            )
        elif value.type == NUMBER:
            return EsScript(
                miss=self.term.missing().partial_eval(),
                type=STRING,
                expr=expand_template(NUMBER_TO_STRING, {"expr": value.expr}),
                frum=self,
                schema=schema,
            )
        elif value.type == STRING:
            return value
        else:
            return EsScript(
                miss=self.term.missing().partial_eval(),
                type=STRING,
                expr=expand_template(NUMBER_TO_STRING, {"expr": value.expr}),
                frum=self,
                schema=schema,
            )

        # ((Runnable)(() -> {int a=2; int b=3; System.out.println(a+b);})).run();
        # "((Runnable)((value) -> {String output=String.valueOf(value); if (output.endsWith('.0')) {return output.substring(0, output.length-2);} else return output;})).run(" + value.expr + ")"
