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

from jx_base.expressions import NULL, Variable as Variable_
from jx_elasticsearch.es52.painless import first_op
from jx_elasticsearch.es52.painless.coalesce_op import CoalesceOp
from jx_elasticsearch.es52.painless.es_script import EsScript
from mo_json import BOOLEAN, OBJECT, STRING
from mo_logs.strings import quote


class Variable(Variable_):
    def __init__(self, var):
        Variable_.__init__(self, var)

    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        if self.var == ".":
            return EsScript(type=OBJECT, expr="_source", frum=self)
        else:
            if self.var == "_id":
                return EsScript(
                    type=STRING,
                    expr='doc["_uid"].value.substring(doc["_uid"].value.indexOf(\'#\')+1)',
                    frum=self,
                    schema=schema,
                )

            columns = schema.values(self.var)
            acc = []
            for c in columns:
                varname = c.es_column
                frum = Variable(c.es_column)
                q = quote(varname)
                if c.multi > 1:
                    acc.append(
                        EsScript(
                            miss=frum.missing(),
                            type=c.jx_type,
                            expr="doc[" + q + "].values",
                            frum=frum,
                            schema=schema,
                            many=True
                        )
                    )
                else:
                    acc.append(
                        EsScript(
                            miss=frum.missing(),
                            type=c.jx_type,
                            expr="doc[" + q + "].value",
                            frum=frum,
                            schema=schema,
                            many=False
                        )
                    )

            if len(acc) == 0:
                return NULL.to_es_script(schema)
            elif len(acc) == 1:
                return acc[0]
            else:
                return CoalesceOp(acc).to_es_script(schema)


first_op.Variable=Variable
