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

from jx_base.expressions import WhenOp as WhenOp_, FALSE, TRUE
from jx_elasticsearch.es52.painless import _utils
from jx_elasticsearch.es52.painless._utils import Painless
from jx_elasticsearch.es52.painless.es_script import EsScript
from jx_elasticsearch.es52.painless.false_op import false_script
from jx_elasticsearch.es52.painless.true_op import true_script
from mo_json import INTEGER, NUMBER, NUMBER_TYPES
from mo_logs import Log


class WhenOp(WhenOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        if self.simplified:
            when = Painless[self.when].to_es_script(schema)
            then = Painless[self.then].to_es_script(schema)
            els_ = Painless[self.els_].to_es_script(schema)

            if when is true_script:
                return then
            elif when is false_script:
                return els_
            elif then.miss is TRUE:
                return EsScript(
                    miss=self.missing(),
                    type=els_.type,
                    expr=els_.expr,
                    frum=self,
                    schema=schema,
                )
            elif els_.miss is TRUE:
                return EsScript(
                    miss=self.missing(),
                    type=then.type,
                    expr=then.expr,
                    frum=self,
                    schema=schema,
                )

            elif then.miss is TRUE or els_.miss is FALSE or then.type == els_.type:
                return EsScript(
                    miss=self.missing(),
                    type=then.type if els_.miss is TRUE else els_.type,
                    expr="("
                    + when.expr
                    + ") ? ("
                    + then.expr
                    + ") : ("
                    + els_.expr
                    + ")",
                    frum=self,
                    schema=schema,
                )
            elif then.type in NUMBER_TYPES and els_.type in NUMBER_TYPES:
                return EsScript(
                    miss=self.missing(),
                    type=NUMBER,
                    expr="("
                    + when.expr
                    + ") ? ("
                    + then.expr
                    + ") : ("
                    + els_.expr
                    + ")",
                    frum=self,
                    schema=schema,
                )
            else:
                Log.error("do not know how to handle: {{self}}", self=self.__data__())
        else:
            return self.partial_eval().to_es_script(schema)


_utils.WhenOp = WhenOp
