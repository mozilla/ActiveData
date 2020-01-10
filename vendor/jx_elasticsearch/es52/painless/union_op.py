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

from jx_base.expressions import UnionOp as UnionOp_, merge_types
from jx_elasticsearch.es52.painless._utils import Painless
from jx_elasticsearch.es52.painless.es_script import EsScript


class UnionOp(UnionOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        code = """
        HashSet output = new HashSet();
        {{LOOPS}}
        return output.toArray();
        """
        parts = [
            Painless[t].partial_eval().to_es_script(schema, many=True)
            for t in self.terms
        ]
        loops = ["for (v in " + p.expr + ") output.add(v);" for p in parts]
        return EsScript(
            type=merge_types(p.type for p in parts),
            expr=code.replace("{{LOOPS}}", "\n".join(loops)),
            many=True,
            frum=self,
            schema=schema,
        )
