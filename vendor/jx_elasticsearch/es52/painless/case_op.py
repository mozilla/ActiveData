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

from jx_base.expressions import CaseOp as CaseOp_
from jx_elasticsearch.es52.painless._utils import Painless
from jx_elasticsearch.es52.painless.when_op import WhenOp


class CaseOp(CaseOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        acc = Painless[self.whens[-1]].partial_eval().to_es_script(schema)
        for w in reversed(self.whens[0:-1]):
            acc = (
                WhenOp(w.when, **{"then": w.then, "else": acc})
                .partial_eval()
                .to_es_script(schema)
            )
        return acc
