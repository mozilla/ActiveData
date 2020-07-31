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

from jx_base.expressions import BooleanOp as BooleanOp_, Variable as Variable_
from jx_base.language import is_op
from jx_elasticsearch.es52.expressions.utils import ES52
from jx_elasticsearch.es52.expressions.exists_op import es_exists
from jx_elasticsearch.es52.painless import Painless
from mo_imports import expect

FindOp = expect("FindOp")


class BooleanOp(BooleanOp_):
    def to_es(self, schema):
        if is_op(self.term, Variable_):
            return es_exists(self.term.var)
        elif is_op(self.term, FindOp):
            return ES52[self.term].to_es(schema)
        else:
            return Painless[self].to_es_script(schema).to_es(schema)
