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

from jx_base.expressions import RangeOp as RangeOp_
from jx_python.expressions._utils import Python


class RangeOp(RangeOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return (
            "("
            + (self.then).to_python(not_null=not_null)
            + ") if ("
            + (self.when).to_python(boolean=True)
            + ") else ("
            + (self.els_).to_python(not_null=not_null)
            + ")"
        )
