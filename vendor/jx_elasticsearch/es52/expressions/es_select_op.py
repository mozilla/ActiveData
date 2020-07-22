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

from mo_dots import Data
from jx_base.expressions import ESSelectOp as _ESSelectOp


class ESSelectOp(_ESSelectOp):
    def to_es(self):
        return Data(
            _source=self.get_source,
            stored_fields=self.fields if not self.get_source else None,
            script_fields=self.scripts if self.scripts else None,
        )
