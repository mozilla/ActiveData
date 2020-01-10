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

from jx_base.expressions import DateOp as DateOp_
from jx_elasticsearch.es52.painless.es_script import EsScript
from mo_future import text
from mo_json import NUMBER
from mo_times import Date


class DateOp(DateOp_):
    def to_es_script(self, schema):
        return EsScript(
            type=NUMBER, expr=text(Date(self.value).unix), frum=self, schema=schema
        )
