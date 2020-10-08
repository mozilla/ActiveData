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

from jx_base.expressions import FalseOp, extend, FALSE
from jx_elasticsearch.es52.painless.es_script import EsScript
from mo_dots import Null
from mo_json import BOOLEAN


@extend(FalseOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    return false_script


false_script = EsScript(type=BOOLEAN, expr="false", frum=FALSE, schema=Null)
