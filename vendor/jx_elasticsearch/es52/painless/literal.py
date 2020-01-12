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

from jx_base.expressions import Literal as Literal_, ONE
from jx_elasticsearch.es52.painless import _utils
from jx_elasticsearch.es52.painless.null_op import null_script
from jx_elasticsearch.es52.painless.true_op import true_script
from jx_elasticsearch.es52.painless.false_op import false_script
from jx_elasticsearch.es52.painless._utils import MIN_INT32, MAX_INT32
from jx_elasticsearch.es52.painless.es_script import EsScript
from mo_dots import FlatList, data_types
from mo_future import integer_types, text
from mo_json import INTEGER, NUMBER, OBJECT, STRING
from mo_logs.strings import quote
from mo_times import Date


class Literal(Literal_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        def _convert(v):
            if v is None:
                return null_script
            if v is True:
                return true_script
            if v is False:
                return false_script
            class_ = v.__class__
            if class_ is text:
                return EsScript(type=STRING, expr=quote(v), frum=self, schema=schema)
            if class_ in integer_types:
                if MIN_INT32 <= v <= MAX_INT32:
                    return EsScript(
                        type=INTEGER, expr=text(v), frum=self, schema=schema
                    )
                else:
                    return EsScript(
                        type=INTEGER, expr=text(v) + "L", frum=self, schema=schema
                    )

            if class_ is float:
                return EsScript(
                    type=NUMBER, expr=text(v) + "D", frum=self, schema=schema
                )
            if class_ in data_types:
                return EsScript(
                    type=OBJECT,
                    expr="["
                    + ", ".join(quote(k) + ": " + _convert(vv) for k, vv in v.items())
                    + "]",
                    frum=self,
                    schema=schema,
                )
            if class_ in (FlatList, list, tuple):
                return EsScript(
                    type=OBJECT,
                    expr="[" + ", ".join(_convert(vv).expr for vv in v) + "]",
                    frum=self,
                    schema=schema,
                )
            if class_ is Date:
                return EsScript(
                    type=NUMBER, expr=text(v.unix), frum=self, schema=schema
                )

        return _convert(self._value)


_utils.Literal = Literal
