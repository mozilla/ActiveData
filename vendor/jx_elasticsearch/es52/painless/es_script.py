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

from jx_base.expressions import EsScript as EsScript_, FALSE, NULL, ONE, TRUE, ZERO
from mo_dots import coalesce, wrap
from mo_future import PY2, text
from mo_json import BOOLEAN, INTEGER, NUMBER
from mo_logs import Log


class EsScript(EsScript_):
    __slots__ = ("simplified", "miss", "data_type", "expr", "many")

    def __init__(self, type, expr, frum, schema, miss=None, many=False):
        self.simplified = True
        object.__init__(self)
        if miss not in [None, NULL, FALSE, TRUE, ONE, ZERO]:
            if frum.lang != miss.lang:
                Log.error("logic error")

        self.miss = coalesce(
            miss, FALSE
        )  # Expression that will return true/false to indicate missing result
        self.data_type = type
        self.expr = expr
        self.many = many  # True if script returns multi-value
        self.frum = frum  # THE ORIGINAL EXPRESSION THAT MADE expr
        self.schema = schema

    @property
    def type(self):
        return self.data_type

    def __str__(self):
        """
        RETURN A SCRIPT SUITABLE FOR CODE OUTSIDE THIS MODULE (NO KNOWLEDGE OF Painless)
        :param schema:
        :return:
        """
        missing = self.miss.partial_eval()
        if missing is FALSE:
            return self.partial_eval().to_es_script(self.schema).expr
        elif missing is TRUE:
            return "null"

        return (
            "(" + missing.to_es_script(self.schema).expr + ")?null:(" + box(self) + ")"
        )

    def __add__(self, other):
        return text(self) + text(other)

    def __radd__(self, other):
        return text(other) + text(self)

    if PY2:
        __unicode__ = __str__

    def to_esfilter(self, schema):
        return {"script": es_script(text(self))}

    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        return self

    def missing(self):
        return self.miss

    def __data__(self):
        return {"script": text(self)}

    def __eq__(self, other):
        if not isinstance(other, EsScript_):
            return False
        elif self.expr == other.expr:
            return True
        else:
            return False


def box(script):
    """
    :param es_script:
    :return: TEXT EXPRESSION WITH NON OBJECTS BOXED
    """
    if script.type is BOOLEAN:
        return "Boolean.valueOf(" + text(script.expr) + ")"
    elif script.type is INTEGER:
        return "Integer.valueOf(" + text(script.expr) + ")"
    elif script.type is NUMBER:
        return "Double.valueOf(" + text(script.expr) + ")"
    else:
        return script.expr


def es_script(term):
    return wrap({"script": {"lang": "painless", "source": term}})


