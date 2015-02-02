# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from pyLibrary.dot.dicts import Dict, nvl

INDEX_CACHE = {}  # MATCH NAMES TO FULL CONNECTION INFO



def _normalize_select(select, schema=None):
    if isinstance(select, basestring):
        if schema:
            s = schema[select]
            if s:
                return s.getSelect()
        return Dict(
            name=select.rstrip("."),  # TRAILING DOT INDICATES THE VALUE, BUT IS INVALID FOR THE NAME
            value=select,
            aggregate="none"
        )
    else:
        if not select.name:
            select = select.copy()
            select.name = nvl(select.value, select.aggregate)

        select.aggregate = nvl(select.aggregate, "none")
        return select
