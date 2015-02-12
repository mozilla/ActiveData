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
from pyLibrary.dot import literal_field

from pyLibrary.dot.dicts import Dict, nvl
from pyLibrary.parsers import Log
from pyLibrary.queries.es_query_util import aggregates1_4

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
        output = select.copy()
        output.name = nvl(select.name, select.value, select.aggregate)

        if not output.name:
            Log.error("expecting select to have a name: {{select}}", {"select": select})

        output.aggregate = nvl(canonical_aggregates.get(select.aggregate), select.aggregate, "none")
        return output


canonical_aggregates = {
    "min": "minimum",
    "max": "maximum",
    "add": "sum",
    "avg": "average",
    "mean": "average"
}
