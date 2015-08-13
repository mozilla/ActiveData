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
from __future__ import division
from __future__ import absolute_import
from pyLibrary import convert

from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict, wrap
from pyLibrary.queries.containers import Container
from pyLibrary.queries.expressions import TRUE_FILTER
from pyLibrary.queries.list.aggs import is_aggs, list_aggs


class ListContainer(Container):
    def __init__(self, frum, schema=None):
        Container.__init__(self, frum, schema)
        self.frum = list(frum)
        if schema == None:
            self.schema = get_schema_from_list(frum)

    def query(self, q):
        frum = self.frum
        if is_aggs(q):
            frum = list_aggs(frum, q)
        else:  # SETOP
            try:
                if q.filter != None or q.esfilter != None:
                    Log.error("use 'where' clause")
            except AttributeError, e:
                pass

            if q.where is not TRUE_FILTER:
                frum = frum.filter(q.where)

            if q.sort:
                frum = frum.sort(q.sort)

            if q.select:
                frum = frum.select(q.select)

        for param in q.window:
            frum = frum.window(param)

        return frum.format(q.format)



    def filter(self, where):
        return self.where(where)

    def where(self, where):
        _ = where
        Log.error("not implemented")

    def sort(self, sort):
        _ = sort
        Log.error("not implemented")

    def select(self, select):
        _ = select
        Log.error("not implemented")

    def window(self, window):
        _ = window
        Log.error("not implemented")

    def having(self, having):
        _ = having
        Log.error("not implemented")

    def format(self, format):
        if format == "table":
            frum = convert.list2table(self.data)
            frum.meta.format = "table"
        else:
            frum = wrap({
                "meta": {"format": "list"},
                "data": self.data
            })

    def get_columns(self, frum):
        return self.schema.values()


def get_schema_from_list(frum):
    """
    SCAN THE LIST FOR COLUMN TYPES
    """
    columns = {}
    _get_schema_from_list(frum, columns, [], 0)
    return columns

def _get_schema_from_list(frum, columns, prefix, depth):
    """
    SCAN THE LIST FOR COLUMN TYPES
    """
    names = {}
    for d in frum:
        for name, value in d.items():
            agg_type = names.get(name, "undefined")
            this_type = _type_to_name[value.__class__]
            new_type = _merge_type[agg_type][this_type]
            names[name] = new_type

            if this_type == "object":
                _get_schema_from_list([value], columns, prefix + [name], depth)
            elif this_type == "nested":
                _get_schema_from_list(value, columns, prefix + [name], depth+1)

    for n, t in names.items():
        full_name = ".".join(prefix + [n])
        column = {"name": full_name, "value": full_name, "type": t, "depth": depth}
        columns[full_name] = column


_type_to_name = {
    None: "undefined",
    str: "string",
    unicode: "string",
    int: "integer",
    long: "long",
    float: "double",
    Dict: "object",
    dict: "object",
    set: "nested",
    list: "nested"
}

_merge_type = {
    "undefined": {
        "boolean": "boolean",
        "integer": "integer",
        "long": "long",
        "float": "float",
        "double": "double",
        "string": "string",
        "object": "object",
        "nested": "nested"
    },
    "boolean": {
        "boolean": "boolean",
        "integer": "integer",
        "long": "long",
        "float": "float",
        "double": "double",
        "string": "string",
        "object": None,
        "nested": None
    },
    "integer": {
        "boolean": "integer",
        "integer": "integer",
        "long": "long",
        "float": "float",
        "double": "double",
        "string": "string",
        "object": None,
        "nested": None
    },
    "long": {
        "boolean": "long",
        "integer": "long",
        "long": "long",
        "float": "double",
        "double": "double",
        "string": "string",
        "object": None,
        "nested": None
    },
    "float": {
        "boolean": "float",
        "integer": "float",
        "long": "double",
        "float": "float",
        "double": "double",
        "string": "string",
        "object": None,
        "nested": None
    },
    "double": {
        "boolean": "double",
        "integer": "double",
        "long": "double",
        "float": "double",
        "double": "double",
        "string": "string",
        "object": None,
        "nested": None
    },
    "string": {
        "boolean": "string",
        "integer": "string",
        "long": "string",
        "float": "string",
        "double": "string",
        "string": "string",
        "object": None,
        "nested": None
    },
    "object": {
        "boolean": None,
        "integer": None,
        "long": None,
        "float": None,
        "double": None,
        "string": None,
        "object": "object",
        "nested": "nested"
    },
    "nested": {
        "boolean": None,
        "integer": None,
        "long": None,
        "float": None,
        "double": None,
        "string": None,
        "object": "nested",
        "nested": "nested"
    }
}



