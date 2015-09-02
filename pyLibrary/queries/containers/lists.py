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
from pyLibrary.dot import Dict, wrap, listwrap, unwraplist
from pyLibrary.queries import qb
from pyLibrary.queries.containers import Container
from pyLibrary.queries.es09.util import Column
from pyLibrary.queries.expressions import TRUE_FILTER, qb_expression_to_python
from pyLibrary.queries.lists.aggs import is_aggs, list_aggs


class ListContainer(Container):
    def __init__(self, frum, schema=None):
        frum = list(frum)
        Container.__init__(self, frum, schema)
        self.frum = frum
        if schema == None:
            self.schema = get_schema_from_list(frum)

    @property
    def query_path(self):
        return None

    def query(self, q):
        frum = self
        if is_aggs(q):
            frum = list_aggs(frum.data, q)
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
        temp = None
        exec("def temp(row):\n    return "+qb_expression_to_python(where))
        return ListContainer(filter(temp, self.data), self.schema)

    def sort(self, sort):
        return ListContainer(qb.sort(self.data, sort), self.schema)

    def select(self, select):
        selects = listwrap(select)
        if selects[0].value == "*" and selects[0].name == ".":
            return self

        Log.error("not implemented")

    def window(self, window):
        _ = window
        Log.error("not implemented")

    def having(self, having):
        _ = having
        Log.error("not implemented")

    def format(self, format):
        if format == "table":
            frum = convert.list2table(self.data, self.schema.keys())
        elif format == "cube":
            frum = convert.list2cube(self.data, self.schema.keys())
        else:
            frum = wrap({
                "meta": {"format": "list"},
                "data": [{k: unwraplist(v) for k, v in row.items()} for row in self.data]
            })
        return frum

    def get_columns(self, query_path=None):
        return self.schema.values()


def get_schema_from_list(frum):
    """
    SCAN THE LIST FOR COLUMN TYPES
    """
    columns = {}
    _get_schema_from_list(frum, columns, [], 0)
    return columns

def _get_schema_from_list(frum, columns, prefix, nested_path):
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
                _get_schema_from_list([value], columns, prefix + [name], nested_path)
            elif this_type == "nested":
                if not nested_path:
                    _get_schema_from_list(value, columns, prefix + [name], [name])
                else:
                    _get_schema_from_list(value, columns, prefix + [name], [nested_path[0]+"."+name]+nested_path)

    for n, t in names.items():
        full_name = ".".join(prefix + [n])
        column = Column(
            name=full_name,
            type=t,
            nested_path=nested_path
        )
        columns[columns.name] = column


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



