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
from collections import Mapping

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict, wrap, listwrap, unwraplist, DictList
from pyLibrary.queries import jx
from pyLibrary.queries.containers import Container
from pyLibrary.queries.domains import is_keyword
from pyLibrary.queries.expressions import TRUE_FILTER, jx_expression, Expression
from pyLibrary.queries.lists.aggs import is_aggs, list_aggs
from pyLibrary.queries.meta import Column
from pyLibrary.thread.threads import Lock
from pyLibrary.times.dates import Date


class ListContainer(Container):
    def __init__(self, name, data, schema=None):
        #TODO: STORE THIS LIKE A CUBE FOR FASTER ACCESS AND TRANSFORMATION
        data = list(data)
        Container.__init__(self, data, schema)
        if schema == None:
            self.schema = get_schema_from_list(data)
        else:
            self.schema = schema
        self.name = name
        self.data = data
        self.locker = Lock()  # JUST IN CASE YOU WANT TO DO MORE THAN ONE THING

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
        #TODO: ADD EXTRA COLUMN DESCRIPTIONS TO RESULTING SCHEMA
        for param in q.window:
            frum.window(param)

        return frum

    def update(self, command):
        """
        EXPECTING command == {"set":term, "clear":term, "where":where}
        THE set CLAUSE IS A DICT MAPPING NAMES TO VALUES
        THE where CLAUSE IS A JSON EXPRESSION FILTER
        """
        command = wrap(command)
        if command.where == None:
            filter_ = lambda: True
        else:
            filter_ = _exec("temp = lambda row: " + jx_expression(command.where).to_python())


        for c in self.data:
            if filter_(c):
                for k in listwrap(command["clear"]):
                    c[k] = None
                for k, v in command.set.items():
                    c[k] = v

    def filter(self, where):
        return self.where(where)

    def where(self, where):
        temp = None
        if isinstance(where, Mapping):
            exec("def temp(row):\n    return "+jx_expression(where).to_python())
        elif isinstance(where, Expression):
            exec("def temp(row):\n    return "+where.to_python())
        else:
            temp = where

        return ListContainer("from "+self.name, filter(temp, self.data), self.schema)

    def sort(self, sort):
        return ListContainer("from "+self.name, jx.sort(self.data, sort), self.schema)

    def select(self, select):
        selects = listwrap(select)
        if selects[0].value == "." and selects[0].name == ".":
            return self

        for s in selects:
            if not isinstance(s.value, basestring) or not is_keyword(s.value):
                Log.error("selecting on structure, or expressions, not supported yet")

        #TODO: DO THIS WITH JUST A SCHEMA TRANSFORM, DO NOT TOUCH DATA
        #TODO: HANDLE STRUCTURE AND EXPRESSIONS
        new_schema = {s.name: self.schema[s.value] for s in selects}
        new_data = [{s.name: d[s.value] for s in selects} for d in self.data]
        return ListContainer("from "+self.name, data=new_data, schema=new_schema)

    def window(self, window):
        _ = window
        jx.window(self.data, window)
        return self

    def having(self, having):
        _ = having
        Log.error("not implemented")

    def format(self, format):
        if format == "table":
            frum = convert.list2table(self.data, self.schema.keys())
        elif format == "cube":
            frum = convert.list2cube(self.data, self.schema.keys())
        else:
            frum = self.to_dict()

        return frum

    def insert(self, documents):
        self.data.extend(documents)

    def extend(self, documents):
        self.data.extend(documents)

    def add(self, doc):
        self.data.append(doc)

    def to_dict(self):
        return wrap({
            "meta": {"format": "list"},
            "data": [{k: unwraplist(v) for k, v in row.items()} for row in self.data]
        })

    def get_columns(self, table_name=None):
        return self.schema.values()

    def __getitem__(self, item):
        return self.data[item]

    def __iter__(self):
        return self.data.__iter__()

    def __len__(self):
        return len(self.data)

def get_schema_from_list(frum):
    """
    SCAN THE LIST FOR COLUMN TYPES
    """
    columns = {}
    _get_schema_from_list(frum, columns, prefix=[], nested_path=[])
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
                np = listwrap(nested_path)
                newpath = unwraplist([".".join((np[0], name))]+np)
                _get_schema_from_list(value, columns, prefix + [name], newpath)

    for n, t in names.items():
        full_name = ".".join(prefix + [n])
        column = Column(
            table=".",
            name=full_name,
            es_column=full_name,
            type=t,
            nested_path=nested_path
        )
        columns[column.name] = column


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
    list: "nested",
    DictList: "nested",
    Date: "double"
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



def _exec(code):
    try:
        temp = None
        exec code
        return temp
    except Exception, e:
        Log.error("Could not execute {{code|quote}}", code=code, cause=e)
