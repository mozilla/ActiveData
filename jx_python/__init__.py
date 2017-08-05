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

from collections import Mapping
from copy import copy

from mo_dots import Data, Null, startswith_field, concat_field
from mo_dots import wrap, set_default, split_field, join_field
from mo_logs import Log

from jx_python.containers import STRUCT

config = Data()   # config.default IS EXPECTED TO BE SET BEFORE CALLS ARE MADE
_ListContainer = None
_meta = None
_containers = None


def _delayed_imports():
    global _ListContainer
    global _meta
    global _containers


    from jx_python import meta as _meta
    from jx_python.containers.list_usingPythonList import ListContainer as _ListContainer
    from jx_python import containers as _containers

    _ = _ListContainer
    _ = _meta
    _ = _containers

    try:
        from pyLibrary.queries.jx_usingMySQL import MySQL
    except Exception:
        MySQL = None

    try:
        from jx_elasticsearch.jx_usingES import FromES
        from jx_elasticsearch.meta import FromESMetadata
    except Exception:
        FromES = None
        FromESMetadata = None

    set_default(_containers.type2container, {
        "elasticsearch": FromES,
        "mysql": MySQL,
        "memory": None,
        "meta": FromESMetadata
    })


def wrap_from(frum, schema=None):
    """
    :param frum:
    :param schema:
    :return:
    """
    if not _containers:
        _delayed_imports()

    frum = wrap(frum)

    if isinstance(frum, basestring):
        if not _containers.config.default.settings:
            Log.error("expecting jx_python.query.config.default.settings to contain default elasticsearch connection info")

        type_ = None
        index = frum
        if frum.startswith("meta."):
            if frum == "meta.columns":
                return _meta.singlton.meta.columns.denormalized()
            elif frum == "meta.tables":
                return _meta.singlton.meta.tables
            else:
                Log.error("{{name}} not a recognized table", name=frum)
        else:
            type_ = _containers.config.default.type
            index = split_field(frum)[0]

        settings = set_default(
            {
                "index": index,
                "name": frum,
                "exists": True,
            },
            _containers.config.default.settings
        )
        settings.type = None
        return _containers.type2container[type_](settings)
    elif isinstance(frum, Mapping) and frum.type and _containers.type2container[frum.type]:
        # TODO: Ensure the frum.name is set, so we capture the deep queries
        if not frum.type:
            Log.error("Expecting from clause to have a 'type' property")
        return _containers.type2container[frum.type](frum.settings)
    elif isinstance(frum, Mapping) and (frum["from"] or isinstance(frum["from"], (list, set))):
        from jx_python.query import QueryOp
        return QueryOp.wrap(frum, schema=schema)
    elif isinstance(frum, (list, set)):
        return _ListContainer("test_list", frum)
    else:
        return frum


class Schema(object):
    """
    A Schema MAPS ALL COLUMNS IN SNOWFLAKE FROM NAME TO COLUMN INSTANCE
    """

    def __init__(self, table_name, columns):
        """
        :param table_name: THE FACT TABLE
        :param query_path: PATH TO ARM OF SNOWFLAKE
        :param columns: ALL COLUMNS IN SNOWFLAKE
        """
        table_path = split_field(table_name)
        self.table = table_path[0]  # USED AS AN EXPLICIT STATEMENT OF PERSPECTIVE IN THE DATABASE
        self.query_path = join_field(table_path[1:])  # TODO: REPLACE WITH THE nested_path ARRAY
        self._columns = copy(columns)

        lookup = self.lookup = _index(columns, self.query_path)
        if self.query_path != ".":
            alternate = _index(columns, ".")
            for k,v in alternate.items():
                lookup.setdefault(k, v)

    def __getitem__(self, column_name):
        return self.lookup.get(column_name, Null)

    def items(self):
        return self.lookup.items()

    def get_column(self, name, table=None):
        return self.lookup[name]

    def get_column_name(self, column):
        """
        RETURN THE COLUMN NAME, FROM THE PERSPECTIVE OF THIS SCHEMA
        :param column:
        :return: NAME OF column
        """
        return column.names[self.query_path]

    def leaves(self, name):
        """
        RETURN LEAVES OF GIVEN PATH NAME
        :param name: 
        :return: 
        """
        full_name = concat_field(self.query_path, name)
        return [
            c
            for k, cs in self.lookup.items()
            # if startswith_field(k, full_name)
            for c in cs
            if c.type not in STRUCT
        ]

    def map_to_es(self):
        """
        RETURN A MAP FROM THE NAME SPACE TO THE  
        """
        full_name = self.query_path
        return set_default(
            {
                c.names[full_name]: c.es_column
                for k, cs in self.lookup.items()
                # if startswith_field(k, full_name)
                for c in cs if c.type not in STRUCT
            },
            {
                c.names["."]: c.es_column
                for k, cs in self.lookup.items()
                # if startswith_field(k, full_name)
                for c in cs if c.type not in STRUCT
            }
        )

    @property
    def columns(self):
        return copy(self._columns)


def _index(columns, query_path):
    lookup = {}
    for c in columns:
        try:
            cname = c.names[query_path]
            cs = lookup.setdefault(cname, [])
            cs.append(c)
        except Exception as e:
            Log.error("Sould not happen", cause=e)
    return lookup
