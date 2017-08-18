# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from copy import copy

from mo_dots import join_field, split_field, Null, startswith_field, concat_field, set_default
from mo_logs import Log

from jx_base import STRUCT


def _indexer(columns, query_path):
    lookup = {}
    for c in columns:
        try:
            cname = c.names[query_path]
            cs = lookup.setdefault(cname, [])
            cs.append(c)
        except Exception as e:
            Log.error("Sould not happen", cause=e)
    return lookup


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

        lookup = self.lookup = _indexer(columns, self.query_path)
        if self.query_path != ".":
            alternate = _indexer(columns, ".")
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
            if startswith_field(k, full_name)
            for c in cs
            if c.type not in STRUCT and c.es_column != "_id"
        ]

    def map_to_es(self):
        """
        RETURN A MAP FROM THE NAME SPACE TO THE es_column NAME
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


