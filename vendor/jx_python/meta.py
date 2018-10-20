# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import sqlite3
from collections import Mapping
from contextlib import contextmanager
from datetime import date
from datetime import datetime
from decimal import Decimal

import jx_base
from jx_base import Column, Table
from jx_base.schema import Schema
from jx_python import jx
from mo_collections import UniqueIndex
from mo_dots import Data, concat_field, listwrap, unwraplist, NullType, FlatList, split_field, join_field, ROOT_PATH, wrap, coalesce, Null
from mo_future import none_type, text_type, long, PY2
from mo_json import python_type_to_json_type, STRUCT, NUMBER, INTEGER, STRING, json2value, value2json
from mo_json.typed_encoder import untype_path, unnest_path
from mo_logs import Log, Except
from mo_threads import Lock, Till, Queue, Thread
from mo_times.dates import Date
from pyLibrary.sql import sql_iso, sql_list, SQL_SELECT, SQL_ORDERBY, SQL_FROM, SQL_WHERE, SQL_AND
from pyLibrary.sql.sqlite import quote_column, json_type_to_sqlite_type, quote_value

DEBUG = True
singlton = None
DB_FILE = "activedata_metadata.sqlite"
db_table_name = quote_column("meta.columns")

INSERT, UPDATE, DELETE = 'insert', 'update', 'delete'


class ColumnList(Table, jx_base.Container):
    """
    OPTIMIZED FOR THE PARTICULAR ACCESS PATTERNS USED
    """

    def __init__(self):
        Table.__init__(self, "meta.columns")
        self.data = {}  # MAP FROM ES_INDEX TO (abs_column_name to COLUMNS)
        self.locker = Lock()
        self._schema = None
        self.db = sqlite3.connect(
            database=DB_FILE,
            check_same_thread=False,
            isolation_level=None
        )
        self.last_load = Null
        self.todo = Queue("update columns to db")  # HOLD (action, column) PAIR, WHERE action in ['insert', 'update']
        self._load_db()
        Thread.run("update " + DB_FILE, self._update_database_worker)

    @contextmanager
    def _transaction(self):
        self.db.execute("BEGIN")
        try:
            yield
            self.db.execute("COMMIT")
        except Exception as e:
            e = Except.wrap(e)
            self.db.execute("ROLLBACK")
            Log.error("Transaction failed", cause=e)

    def _query(self, query):
        result = Data()
        curr = self.db.execute(query)
        result.meta.format = "table"
        result.header = [d[0] for d in curr.description] if curr.description else None
        result.data = curr.fetchall()
        return result

    def _create_db(self):
        with self._transaction():
            self.db.execute(
                "CREATE TABLE " + db_table_name +
                sql_iso(sql_list(
                    [
                        quote_column(c.name) + " " + json_type_to_sqlite_type[c.jx_type]
                        for c in METADATA_COLUMNS
                    ] + [
                        "PRIMARY KEY" + sql_iso(sql_list(map(quote_column, ["es_index", "es_column"])))
                    ]
                ))
            )

            for c in METADATA_COLUMNS:
                self._add(c)
                self._insert_column(c)

    def _load_db(self):
        self.last_load = Date.now()

        result = self._query(
            SQL_SELECT + "name" +
            SQL_FROM + "sqlite_master" +
            SQL_WHERE + SQL_AND.join([
                "name=" + db_table_name,
                "type=" + quote_value("table")
            ])
        )
        if not result.data:
            self._create_db()
            return

        result = self._query(
            SQL_SELECT + all_columns +
            SQL_FROM + db_table_name +
            SQL_ORDERBY + sql_list(map(quote_column, ["es_index", "name", "es_column"]))
        )

        with self.locker:
            for r in result.data:
                c = row_to_column(result.header, r)
                self._add(c)

    def _update_database_worker(self, please_stop):
        while not please_stop:
            try:
                with self._transaction():
                    result = self._query(
                        SQL_SELECT + all_columns +
                        SQL_FROM + db_table_name +
                        SQL_WHERE + "last_updated>" + quote_value(self.last_load) +
                        SQL_ORDERBY + sql_list(map(quote_column, ["es_index", "name", "es_column"]))
                    )

                with self.locker:
                    for r in result.data:
                        c = row_to_column(result.header, r)
                        self._add(c)
                        if c.last_updated > self.last_load:
                            self.last_load = c.last_updated

                updates = self.todo.pop_all()
                DEBUG and updates and Log.note("{{num}} columns to push to db", num=len(updates))
                for action, column in updates:
                    while not please_stop:
                        try:
                            with self._transaction():
                                DEBUG and Log.note("update db for {{table}}.{{column}}", table=column.es_index, column=column.es_column)
                                if action is UPDATE:
                                    self.db.execute(
                                        "UPDATE" + db_table_name +
                                        "SET" + sql_list([
                                            "count=" + quote_value(column.count),
                                            "cardinality=" + quote_value(column.cardinality),
                                            "multi=" + quote_value(column.multi),
                                            "partitions=" + quote_value(value2json(column.partitions)),
                                            "last_updated=" + quote_value(column.last_updated)
                                        ]) +
                                        SQL_WHERE + SQL_AND.join([
                                            "es_index = " + quote_value(column.es_index),
                                            "es_column = " + quote_value(column.es_column),
                                            "last_updated < " + quote_value(column.last_updated)
                                        ])
                                    )
                                elif action is DELETE:
                                    self.db.execute(
                                        "DELETE FROM" + db_table_name +
                                        SQL_WHERE + SQL_AND.join([
                                            "es_index = " + quote_value(column.es_index),
                                            "es_column = " + quote_value(column.es_column)
                                        ])
                                    )
                                else:
                                    self._insert_column(column)
                            break
                        except Exception as e:
                            e = Except.wrap(e)
                            if "database is locked" in e:
                                Log.note("metadata database is locked")
                                Till(seconds=1).wait()
                                break
                            else:
                                Log.warning("problem updataing database", cause=e)

            except Exception as e:
                Log.warning("problem updataing database", cause=e)

            (Till(seconds=10) | please_stop).wait()

    def _insert_column(self, column):
        try:
            self.db.execute(
                "INSERT INTO" + db_table_name + sql_iso(all_columns) +
                "VALUES" + sql_iso(sql_list([
                    quote_value(column[c.name]) if c.name not in ("nested_path", "partitions") else quote_value(value2json(column[c.name]))
                    for c in METADATA_COLUMNS
                ]))
            )
        except Exception as e:
            e = Except.wrap(e)
            if "UNIQUE constraint failed" in e:
                # THIS CAN HAPPEN BECAUSE todo HAS OLD COLUMN DATA
                self.todo.add((UPDATE, column))
            else:
                Log.error("do not know how to handle", cause=e)


    def __copy__(self):
        output = object.__new__(ColumnList)
        Table.__init__(output, "meta.columns")
        output.data = {
            t: {c: list(cs) for c, cs in dd.items()}
            for t, dd in self.data.items()
        }
        output.locker = Lock()
        output._schema = None
        return output

    def find(self, es_index, abs_column_name=None):
        with self.locker:
            if es_index.startswith("meta."):
                self._update_meta()

            if not abs_column_name:
                return [c for cs in self.data.get(es_index, {}).values() for c in cs]
            else:
                return self.data.get(es_index, {}).get(abs_column_name, [])

    def extend(self, columns):
        self.dirty = True
        with self.locker:
            for column in columns:
                self._add(column)

    def add(self, column):
        self.dirty = True
        with self.locker:
            canonical = self._add(column)
            self.todo.add((INSERT if canonical is column else UPDATE, canonical))
            return canonical

    def remove_table(self, table_name):
        del self.data[table_name]

    def _add(self, column):
        columns_for_table = self.data.setdefault(column.es_index, {})
        existing_columns = columns_for_table.setdefault(column.name, [])

        for canonical in existing_columns:
            if canonical is column:
                return canonical
            if canonical.es_type == column.es_type:
                if column.last_updated > canonical.last_updated:
                    for key in Column.__slots__:
                        old_value = canonical[key]
                        new_value = column[key]
                        if new_value == None:
                            pass  # DO NOT BOTHER CLEARING OLD VALUES (LIKE cardinality AND paritiions)
                        elif new_value == old_value:
                            pass  # NO NEED TO UPDATE WHEN NO CHANGE MADE (COMMON CASE)
                        else:
                            canonical[key] = new_value
                return canonical
        existing_columns.append(column)
        return column

    def _update_meta(self):
        if not self.dirty:
            return

        for mcl in self.data.get("meta.columns").values():
            for mc in mcl:
                count = 0
                values = set()
                objects = 0
                multi = 1
                for column in self._all_columns():
                    value = column[mc.name]
                    if value == None:
                        pass
                    else:
                        count += 1
                        if isinstance(value, list):
                            multi = max(multi, len(value))
                            try:
                                values |= set(value)
                            except Exception:
                                objects += len(value)
                        elif isinstance(value, Mapping):
                            objects += 1
                        else:
                            values.add(value)
                mc.count = count
                mc.cardinality = len(values) + objects
                mc.partitions = jx.sort(values)
                mc.multi = multi
                mc.last_updated = Date.now()
        self.dirty = False

    def _all_columns(self):
        return [
            column
            for t, cs in self.data.items()
            for _, css in cs.items()
            for column in css
        ]

    def __iter__(self):
        with self.locker:
            self._update_meta()
            return iter(self._all_columns())

    def __len__(self):
        return self.data['meta.columns']['es_index'].count

    def update(self, command):
        self.dirty = True
        try:
            command = wrap(command)
            DEBUG and Log.note("Update {{timestamp}}: {{command|json}}", command=command, timestamp=Date(command['set'].last_updated))
            eq = command.where.eq
            if eq.es_index:
                if len(eq) == 1:
                    if unwraplist(command.clear) == ".":
                        with self.locker:
                            del self.data[eq.es_index]
                            with self._transaction():
                                self.db.execute("DELETE FROM "+db_table_name+SQL_WHERE+" es_index="+quote_value(eq.es_index))
                            return

                    # FASTEST
                    all_columns = self.data.get(eq.es_index, {}).values()
                    with self.locker:
                        columns = [
                            c
                            for cs in all_columns
                            for c in cs
                        ]
                elif eq.es_column and len(eq) == 2:
                    # FASTER
                    all_columns = self.data.get(eq.es_index, {}).values()
                    with self.locker:
                        columns = [
                            c
                            for cs in all_columns
                            for c in cs
                            if c.es_column == eq.es_column
                        ]

                else:
                    # SLOWER
                    all_columns = self.data.get(eq.es_index, {}).values()
                    with self.locker:
                        columns = [
                            c
                            for cs in all_columns
                            for c in cs
                            if all(c[k] == v for k, v in eq.items())  # THIS LINE IS VERY SLOW
                        ]
            else:
                columns = list(self)
                columns = jx.filter(columns, command.where)

            with self.locker:
                for col in columns:
                    DEBUG and Log.note("Update column {{table}}.{{column}}", table=col.es_index, column=col.es_column)
                    for k in command["clear"]:
                        if k == ".":
                            self.todo.add((DELETE, col))
                            lst = self.data[col.es_index]
                            cols = lst[col.name]
                            cols.remove(col)
                            if len(cols) == 0:
                                del lst[col.name]
                                if len(lst) == 0:
                                    del self.data[col.es_index]
                        else:
                            col[k] = None

                    for k, v in command.set.items():
                        # if k == 'partitions':
                        #     Log.note("set partitions on {{id}}", id=id(col))
                        #     def check(please_stop):
                        #         while not please_stop:
                        #             Log.note("{{id}}: {{parts}}", id=id(col), parts=col[k])
                        #             Till(seconds=0.05).wait()
                        #     Thread.run("check", check)
                        col[k] = v
                    self.todo.add((UPDATE, col))
        except Exception as e:
            Log.error("should not happen", cause=e)

    def query(self, query):
        # NOT EXPECTED TO BE RUN
        Log.error("not")
        with self.locker:
            self._update_meta()
            if not self._schema:
                self._schema = Schema(".", [c for cs in self.data["meta.columns"].values() for c in cs])
            snapshot = self._all_columns()

        from jx_python.containers.list_usingPythonList import ListContainer
        query.frum = ListContainer("meta.columns", snapshot, self._schema)
        return jx.run(query)

    def groupby(self, keys):
        with self.locker:
            self._update_meta()
            return jx.groupby(self.__iter__(), keys)

    @property
    def schema(self):
        if not self._schema:
            with self.locker:
                self._update_meta()
                self._schema = Schema(".", [c for cs in self.data["meta.columns"].values() for c in cs])
        return self._schema

    @property
    def namespace(self):
        return self

    def get_table(self, table_name):
        if table_name != "meta.columns":
            Log.error("this container has only the meta.columns")
        return self

    def denormalized(self):
        """
        THE INTERNAL STRUCTURE FOR THE COLUMN METADATA IS VERY DIFFERENT FROM
        THE DENORMALIZED PERSPECITVE. THIS PROVIDES THAT PERSPECTIVE FOR QUERIES
        """
        with self.locker:
            self._update_meta()
            output = [
                {
                    "table": c.es_index,
                    "name": untype_path(c.name),
                    "cardinality": c.cardinality,
                    "es_column": c.es_column,
                    "es_index": c.es_index,
                    "last_updated": c.last_updated,
                    "count": c.count,
                    "nested_path": [unnest_path(n) for n in c.nested_path],
                    "es_type": c.es_type,
                    "type": c.jx_type
                }
                for tname, css in self.data.items()
                for cname, cs in css.items()
                for c in cs
                if c.jx_type not in STRUCT  # and c.es_column != "_id"
            ]

        from jx_python.containers.list_usingPythonList import ListContainer
        return ListContainer(
            self.name,
            data=output,
            schema=jx_base.Schema(
                "meta.columns",
                SIMPLE_METADATA_COLUMNS
            )
        )


def get_schema_from_list(table_name, frum):
    """
    SCAN THE LIST FOR COLUMN TYPES
    """
    columns = UniqueIndex(keys=("name",))
    _get_schema_from_list(frum, ".", parent=".", nested_path=ROOT_PATH, columns=columns)
    return Schema(table_name=table_name, columns=list(columns))


def _get_schema_from_list(frum, table_name, parent, nested_path, columns):
    """
    :param frum: The list
    :param table_name: Name of the table this list holds records for
    :param parent: parent path
    :param nested_path: each nested array, in reverse order
    :param columns: map from full name to column definition
    :return:
    """

    for d in frum:
        row_type = _type_to_name[d.__class__]

        if row_type != "object":
            full_name = parent
            column = columns[full_name]
            if not column:
                column = Column(
                    name=concat_field(table_name, full_name),
                    es_column=full_name,
                    es_index=".",
                    jx_type=python_type_to_json_type[d.__class__],
                    es_type=row_type,
                    last_updated=Date.now(),
                    nested_path=nested_path
                )
                columns.add(column)
            column.es_type = _merge_type[column.es_type][row_type]
            column.jx_type = _merge_type[coalesce(column.jx_type, "undefined")][row_type]
        else:
            for name, value in d.items():
                full_name = concat_field(parent, name)
                column = columns[full_name]
                if not column:
                    column = Column(
                        name=concat_field(table_name, full_name),
                        es_column=full_name,
                        es_index=".",
                        es_type="undefined",
                        last_updated=Date.now(),
                        nested_path=nested_path
                    )
                    columns.add(column)
                if isinstance(value, (list, set)):  # GET TYPE OF MULTIVALUE
                    v = list(value)
                    if len(v) == 0:
                        this_type = "undefined"
                    elif len(v) == 1:
                        this_type = _type_to_name[v[0].__class__]
                    else:
                        this_type = _type_to_name[v[0].__class__]
                        if this_type == "object":
                            this_type = "nested"
                else:
                    this_type = _type_to_name[value.__class__]
                new_type = _merge_type[column.es_type][this_type]
                column.es_type = new_type

                if this_type == "object":
                    _get_schema_from_list([value], table_name, full_name, nested_path, columns)
                elif this_type == "nested":
                    np = listwrap(nested_path)
                    newpath = unwraplist([join_field(split_field(np[0]) + [name])] + np)
                    _get_schema_from_list(value, table_name, full_name, newpath, columns)


METADATA_COLUMNS = (
    [
        Column(
            name=c,
            es_index="meta.columns",
            es_column=c,
            es_type="keyword",
            jx_type=STRING,
            last_updated=Date.now(),
            nested_path=ROOT_PATH
        )
        for c in ["name", "es_type", "jx_type", "nested_path", "es_column", "es_index", "partitions"]
    ] + [
        Column(
            name=c,
            es_index="meta.columns",
            es_column=c,
            es_type="integer",
            jx_type=INTEGER,
            last_updated=Date.now(),
            nested_path=ROOT_PATH
        )
        for c in ["count", "cardinality", "multi"]
    ] + [
        Column(
            name="last_updated",
            es_index="meta.columns",
            es_column="last_updated",
            es_type="double",
            jx_type=NUMBER,
            last_updated=Date.now(),
            nested_path=ROOT_PATH
        )
    ]
)


def row_to_column(header, row):
    return Column(**{
        h: c if c is None or h not in ("nested_path", "partitions") else json2value(c)
        for h, c in zip(header, row)
    })


all_columns = sql_list([quote_column(c.name) for c in METADATA_COLUMNS])


SIMPLE_METADATA_COLUMNS = (  # FOR PURLY INTERNAL PYTHON LISTS, NOT MAPPING TO ANOTHER DATASTORE
    [
        Column(
            name=c,
            es_index="meta.columns",
            es_column=c,
            es_type="string",
            jx_type=STRING,
            last_updated=Date.now(),
            nested_path=ROOT_PATH
        )
        for c in ["table", "name", "type", "nested_path"]
    ] + [
        Column(
            name=c,
            es_index="meta.columns",
            es_column=c,
            es_type="long",
            jx_type=INTEGER,
            last_updated=Date.now(),
            nested_path=ROOT_PATH
        )
        for c in ["count", "cardinality", "multi"]
    ] + [
        Column(
            name="last_updated",
            es_index="meta.columns",
            es_column="last_updated",
            es_type="time",
            jx_type=NUMBER,
            last_updated=Date.now(),
            nested_path=ROOT_PATH
        )
    ]
)

_type_to_name = {
    none_type: "undefined",
    NullType: "undefined",
    bool: "boolean",
    str: "string",
    text_type: "string",
    int: "integer",
    float: "double",
    Data: "object",
    dict: "object",
    set: "nested",
    list: "nested",
    FlatList: "nested",
    Date: "double",
    Decimal: "double",
    datetime: "double",
    date: "double"
}

if PY2:
    _type_to_name[long] = "integer"

_merge_type = {
    "undefined": {
        "undefined": "undefined",
        "boolean": "boolean",
        "integer": "integer",
        "long": "long",
        "float": "float",
        "double": "double",
        "number": "number",
        "string": "string",
        "object": "object",
        "nested": "nested"
    },
    "boolean": {
        "undefined": "boolean",
        "boolean": "boolean",
        "integer": "integer",
        "long": "long",
        "float": "float",
        "double": "double",
        "number": "number",
        "string": "string",
        "object": None,
        "nested": None
    },
    "integer": {
        "undefined": "integer",
        "boolean": "integer",
        "integer": "integer",
        "long": "long",
        "float": "float",
        "double": "double",
        "number": "number",
        "string": "string",
        "object": None,
        "nested": None
    },
    "long": {
        "undefined": "long",
        "boolean": "long",
        "integer": "long",
        "long": "long",
        "float": "double",
        "double": "double",
        "number": "number",
        "string": "string",
        "object": None,
        "nested": None
    },
    "float": {
        "undefined": "float",
        "boolean": "float",
        "integer": "float",
        "long": "double",
        "float": "float",
        "double": "double",
        "number": "number",
        "string": "string",
        "object": None,
        "nested": None
    },
    "double": {
        "undefined": "double",
        "boolean": "double",
        "integer": "double",
        "long": "double",
        "float": "double",
        "double": "double",
        "number": "number",
        "string": "string",
        "object": None,
        "nested": None
    },
    "number": {
        "undefined": "number",
        "boolean": "number",
        "integer": "number",
        "long": "number",
        "float": "number",
        "double": "number",
        "number": "number",
        "string": "string",
        "object": None,
        "nested": None
    },
    "string": {
        "undefined": "string",
        "boolean": "string",
        "integer": "string",
        "long": "string",
        "float": "string",
        "double": "string",
        "number": "string",
        "string": "string",
        "object": None,
        "nested": None
    },
    "object": {
        "undefined": "object",
        "boolean": None,
        "integer": None,
        "long": None,
        "float": None,
        "double": None,
        "number": None,
        "string": None,
        "object": "object",
        "nested": "nested"
    },
    "nested": {
        "undefined": "nested",
        "boolean": None,
        "integer": None,
        "long": None,
        "float": None,
        "double": None,
        "number": None,
        "string": None,
        "object": "nested",
        "nested": "nested"
    }
}
