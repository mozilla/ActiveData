# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from collections import Mapping

import jx_base
from jx_base import Column, Table
from jx_base.schema import Schema
from jx_python import jx
from mo_collections import UniqueIndex
from mo_dots import (
    Data,
    FlatList,
    Null,
    NullType,
    ROOT_PATH,
    concat_field,
    is_container,
    is_data,
    is_list,
    join_field,
    listwrap,
    split_field,
    unwraplist,
    wrap,
)
from mo_future import binary_type, items, long, none_type, reduce, text_type
from mo_json import INTEGER, NUMBER, STRING, STRUCT, python_type_to_json_type
from mo_json.typed_encoder import unnest_path, untype_path, untyped
from mo_logs import Except, Log
from mo_threads import Lock, Queue, Thread, Till
from mo_times.dates import Date

DEBUG = True
singlton = None
META_INDEX_NAME = "meta.columns"
META_TYPE_NAME = "column"

DELETE_INDEX, UPDATE, DELETE = "delete_index", "update", "delete"


class ColumnList(Table, jx_base.Container):
    """
    OPTIMIZED FOR THE PARTICULAR ACCESS PATTERNS USED
    """

    def __init__(self, es_cluster):
        Table.__init__(self, META_INDEX_NAME)
        self.data = {}  # MAP FROM ES_INDEX TO (abs_column_name to COLUMNS)
        self.locker = Lock()
        self._schema = None
        self.dirty = False
        self.es_cluster = es_cluster
        self.es_index = None
        self.last_load = Null
        self.todo = Queue(
            "update columns to db"
        )  # HOLD (action, column) PAIR, WHERE action in ['insert', 'update']
        self._db_load()
        Thread.run("update " + META_INDEX_NAME, self._db_worker)

    def _query(self, query):
        result = Data()
        curr = self.es_cluster.execute(query)
        result.meta.format = "table"
        result.header = [d[0] for d in curr.description] if curr.description else None
        result.data = curr.fetchall()
        return result

    def _db_create(self):
        schema = {
            "settings": {"index.number_of_shards": 1, "index.number_of_replicas": 2},
            "mappings": {META_TYPE_NAME: {}},
        }

        self.es_index = self.es_cluster.create_index(
            index=META_INDEX_NAME, schema=schema
        )
        self.es_index.add_alias(META_INDEX_NAME)

        for c in METADATA_COLUMNS:
            self._add(c)
            self.es_index.add({"id": get_id(c), "value": c.__dict__()})

    def _db_load(self):
        self.last_load = Date.now()

        try:
            self.es_index = self.es_cluster.get_index(
                index=META_INDEX_NAME, type=META_TYPE_NAME, read_only=False
            )

            result = self.es_index.search(
                {
                    "query": {"match_all": {}},
                    "sort": ["es_index.~s~", "name.~s~", "es_column.~s~"],
                    "size": 10000
                }
            )

            DEBUG and Log.note("{{num}} columns loaded", num=result.hits.total)
            with self.locker:
                for r in result.hits.hits._source:
                    self._add(doc_to_column(r))

        except Exception as e:
            Log.warning(
                "no {{index}} exists, making one", index=META_INDEX_NAME, cause=e
            )
            self._db_create()

    def _db_worker(self, please_stop):
        batch_size = 10000
        while not please_stop:
            try:
                result = self.es_index.search(
                    {
                        "query": {
                            "range": {"last_updated.~n~": {"gt": self.last_load}}
                        },
                        "sort": ["es_index.~s~", "name.~s~", "es_column.~s~"],
                        "from": 0,
                        "size": batch_size,
                    }
                )
                batch_size = 10

                with self.locker:
                    for r in result.hits.hits._source:
                        c = doc_to_column(r)
                        self._add(c)
                        if c.last_updated > self.last_load:
                            self.last_load = c.last_updated

                updates = self.todo.pop_all()
                DEBUG and updates and Log.note(
                    "{{num}} columns to push to db", num=len(updates)
                )
                for action, column in updates:
                    if please_stop:
                        return
                    try:
                        DEBUG and Log.note(
                            "{{action}} db for {{table}}.{{column}}",
                            action=action,
                            table=column.es_index,
                            column=column.es_column,
                        )
                        if action is DELETE_INDEX:
                            self.es_index.delete_record(
                                {"term": {"es_index.~s~": column}}
                            )
                        elif action is UPDATE:
                            self.es_index.add(
                                {"id": get_id(column), "value": column.__dict__()}
                            )
                        elif action is DELETE:
                            self.es_index.delete_id(get_id(column))
                    except Exception as e:
                        e = Except.wrap(e)
                        Log.warning("problem updataing database", cause=e)

            except Exception as e:
                Log.warning("problem updating database", cause=e)

            (Till(seconds=10) | please_stop).wait()

    def __copy__(self):
        output = object.__new__(ColumnList)
        Table.__init__(output, META_INDEX_NAME)
        output.data = {
            t: {c: list(cs) for c, cs in dd.items()} for t, dd in self.data.items()
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
        if canonical == None:
            return column  # ALREADY ADDED
        self.todo.add((UPDATE, canonical))
        return canonical

    def remove_table(self, table_name):
        del self.data[table_name]

    def _add(self, column):
        """
        :param column: ANY COLUMN OBJECT
        :return:  None IF column IS canonical ALREADY (NET-ZERO EFFECT)
        """
        columns_for_table = self.data.setdefault(column.es_index, {})
        existing_columns = columns_for_table.setdefault(column.name, [])

        for canonical in existing_columns:
            if canonical is column:
                return None
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

        for mcl in self.data.get(META_INDEX_NAME).values():
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
                        if is_list(value):
                            multi = max(multi, len(value))
                            try:
                                values |= set(value)
                            except Exception:
                                objects += len(value)
                        elif is_data(value):
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
        return self.data[META_INDEX_NAME]["es_index"].count

    def update(self, command):
        self.dirty = True
        try:
            command = wrap(command)
            DEBUG and Log.note(
                "Update {{timestamp}}: {{command|json}}",
                command=command,
                timestamp=Date(command["set"].last_updated),
            )
            eq = command.where.eq
            if eq.es_index:
                if len(eq) == 1:
                    if unwraplist(command.clear) == ".":
                        with self.locker:
                            del self.data[eq.es_index]
                        self.todo.add((DELETE_INDEX, eq.es_index))
                        return

                    # FASTEST
                    all_columns = self.data.get(eq.es_index, {}).values()
                    with self.locker:
                        columns = [c for cs in all_columns for c in cs]
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
                            if all(
                                c[k] == v for k, v in eq.items()
                            )  # THIS LINE IS VERY SLOW
                        ]
            else:
                columns = list(self)
                columns = jx.filter(columns, command.where)

            with self.locker:
                for col in columns:
                    DEBUG and Log.note(
                        "update column {{table}}.{{column}}",
                        table=col.es_index,
                        column=col.es_column,
                    )
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
                            break
                        else:
                            col[k] = None
                    else:
                        # DID NOT DELETE COLUMNM ("."), CONTINUE TO SET PROPERTIES
                        for k, v in command.set.items():
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
                self._schema = Schema(
                    ".", [c for cs in self.data[META_INDEX_NAME].values() for c in cs]
                )
            snapshot = self._all_columns()

        from jx_python.containers.list_usingPythonList import ListContainer

        query.frum = ListContainer(META_INDEX_NAME, snapshot, self._schema)
        return jx.run(query)

    def groupby(self, keys):
        with self.locker:
            self._update_meta()
            return jx.groupby(self.__iter__(), keys)

    def window(self, window):
        raise NotImplemented()

    @property
    def schema(self):
        if not self._schema:
            with self.locker:
                self._update_meta()
                self._schema = Schema(
                    ".", [c for cs in self.data[META_INDEX_NAME].values() for c in cs]
                )
        return self._schema

    @property
    def namespace(self):
        return self

    def get_table(self, table_name):
        if table_name != META_INDEX_NAME:
            Log.error("this container has only the " + META_INDEX_NAME)
        return self

    def get_columns(self, table_name):
        if table_name != META_INDEX_NAME:
            Log.error("this container has only the " + META_INDEX_NAME)
        return self._all_columns()

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
                    "type": c.jx_type,
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
            schema=jx_base.Schema(META_INDEX_NAME, SIMPLE_METADATA_COLUMNS),
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
        row_type = python_type_to_json_type[d.__class__]

        if row_type != "object":
            # EXPECTING PRIMITIVE VALUE
            full_name = parent
            column = columns[full_name]
            if not column:
                column = Column(
                    name=concat_field(table_name, full_name),
                    es_column=full_name,
                    es_index=".",
                    es_type=d.__class__.__name__,
                    jx_type=None,  # WILL BE SET BELOW
                    last_updated=Date.now(),
                    nested_path=nested_path,
                )
                columns.add(column)
            column.es_type = _merge_python_type(column.es_type, d.__class__)
            column.jx_type = python_type_to_json_type[column.es_type]
        else:
            for name, value in d.items():
                full_name = concat_field(parent, name)
                column = columns[full_name]
                if not column:
                    column = Column(
                        name=concat_field(table_name, full_name),
                        es_column=full_name,
                        es_index=".",
                        es_type=value.__class__.__name__,
                        jx_type=None,  # WILL BE SET BELOW
                        last_updated=Date.now(),
                        nested_path=nested_path,
                    )
                    columns.add(column)
                if is_container(value):  # GET TYPE OF MULTIVALUE
                    v = list(value)
                    if len(v) == 0:
                        this_type = none_type.__name__
                    elif len(v) == 1:
                        this_type = v[0].__class__.__name__
                    else:
                        this_type = reduce(
                            _merge_python_type, (vi.__class__.__name__ for vi in value)
                        )
                else:
                    this_type = value.__class__.__name__
                column.es_type = _merge_python_type(column.es_type, this_type)
                column.jx_type = python_type_to_json_type[column.es_type]

                if this_type in {"object", "dict", "Mapping", "Data"}:
                    _get_schema_from_list(
                        [value], table_name, full_name, nested_path, columns
                    )
                elif this_type in {"list", "FlatList"}:
                    np = listwrap(nested_path)
                    newpath = unwraplist([join_field(split_field(np[0]) + [name])] + np)
                    _get_schema_from_list(
                        value, table_name, full_name, newpath, columns
                    )


def get_id(column):
    """
    :param column:
    :return: Elasticsearch id for column
    """
    return column.es_index + "|" + column.es_column


METADATA_COLUMNS = (
    [
        Column(
            name=c,
            es_index=META_INDEX_NAME,
            es_column=c,
            es_type="keyword",
            jx_type=STRING,
            last_updated=Date.now(),
            nested_path=ROOT_PATH,
        )
        for c in [
            "name",
            "es_type",
            "jx_type",
            "nested_path",
            "es_column",
            "es_index",
            "partitions",
        ]
    ]
    + [
        Column(
            name=c,
            es_index=META_INDEX_NAME,
            es_column=c,
            es_type="integer",
            jx_type=INTEGER,
            last_updated=Date.now(),
            nested_path=ROOT_PATH,
        )
        for c in ["count", "cardinality", "multi"]
    ]
    + [
        Column(
            name="last_updated",
            es_index=META_INDEX_NAME,
            es_column="last_updated",
            es_type="double",
            jx_type=NUMBER,
            last_updated=Date.now(),
            nested_path=ROOT_PATH,
        )
    ]
)


def doc_to_column(doc):
    return Column(**wrap(untyped(doc)))


SIMPLE_METADATA_COLUMNS = (  # FOR PURELY INTERNAL PYTHON LISTS, NOT MAPPING TO ANOTHER DATASTORE
    [
        Column(
            name=c,
            es_index=META_INDEX_NAME,
            es_column=c,
            es_type="string",
            jx_type=STRING,
            last_updated=Date.now(),
            nested_path=ROOT_PATH,
        )
        for c in ["table", "name", "type", "nested_path"]
    ]
    + [
        Column(
            name=c,
            es_index=META_INDEX_NAME,
            es_column=c,
            es_type="long",
            jx_type=INTEGER,
            last_updated=Date.now(),
            nested_path=ROOT_PATH,
        )
        for c in ["count", "cardinality", "multi"]
    ]
    + [
        Column(
            name="last_updated",
            es_index=META_INDEX_NAME,
            es_column="last_updated",
            es_type="time",
            jx_type=NUMBER,
            last_updated=Date.now(),
            nested_path=ROOT_PATH,
        )
    ]
)

_merge_order = {
    none_type: 0,
    NullType: 1,
    bool: 2,
    int: 3,
    long: 3,
    Date: 4,
    float: 5,
    text_type: 6,
    binary_type: 6,
    object: 7,
    dict: 8,
    Mapping: 9,
    Data: 10,
    list: 11,
    FlatList: 12,
}

for k, v in items(_merge_order):
    _merge_order[k.__name__] = v


def _merge_python_type(A, B):
    a = _merge_order[A]
    b = _merge_order[B]

    if a >= b:
        output = A
    else:
        output = B

    if isinstance(output, str):
        return output
    else:
        return output.__name__
