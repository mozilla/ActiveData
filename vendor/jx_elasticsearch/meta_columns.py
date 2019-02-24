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

import jx_base
from jx_base import Column, Table
from jx_base.meta_columns import METADATA_COLUMNS, SIMPLE_METADATA_COLUMNS
from jx_base.schema import Schema
from jx_python import jx
from mo_dots import Data, Null, is_data, is_list, unwraplist, wrap
from mo_json import STRUCT
from mo_json.typed_encoder import unnest_path, untype_path, untyped
from mo_logs import Log
from mo_math import MAX
from mo_threads import Lock, MAIN_THREAD, Queue, Thread, Till
from mo_times.dates import Date

DEBUG = False
singlton = None
META_INDEX_NAME = "meta.columns"
META_TYPE_NAME = "column"
COLUMN_LOAD_PERIOD = 10
COLUMN_EXTRACT_PERIOD = 2 * 60
ID = {"field": ["es_index", "es_column"], "version": "last_updated"}


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
            "update columns to es"
        )  # HOLD (action, column) PAIR, WHERE action in ['insert', 'update']
        self._db_load()
        Thread.run(
            "update " + META_INDEX_NAME, self._synch_with_es, parent_thread=MAIN_THREAD
        )

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
            id=ID, index=META_INDEX_NAME, schema=schema
        )
        self.es_index.add_alias(META_INDEX_NAME)

        for c in METADATA_COLUMNS:
            self._add(c)
            self.es_index.add({"value": c.__dict__()})

    def _db_load(self):
        self.last_load = Date.now()

        try:
            self.es_index = self.es_cluster.get_index(
                id=ID, index=META_INDEX_NAME, type=META_TYPE_NAME, read_only=False
            )

            result = self.es_index.search(
                {
                    "query": {
                        "bool": {
                            "should": [
                                {
                                    "bool": {
                                        "must_not": {
                                            "exists": {"field": "cardinality.~n~"}
                                        }
                                    }
                                },
                                {  # ASSUME UNUSED COLUMNS DO NOT EXIST
                                    "range": {"cardinality.~n~": {"gt": 0}}
                                },
                            ]
                        }
                    },
                    "sort": ["es_index.~s~", "name.~s~", "es_column.~s~"],
                    "size": 10000,
                }
            )

            Log.note("{{num}} columns loaded", num=result.hits.total)
            with self.locker:
                for r in result.hits.hits._source:
                    self._add(doc_to_column(r))

        except Exception as e:
            Log.warning(
                "no {{index}} exists, making one", index=META_INDEX_NAME, cause=e
            )
            self._db_create()

    def _synch_with_es(self, please_stop):
        try:
            last_extract = Date.now()
            while not please_stop:
                now = Date.now()
                try:
                    if (now - last_extract).seconds > COLUMN_EXTRACT_PERIOD:
                        result = self.es_index.search(
                            {
                                "query": {
                                    "range": {
                                        "last_updated.~n~": {"gt": self.last_load}
                                    }
                                },
                                "sort": ["es_index.~s~", "name.~s~", "es_column.~s~"],
                                "from": 0,
                                "size": 10000,
                            }
                        )
                        last_extract = now

                        with self.locker:
                            for r in result.hits.hits._source:
                                c = doc_to_column(r)
                                self._add(c)
                                self.last_load = MAX((self.last_load, c.last_updated))

                    while not please_stop:
                        updates = self.todo.pop_all()
                        if not updates:
                            break

                        DEBUG and updates and Log.note(
                            "{{num}} columns to push to db", num=len(updates)
                        )
                        self.es_index.extend(
                            {"value": column.__dict__()} for column in updates
                        )
                except Exception as e:
                    Log.warning("problem updating database", cause=e)

                (Till(seconds=COLUMN_LOAD_PERIOD) | please_stop).wait()
        finally:
            Log.note("done")

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
        self.todo.add(canonical)
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
                        d = self.data
                        i = eq.es_index
                        with self.locker:
                            cols = d[i]
                            del d[i]

                        for c in cols:
                            mark_as_deleted(c)
                            self.todo.add(c)
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
                            mark_as_deleted(col)
                            self.todo.add(col)
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
                        self.todo.add(col)

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


def doc_to_column(doc):
    return Column(**wrap(untyped(doc)))


def mark_as_deleted(col):
    col.count = 0
    col.cardinality = 0
    col.multi = 0
    col.partitions = None
    col.last_updated = Date.now()
