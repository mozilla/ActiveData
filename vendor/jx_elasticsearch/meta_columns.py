# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

import jx_base
from jx_base import Column, Table
from jx_base.meta_columns import META_COLUMNS_NAME, META_COLUMNS_TYPE_NAME, SIMPLE_METADATA_COLUMNS, META_COLUMNS_DESC
from jx_base.schema import Schema
from jx_python import jx
from mo_dots import Data, Null, is_data, is_list, unwraplist, to_data, listwrap, split_field
from mo_dots.lists import last
from mo_json import INTERNAL, NESTED, OBJECT, EXISTS, STRUCT
from mo_json.typed_encoder import unnest_path, untype_path, untyped, NESTED_TYPE, get_nested_path, EXISTS_TYPE
from mo_logs import Log
from mo_math import MAX
from mo_threads import Lock, MAIN_THREAD, Queue, Thread, Till, THREAD_STOP
from mo_times import YEAR, Timer
from mo_times.dates import Date

DEBUG = False
singlton = None
REPLICAS = 5
COLUMN_LOAD_PERIOD = 10
COLUMN_EXTRACT_PERIOD = 2 * 60
ID = {"field": ["es_index", "es_column"], "version": "last_updated"}


class ColumnList(Table, jx_base.Container):
    """
    CENTRAL CONTAINER FOR ALL COLUMNS
    SYNCHRONIZED WITH ELASTICSEARCH
    OPTIMIZED FOR THE PARTICULAR ACCESS PATTERNS USED
    """

    def __init__(self, es_cluster):
        Table.__init__(self, META_COLUMNS_NAME)
        self.data = {}  # MAP FROM ES_INDEX TO (abs_column_name to COLUMNS)
        self.locker = Lock()
        self._schema = None
        self.dirty = False
        self.es_cluster = es_cluster
        self.es_index = None
        self.last_load = Null
        self.for_es_update = Queue(
            "update columns to es"
        )  # HOLD (action, column) PAIR, WHERE action in ['insert', 'update']
        self._db_load()
        self.delete_queue = Queue("delete columns from es")  # CONTAINS (es_index, after) PAIRS
        Thread.run(
            "update " + META_COLUMNS_NAME, self._update_from_es, parent_thread=MAIN_THREAD
        ).release()
        Thread.run(
            "delete columns", self._delete_columns, parent_thread=MAIN_THREAD
        ).release()

    def _query(self, query):
        result = Data()
        curr = self.es_cluster.execute(query)
        result.meta.format = "table"
        result.header = [d[0] for d in curr.description] if curr.description else None
        result.data = curr.fetchall()
        return result

    def _db_create(self):
        schema = {
            "settings": {"index.number_of_shards": 1, "index.number_of_replicas": REPLICAS},
            "mappings": {META_COLUMNS_TYPE_NAME: {}},
        }

        self.es_index = self.es_cluster.create_index(
            id=ID, index=META_COLUMNS_NAME, schema=schema
        )
        self.es_index.add_alias(META_COLUMNS_NAME)

        for c in META_COLUMNS_DESC.columns:
            self._add(c)
            self.es_index.add({"value": c.__dict__()})

    def _db_load(self):
        self.last_load = Date.now()

        try:
            self.es_index = self.es_cluster.get_index(
                id=ID, index=META_COLUMNS_NAME, type=META_COLUMNS_TYPE_NAME, read_only=False
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
                                    "range": {"cardinality.~n~": {"gte": 0}}
                                },
                            ]
                        }
                    },
                    "sort": ["es_index.~s~", "name.~s~", "es_column.~s~"],
                    "size": 10000,
                }
            )

            with Timer("adding columns to structure"):
                with self.locker:
                    for r in result.hits.hits._source:
                        col = doc_to_column(r)
                        if col:
                            self._add(col)

            Log.note("{{num}} columns loaded", num=result.hits.total)
            if not self.data.get(META_COLUMNS_NAME):
                Log.error("metadata missing from index!")

        except Exception as e:
            metadata = self.es_cluster.get_metadata(after=Date.now())
            if any(index.startswith(META_COLUMNS_NAME) for index in metadata.indices.keys()):
                Log.error("metadata already exists!", cause=e)

            Log.warning("no {{index}} exists, making one", index=META_COLUMNS_NAME, cause=e)
            self._db_create()

    def delete_from_es(self, es_index, after):
        """
        DELETE COLUMNS STORED IN THE ES INDEX
        :param es_index:
        :param after: ONLY DELETE RECORDS BEFORE THIS TIME
        :return:
        """
        self.delete_queue.add((es_index, after))

    def _delete_columns(self, please_stop):
        while not please_stop:
            result = self.delete_queue.pop(till=please_stop)
            if result == THREAD_STOP:
                break
            more_result = self.delete_queue.pop_all()
            results = [result] + more_result
            try:
                delete_result = self.es_index.delete_record({"bool": {"should": [
                    {"bool": {"must": [
                        {"term": {"es_index.~s~": es_index}},
                        {"range": {"timestamp.~n~": {"lt": after.unix}}}
                    ]}}
                    for es_index, after in results
                ]}})
                Log.note("Num deleted = {{delete_result}}", delete_result=delete_result.deleted)
            except Exception as cause:
                Log.error("Problem with delete of table", cause=cause)
            Till(seconds=1).wait()

    def _update_from_es(self, please_stop):
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
                                        "last_updated.~n~": {"gte": self.last_load}
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
                                if c:
                                    self._add(c)
                                    self.last_load = MAX((self.last_load, c.last_updated))

                    while not please_stop:
                        updates = self.for_es_update.pop_all()
                        if not updates:
                            break

                        DEBUG and updates and Log.note(
                            "{{num}} columns to push to db", num=len(updates)
                        )
                        self.es_index.extend([
                            {"value": column.__dict__()} for column in updates
                        ])
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
                return [
                    c
                    for cs in self.data.get(es_index, {}).values()
                    for c in cs
                ]
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
        self.for_es_update.add(canonical)
        return canonical

    def remove(self, column, after):
        if column.last_updated > after:
            return
        mark_as_deleted(column, after)
        with self.locker:
            canonical = self._add(column)
        if canonical:
            Log.error("Expecting canonical column to be removed")
        DEBUG and Log.note("delete {{col|quote}}, at {{timestamp}}", col=column.es_column, timestamp=column.last_updated)
        self.for_es_update.add(column)

    def remove_table(self, table_name):
        del self.data[table_name]

    def _add(self, column):
        """
        :param column: ANY COLUMN OBJECT
        :return:  None IF column IS canonical ALREADY (NET-ZERO EFFECT)
        """
        if not isinstance(column, Column):
            Log.warning("expecting a column not {{column|json}}", column=column)
            return
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
                        if new_value == old_value:
                            pass  # NO NEED TO UPDATE WHEN NO CHANGE MADE (COMMON CASE)
                        else:
                            canonical[key] = new_value
                return canonical
        existing_columns.append(column)
        return column

    def _update_meta(self):
        if not self.dirty:
            return

        now = Date.now()
        for mc in META_COLUMNS_DESC.columns:
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
            mc.last_updated = now

        META_COLUMNS_DESC.last_updated = now
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
        return self.data[META_COLUMNS_NAME]["es_index"].count

    def clear(self, es_index, es_column=None, after=None):
        if es_column:
            for c in self.data.get(es_index, {}).get(es_column, []):
                self.remove(c, after=after)
            return

        data = self.data
        with self.locker:
            cols = data.get(es_index)
            if not cols:
                return
            del data[es_index]

        for c in cols.values():
            for cc in c:
                mark_as_deleted(cc, after=after)

    def update(self, command):
        self.dirty = True
        try:
            command = to_data(command)
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
                            self.remove(c)
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
                            mark_as_deleted(col, Date.now())
                            self.for_es_update.add(col)
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
                        self.for_es_update.add(col)

        except Exception as e:
            Log.error("should not happen", cause=e)

    def query(self, query):
        # NOT EXPECTED TO BE RUN
        Log.error("not")
        with self.locker:
            self._update_meta()
            if not self._schema:
                self._schema = Schema(
                    ".", [c for cs in self.data[META_COLUMNS_NAME].values() for c in cs]
                )
            snapshot = self._all_columns()

        from jx_python.containers.list_usingPythonList import ListContainer

        query.frum = ListContainer(META_COLUMNS_NAME, snapshot, self._schema)
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
                    ".", [c for cs in self.data[META_COLUMNS_NAME].values() for c in cs]
                )
        return self._schema

    @property
    def namespace(self):
        return self

    def get_table(self, table_name):
        if table_name != META_COLUMNS_NAME:
            Log.error("this container has only the " + META_COLUMNS_NAME)
        return self

    def get_columns(self, table_name):
        if table_name != META_COLUMNS_NAME:
            Log.error("this container has only the " + META_COLUMNS_NAME)
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
                if c.jx_type not in INTERNAL  # and c.es_column != "_id"
            ]

        from jx_python.containers.list_usingPythonList import ListContainer

        return ListContainer(
            self.name,
            data=output,
            schema=jx_base.Schema(META_COLUMNS_NAME, SIMPLE_METADATA_COLUMNS),
        )


def doc_to_column(doc):
    now = Date.now()
    try:
        doc = to_data(untyped(doc))

        # I HAVE MANAGED TO MAKE MANY MISTAKES WRITING COLUMNS TO ES. HERE ARE THE FIXES

        # FIX
        if not doc.last_updated:
            doc.last_updated = Date.now()-YEAR

        # FIX
        if doc.es_type == None:
            if doc.jx_type == OBJECT:
                doc.es_type = "object"
            else:
                Log.warning("{{doc}} has no es_type", doc=doc)

        # FIX
        doc.multi = 1001 if doc.es_type == "nested" else doc.multi

        # FIX
        if doc.es_column.endswith("."+NESTED_TYPE):
            if doc.jx_type == OBJECT:
                doc.jx_type = NESTED
                doc.last_updated = now
            if doc.es_type == "nested":
                doc.es_type = "nested"
                doc.last_updated = now

        # FIX
        doc.nested_path = tuple(listwrap(doc.nested_path))
        if last(split_field(doc.es_column)) == NESTED_TYPE and doc.es_type != "nested":
            doc.es_type = "nested"
            doc.jx_type = NESTED
            doc.multi = 1001
            doc.last_updated = now

        # FIX
        expected_nested_path = get_nested_path(doc.es_column)
        if len(doc.nested_path) > 1 and doc.nested_path[-2] == '.':
            doc.nested_path = doc.nested_path[:-1]
            doc.last_updated = now

        # FIX
        if untype_path(doc.es_column) == doc.es_column:
            if doc.nested_path != (".",):
                if doc.es_index in {"repo"}:
                    pass
                else:
                    Log.note("not expected")
                    doc.nested_path = expected_nested_path
                    doc.last_updated = now
        else:
            if doc.nested_path != expected_nested_path:
                doc.nested_path = expected_nested_path
                doc.last_updated = now

        # FIX
        if last(split_field(doc.es_column)) == EXISTS_TYPE:
            if doc.jx_type != EXISTS:
                doc.jx_type = EXISTS
                doc.last_updated = now

            if doc.cardinality == None:
                doc.cardinality = 1
                doc.last_updated = now

        # FIX
        if doc.jx_type in STRUCT:
            if doc.cardinality not in [0, 1]:
                doc.cardinality = 1  # DO NOT KNOW IF EXISTS OR NOT
                doc.last_updated = now

        return Column(**doc)
    except Exception as e:
        try:
            mark_as_deleted(Column(**doc), now)
        except Exception:
            pass
        return None


def mark_as_deleted(col, after):
    if col.last_updated > after:
        return
    col.count = 0
    col.cardinality = 0
    col.multi = 1001 if col.es_type == "nested" else 0,
    col.partitions = None
    col.last_updated = after
