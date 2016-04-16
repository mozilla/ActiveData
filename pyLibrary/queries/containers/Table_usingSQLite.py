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

from collections import Mapping
from copy import copy

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import listwrap, coalesce, Dict, wrap, Null, unwraplist, split_field, join_field, literal_field, \
    set_default
from pyLibrary.maths.randoms import Random
from pyLibrary.queries import jx
from pyLibrary.queries.containers import Container
from pyLibrary.queries.domains import SimpleSetDomain
from pyLibrary.queries.expressions import jx_expression, CoalesceOp, Variable
from pyLibrary.queries.meta import Column
from pyLibrary.queries.query import QueryOp
from pyLibrary.sql.sqlite import Sqlite
from pyLibrary.times.dates import Date

UID_PREFIX = "___"


class Table_usingSQLite(Container):
    def __init__(self, name, db=None, uid=UID_PREFIX+"id", exists=False):
        """
        :param name: NAME FOR THIS TABLE
        :param db: THE DB TO USE
        :param uid: THE UNIQUE INDEX FOR THIS TABLE
        :return: HANDLE FOR TABLE IN db
        """
        Container.__init__(self, frum=None)
        if db:
            self.db = db
        else:
            self.db = db = Sqlite()
        self.name = name
        self.uid = listwrap(uid)

        self.columns = {}
        for u in uid:
            if not self.columns.get(u, None):
                cs = self.columns[u] = set()
            if u.startswith(UID_PREFIX):
                cs.add(Column(name=u, table=name, type="integer", es_column=typed_column(u, "integer"), es_index=name))
            else:
                cs.add(Column(name=u, table=name, type="text", es_column=typed_column(u, "text"), es_index=name))

        self.uid_accessor = jx.get(self.uid)
        self.nested_tables = {}  # MAP FROM TABLE NAME TO Table OBJECT
        if exists:
            # LOAD THE COLUMNS
            command = "PRAGMA table_info(" + quote_table(name) + ")"
            details = self.db.query(command)
            self.columns = {}
            for r in details:
                cname = untyped_column(r[1])
                ctype = r[2].lower()
                column = Column(
                    name=cname,
                    table=name,
                    type=ctype,
                    es_column=typed_column(cname, ctype),
                    es_index=name
                )

                cs = self.columns.get(name, Null)
                if not cs:
                    cs = self.columns[name] = set()
                cs.add(column)
        else:
            command = "CREATE TABLE " + quote_table(name) + "(" + \
                      (",".join(_quote_column(c) + " " + c.type for u, cs in self.columns.items() for c in cs)) + \
                      ", PRIMARY KEY (" + \
                      (", ".join(_quote_column(c) for u in self.uid for c in self.columns[u])) + \
                      "))"

            self.db.execute(command)

    def __del__(self):
        self.db.execute("DROP TABLE " + quote_table(self.name))

    def add(self, doc):
        self.insert([doc])

    def get_leaves(self, table_name=None):
        output = []
        for columns_by_type in self.columns.values():
            for c in columns_by_type:
                if c.type in ["nested", "object"]:
                    continue
                c = c.__copy__()
                c.type = "value"  # MULTI-VALUED, SO HIDE THE TYPE IN THIS GENERIC NAME
                output.append(c)
                break
        return output

    def insert(self, docs):
        doc_collection = {}
        for d in docs:
            # ASSIGN A NON-NULL PRIMARY KEY
            if any(v == None for v in self.uid_accessor(d)):
                for u in self.uid:
                    d[u] = coalesce(d[u], unique_name())

            uid = wrap({u: d[u] for u in self.uid})
            self.flatten(d, uid, doc_collection)

        for nested_path, insertion in doc_collection.items():
            active_columns = list(insertion.active_columns)
            vals = [[quote_value(get_document_value(d, c)) for c in active_columns] for d in insertion.rows]

            command = "INSERT INTO " + quote_table(join_field(split_field(self.name)+split_field(nested_path[0]))) + "(" + \
                      ",".join(_quote_column(c) for c in active_columns) + \
                      ")\n" + \
                      " UNION ALL\n".join("SELECT " + ",".join(vv) for vv in vals)

            self.db.execute(command)

    def add_column(self, column):
        """
        ADD COLUMN, IF IT DOES NOT EXIST ALREADY
        """
        if column.name not in self.columns:
            self.columns[column.name] = {column}
        elif column.type not in [c.type for c in self.columns[column.name]]:
            self.columns[column.name].add(column)

        if column.type == "nested":
            nested_table_name = join_field(split_field(self.name) + split_field(column.name))
            # MAKE THE TABLE
            table = Table_usingSQLite(nested_table_name, self.db, self.uid + [UID_PREFIX+"id"+unicode(len(self.uid))], exists=False)
            self.nested_tables[nested_table_name] = table
        else:
            self.db.execute(
                "ALTER TABLE " + quote_table(self.name) + " ADD COLUMN " + _quote_column(column) + " " + column.type
            )


    def __len__(self):
        counter = self.db.query("SELECT COUNT(*) FROM " + quote_table(self.name))[0][0]
        return counter

    def __nonzero__(self):
        counter = self.db.query("SELECT COUNT(*) FROM " + quote_table(self.name))[0][0]
        return bool(counter)

    def __getattr__(self, item):
        return self.__getitem__(item)

    def __getitem__(self, item):
        cs = self.columns.get(item, None)
        if not cs:
            return [Null]

        command = " UNION ALL ".join(
            "SELECT " + _quote_column(c) + " FROM " + quote_table(c.es_index)
            for c in cs
        )

        output = self.db.query(command)
        return [o[0] for o in output]

    def __iter__(self):
        columns = [c for c, cs in self.columns.items() for c in cs if c.type not in ["object", "nested"]]
        command = "SELECT " + \
                  ",\n".join(_quote_column(c) for c in columns) + \
                  " FROM " + quote_table(self.name)
        rows = self.db.query(command)
        for r in rows:
            output = Dict()
            for (k, t), v in zip(columns, r):
                output[k] = v
            yield output

    def delete(self, where):
        filter = where.to_sql()
        self.db.execute("DELETE FROM " + quote_table(self.name) + " WHERE " + filter)

    def vars(self):
        return set(self.columns.keys())

    def map(self, map_):
        return self

    def update(self, command):
        """
        :param command:  EXPECTING dict WITH {"set": s, "clear": c, "where": w} FORMAT
        """
        command = wrap(command)

        # REJECT DEEP UPDATES
        touched_columns = command.set.keys() | set(listwrap(command["clear"]))
        for c in self.get_leaves():
            if c.name in touched_columns and c.nested_path and len(c.name) > len(c.nested_path[0]):
                Log.error("Deep update not supported")

        # ADD NEW COLUMNS
        where = jx_expression(command.where)
        _vars = where.vars()
        _map = {
            v: c.es_column
            for v in _vars
            for c in self.columns.get(v, Null)
            if c.type not in ["nested", "object"]
            }
        where_sql = where.map(_map).to_sql()
        new_columns = set(command.set.keys()) - set(self.columns.keys())
        for new_column_name in new_columns:
            nested_value = command.set[new_column_name]
            ctype = get_type(nested_value)
            column = Column(
                name=new_column_name,
                type=ctype,
                table=self.name,
                es_index=self.name,
                es_column=typed_column(new_column_name, ctype)
            )
            self.add_column(column)

        # UPDATE THE NESTED VALUES
        for nested_column_name, nested_value in command.set.items():
            if get_type(nested_value) == "nested":
                nested_table_name = join_field(split_field(self.name)+split_field(nested_column_name))
                nested_table = self.nested_tables[nested_table_name]
                self_primary_key = ",".join(quote_table(c.es_column) for u in self.uid for c in self.columns[u])
                extra_key_name = UID_PREFIX+"id"+unicode(len(self.uid))
                extra_key = [e for e in nested_table.columns[extra_key_name]][0]

                sql_command = "DELETE FROM " + quote_table(nested_table.name) + \
                              "\nWHERE EXISTS (" + \
                              "\nSELECT 1 " + \
                              "\nFROM " + quote_table(nested_table.name) + " n" + \
                              "\nJOIN (" + \
                              "\nSELECT " + self_primary_key + \
                              "\nFROM " + quote_table(self.name) + \
                              "\nWHERE " + where_sql + \
                              "\n) t ON " + \
                              " AND ".join(
                                  "t." + quote_table(c.es_column) + " = n." + quote_table(c.es_column)
                                  for u in self.uid
                                  for c in self.columns[u]
                              ) + \
                              ")"
                self.db.execute(sql_command)

                # INSERT NEW RECORDS
                if not nested_value:
                    continue

                doc_collection = {}
                for d in listwrap(nested_value):
                    nested_table.flatten(d, Dict(), doc_collection, path=nested_column_name)

                prefix = "INSERT INTO " + quote_table(nested_table.name) + \
                         "(" + \
                         self_primary_key + "," + \
                         _quote_column(extra_key) + "," + \
                         ",".join(
                             quote_table(c.es_column)
                             for c in doc_collection.get(".", Null).active_columns
                         ) + ")"

                # BUILD THE PARENT TABLES
                parent = "\nSELECT " + \
                         self_primary_key + \
                         "\nFROM " + quote_table(self.name) + \
                         "\nWHERE " + jx_expression(command.where).to_sql()

                # BUILD THE RECORDS
                children = " UNION ALL ".join(
                    "\nSELECT " +
                    quote_value(i) + " " +quote_table(extra_key.es_column) + "," +
                    ",".join(
                        quote_value(row[c.name]) + " " + quote_table(c.es_column)
                        for c in doc_collection.get(".", Null).active_columns
                    )
                    for i, row in enumerate(doc_collection.get(".", Null).rows)
                )

                sql_command = prefix + \
                              "\nSELECT " + \
                              ",".join(
                                  "p." + quote_table(c.es_column)
                                  for u in self.uid for c in self.columns[u]
                              ) + "," + \
                              "c." + _quote_column(extra_key) + "," + \
                              ",".join(
                                  "c." + quote_table(c.es_column)
                                  for c in doc_collection.get(".", Null).active_columns
                              ) + \
                              "\nFROM (" + parent + ") p " + \
                              "\nJOIN (" + children + \
                              "\n) c on 1=1"

                self.db.execute(sql_command)

                # THE CHILD COLUMNS COULD HAVE EXPANDED
                # ADD COLUMNS TO SELF
                for n, cs in nested_table.columns.items():
                    for c in cs:
                        column = Column(
                            name=c.name,
                            type=c.type,
                            table=self.name,
                            es_index=c.es_index,
                            es_column=c.es_column,
                            nested_path=[nested_column_name]+listwrap(c.nested_path)
                        )
                        if c.name not in self.columns:
                            self.columns[column.name] = {column}
                        elif c.type not in [c.type for c in self.columns[c.name]]:
                            self.columns[column.name].add(column)

        command = "UPDATE " + quote_table(self.name) + " SET " + \
                  ",\n".join(
                      [
                          _quote_column(c) + "=" + quote_value(get_if_type(v, c.type))
                          for k, v in command.set.items()
                          if get_type(v) != "nested"
                          for c in self.columns[k]
                          if c.type != "nested" and not c.nested_path
                          ] +
                      [
                          _quote_column(c) + "=NULL"
                          for k in listwrap(command["clear"])
                          if k in self.columns
                          for c in self.columns[k]
                          if c.type != "nested" and not c.nested_path
                          ]
                  ) + \
                  " WHERE " + where_sql

        self.db.execute(command)

    def upsert(self, doc, where):
        old_docs = self.filter(where)
        if len(old_docs) == 0:
            self.insert(doc)
        else:
            self.delete(where)
            self.insert(doc)

    def where(self, filter):
        """
        WILL NOT PULL WHOLE OBJECT, JUST TOP-LEVEL PROPERTIES
        :param filter:  jx_expression filter
        :return: list of objects that match
        """
        select = []
        column_names = []
        for cname, cs in self.columns.items():
            cs = [c for c in cs if c.type not in ["nested", "object"] and not c.nested_path]
            if len(cs) == 0:
                continue
            column_names.append(cname)
            if len(cs) == 1:
                select.append(quote_table(c.es_column) + " " + quote_table(c.name))
            else:
                select.append(
                    "coalesce(" +
                    ",".join(quote_table(c.es_column) for c in cs) +
                    ") " + quote_table(c.name)
                )

        result = self.db.query(
            " SELECT " + "\n,".join(select) +
            " FROM " + quote_table(self.name) +
            " WHERE " + jx_expression(filter).to_sql()
        )
        return wrap([{c: v for c, v in zip(column_names, r)} for r in result.data])

    def query(self, query):
        """
        :param query:  JSON Query Expression, SET `format="container"` TO MAKE NEW TABLE OF RESULT
        :return:
        """
        query["from"] = self
        query = QueryOp.wrap(query)

        # TYPE CONFLICTS MUST NOW BE RESOLVED DURING
        # TYPE-SPECIFIC QUERY NORMALIZATION
        vars_ = query.vars(exclude_select=True)
        type_map = {
            v: c.es_column
            for v in vars_
            if v in self.columns and len([c for c in self.columns[v] if c.type != "nested"]) == 1
            for c in self.columns[v]
            if c.type != "nested"
            }

        sql_query = query.map(type_map)

        new_table = "temp_"+unique_name()

        if query.format == "container":
            create_table = "CREATE TABLE " + quote_table(new_table) + " AS "
        else:
            create_table = ""

        if sql_query.edges:
            command = create_table + self._edges_op(sql_query)
        elif sql_query.groupby:
            command = create_table + self._groupby_op(sql_query)
        else:
            command = create_table + self._set_op(sql_query)

        if sql_query.sort:
            command += "\nORDER BY " + ",\n".join(
                s.value.to_sql() + (" DESC" if s.sort == -1 else "")
                for s in sql_query.sort
            )

        result = self.db.query(command)

        column_names = query.column_names
        if query.format == "container":
            output = Table_usingSQLite(new_table, db=self.db, uid=self.uid, exists=True)
        elif query.format == "cube" or query.edges:
            if len(query.edges) > 1:
                Log.error("Only support one dimension right now")

            if not result.data:
                return Dict(
                    data={}
                )

            columns = zip(*result.data)

            edges = []
            ci = []
            for i, e in enumerate(query.edges):
                if e.domain.type != "default":
                    Log.error("Can only handle default domains")
                ci.append(i - len(query.edges))
                parts = columns[ci[i]]
                allowNulls=False
                if parts[0]==None:
                    allowNulls=True
                    # ONLY ONE EDGE, SO WE CAN DO THIS TO PUT NULL LAST
                    for ii, c in enumerate(copy(columns)):
                        columns[ii] = list(c[1:]) + [c[0]]
                    parts = parts[1:]

                edges.append(Dict(
                    name=e.name,
                    allowNulls=allowNulls,
                    domain=SimpleSetDomain(partitions=parts)
                ))

            data = {s.name: columns[i] for i, s in enumerate(sql_query.select)}

            return Dict(
                edges=edges,
                data=data
            )
        elif query.format == "list" or (not query.edges and not query.groupby):
            output = Dict(
                meta={"format": "list"},
                header=column_names,
                data=[{c: v for c, v in zip(column_names, r)} for r in result.data]
            )
        else:
            Log.error("unknown format {{format}}", format=query.format)

        return output

    def _edges_op(self, query):
        selects = []
        for s in listwrap(query.select):
            if s.value=="." and s.aggregate=="count":
                selects.append("COUNT(1) AS " + quote_table(s.name))
            else:
                selects.append(sql_aggs[s.aggregate]+"("+jx_expression(s.value).to_sql() + ") AS " + quote_table(s.name))

        for w in query.window:
            selects.append(self._window_op(self, query, w))

        agg_prefix = " FROM "
        agg_suffix = "\n"

        agg = ""
        ons = []
        groupby = ""
        groupby_prefix = "\nGROUP BY "

        for i, e in enumerate(query.edges):
            edge_alias = "e" + unicode(i)
            edge_value = e.value.to_sql()
            value = edge_value
            for v in e.value.vars():
                value = value.replace(quote_table(v), "a."+quote_table(v))

            edge_name = quote_table(e.name)
            selects.append(edge_alias + "." + edge_name + " AS " + edge_name)
            agg += \
                agg_prefix + "(" + \
                "SELECT DISTINCT " + edge_value + " AS " + edge_name + " FROM " + quote_table(self.name) + \
                ") " + edge_alias + \
                agg_suffix
            agg_prefix = " LEFT JOIN "
            agg_suffix = " ON 1=1\n"
            ons.append(edge_alias + "." + edge_name + " = "+ value)
            groupby += groupby_prefix + edge_alias + "." + edge_name
            groupby_prefix = ",\n"

        agg += agg_prefix + quote_table(self.name) + " a ON "+" AND ".join(ons)

        where = "\nWHERE " + query.where.to_sql()

        return "SELECT " + (",\n".join(selects)) + agg + where+groupby

    def _groupby_op(self, query):
        selects = []
        for s in listwrap(query.select):
            if s.value=="." and s.aggregate=="count":
                selects.append("COUNT(1) AS " + quote_table(s.name))
            else:
                selects.append(sql_aggs[s.aggregate]+"("+jx_expression(s.value).to_sql() + ") AS " + quote_table(s.name))

        for w in query.window:
            selects.append(self._window_op(self, query, w))

        agg = " FROM " + quote_table(self.name) + " a\n"
        groupby = ""
        groupby_prefix = " GROUP BY "

        for i, e in enumerate(query.edges):
            value = e.to_sql()
            groupby += groupby_prefix + value
            groupby_prefix = ",\n"

        where = "\nWHERE " + query.where.to_sql()

        return "SELECT " + (",\n".join(selects)) + agg + where+groupby

    def _set_op(self, query):
        if listwrap(query.select)[0].value == ".":
            selects = ["*"]
        else:
            selects = [s.value.to_sql() + " AS " + quote_table(s.name) for s in listwrap(query.select)]

        for w in query.window:
            selects.append(self._window_op(self, query, w))

        agg = " FROM " + quote_table(self.name) + " a"
        where = "\nWHERE " + query.where.to_sql()

        return "SELECT " + (",\n".join(selects)) + agg + where

    def _window_op(self, query, window):
        # http://www2.sqlite.org/cvstrac/wiki?p=UnsupportedSqlAnalyticalFunctions
        if window.value == "rownum":
            return "ROW_NUMBER()-1 OVER (" + \
                   " PARTITION BY " + (", ".join(window.edges.values)) + \
                   " ORDER BY " + (", ".join(window.edges.sort)) + \
                   ") AS " + quote_table(window.name)

        range_min = unicode(coalesce(window.range.min, "UNBOUNDED"))
        range_max = unicode(coalesce(window.range.max, "UNBOUNDED"))

        return sql_aggs[window.aggregate] + "(" + window.value.to_sql() + ") OVER (" + \
               " PARTITION BY " + (", ".join(window.edges.values)) + \
               " ORDER BY " + (", ".join(window.edges.sort)) + \
               " ROWS BETWEEN " + range_min + " PRECEDING AND " + range_max + " FOLLOWING " + \
               ") AS " + quote_table(window.name)

    def _normalize_select(self, select):
        output = []
        if select.value == ".":
            for cname, cs in self.columns.items():
                num_types = 0
                for c in cs:
                    if c.type in ["nested", "object"]:
                        continue
                    if not num_types:
                        new_select = select.copy()
                        new_select.name = cname
                        new_select.value = Variable(c.es_column)
                        output.append(new_select)
                    elif num_types == 1:
                        new_select.value = CoalesceOp("coalesce", [Variable(c.es_column), new_select.value])
                    else:
                        new_select.value.terms.append(Variable(c.es_column))
                    num_types += 1
        elif select.value.endswith(".*"):
            Log.error("not done")
        else:
            Log.error("not done")
        return output

    def flatten(self, doc, uid, doc_collection, path=None):
        """
        :param doc:  THE JSON DOCUMENT
        :param doc_collection: MAP NESTED PATH TO INSERTION PARAMETERS
        """

        def _flatten(d, uid, path, nested_path):
            insertion = doc_collection["." if not nested_path else nested_path[0]]
            row = uid.copy()
            insertion.rows.append(row)
            if isinstance(d, Mapping):
                for k, v in d.items():
                    cname = join_field(split_field(path)+[k])
                    ctype = get_type(v)
                    if ctype is None:
                        continue

                    c = unwraplist([c for c in self.columns.get(cname, Null) if c.type == ctype])
                    if not c:
                        c = Column(
                            name=cname,
                            table=self.name,
                            type=ctype,
                            es_column=typed_column(cname, ctype),
                            es_index=self.name,
                            nested_path=nested_path
                        )
                        self.add_column(c)
                    insertion.active_columns.add(c)

                    if ctype == "nested":
                        row[cname] = "."
                        deeper = [cname] + listwrap(nested_path)
                        insertion = doc_collection.get(cname, None)
                        if not insertion:
                            doc_collection[cname] = Dict(
                                active_columns=set(),
                                rows=[]
                            )
                        for i, r in enumerate(v):
                            child_uid = set_default({UID_PREFIX+"id"+unicode(len(uid)): i}, uid)
                            _flatten(r, child_uid, cname, deeper)
                    elif ctype == "object":
                        row[cname] = "."
                        _flatten(v, cname, nested_path)
                    elif c.type:
                        row[cname] = v
            else:
                k="."
                v=d
                cname = join_field(split_field(path)+[k])
                ctype = get_type(v)
                if ctype is None:
                    return

                c = unwraplist([c for c in self.columns.get(cname, Null) if c.type == ctype])
                if not c:
                    c = Column(
                        name=cname,
                        table=self.name,
                        type=ctype,
                        es_column=typed_column(cname, ctype),
                        es_index=self.name,
                        nested_path=nested_path
                    )
                    self.add_column(c)
                insertion.active_columns.add(c)

                if ctype == "nested":
                    row[cname] = "."
                    deeper = [cname] + listwrap(nested_path)
                    insertion = doc_collection.get(cname, None)
                    if not insertion:
                        doc_collection[cname] = Dict(
                            active_columns=set(),
                            rows=[]
                        )
                    for i, r in enumerate(v):
                        child_uid = set_default({UID_PREFIX+"id"+unicode(len(uid)): i}, uid)
                        _flatten(r, child_uid, cname, deeper)
                elif ctype == "object":
                    row[cname] = "."
                    _flatten(v, cname, nested_path)
                elif c.type:
                    row[cname] = v

        insertion = doc_collection.get(".", None)
        if not insertion:
            doc_collection["."] = Dict(
                active_columns=set(),
                rows = []
            )

        _flatten(doc, uid, coalesce(path, "."), None)


def quote_table(column):
    return convert.string2quote(column)


def _quote_column(column):
    return convert.string2quote(column.es_column)


def quote_value(value):
    if isinstance(value, (Mapping, list)):
        return "."
    elif isinstance(value, basestring):
        return "'" + value.replace("'", "''") + "'"
    elif value == None:
        return "NULL"
    elif value is True:
        return "1"
    elif value is False:
        return "0"
    else:
        return unicode(value)


def unique_name():
    return Random.string(20)


def column_key(k, v):

    if v == None:
        return None
    elif isinstance(v, basestring):
        return k, "text"
    elif isinstance(v, list):
        return k, None
    elif isinstance(v, Mapping):
        return k, "object"
    elif isinstance(v, Date):
        return k, "real"
    else:
        return k, "real"


def get_type(v):
    if v == None:
        return None
    elif isinstance(v, basestring):
        return "text"
    elif isinstance(v, Mapping):
        return "object"
    elif isinstance(v, (int, float, Date)):
        return "real"
    elif isinstance(v, list):
        return "nested"
    return None


def get_document_value(document, column):
    """
    RETURN DOCUMENT VALUE IF MATCHES THE column (name, type)

    :param document: THE DOCUMENT
    :param column: A (name, type) PAIR
    :return: VALUE, IF IT IS THE SAME NAME AND TYPE
    """
    v = document.get(split_field(column.name)[0], None)
    return get_if_type(v, column.type)


def get_if_type(value, type):
    if is_type(value, type):
        if type == "object":
            return "."
        if isinstance(value, Date):
            return value.unix
        return value
    return None


def is_type(value, type):
    if value == None:
        return False
    elif isinstance(value, basestring) and type == "text":
        return value
    elif isinstance(value, list):
        return False
    elif isinstance(value, Mapping) and type == "object":
        return True
    elif isinstance(value, (int, float, Date)) and type == "real":
        return True
    return False


def typed_column(name, type):
    return join_field(split_field(name)+["$" + type])


def untyped_column(column_name):
    if "$" in column_name:
        return join_field(split_field(column_name)[:-1])
    else:
        return column_name
    # return column_name.split(".$")[0]


sql_aggs = {
    "min": "MIN",
    "max": "MAX",
    "sum": "SUM",
    "avg": "AVG",
    "average": "AVG",
    "last": "LAST_VALUE",
    "first": "FIRST_VALUE"
}

sql_types = {
    "string": "text",
    "long": "integer",
    "double": "real",
    "boolean": "integer"
}
