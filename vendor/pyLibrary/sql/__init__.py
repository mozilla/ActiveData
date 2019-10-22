# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

from mo_dots import is_many, is_container
from mo_future import is_text, PY2
from mo_future import text_type
from mo_logs import Log

DEBUG = True


class _Base(object):
    __slots__ = []

    @property
    def sql(self):
        return "".join(self)

    def __add__(self, other):
        if not isinstance(other, _Base):
            if is_text(other) and DEBUG and all(c not in other for c in ('"', "'", "`")):
                return ConcatSQL((self, SQL(other)))
            Log.error("Can only concat other SQL")
        else:
            return ConcatSQL((self, other))

    def __radd__(self, other):
        if not isinstance(other, _Base):
            if is_text(other) and DEBUG and all(c not in other for c in ('"', "'", "`")):
                return ConcatSQL((SQL(other), self))
            Log.error("Can only concat other SQL")
        else:
            return ConcatSQL((other, self))

    def join(self, list_):
        return _Join(self, list_)

    def __data__(self):
        return self.sql

    if PY2:
        def __unicode__(self):
            return "".join(self)
    else:
        def __str__(self):
            return "".join(self)


class SQL(_Base):
    """
    ACTUAL SQL, DO NOT QUOTE THIS STRING
    """
    __slots__ = ["value"]

    def __init__(self, value):
        _Base.__init__(self)
        if DEBUG and isinstance(value, _Base):
            Log.error("Expecting text, not SQL")
        self.value = value

    def __iter__(self):
        yield self.value


class _Join(_Base):
    __slots__ = ["sep", "concat"]

    def __init__(self, sep, concat):
        _Base.__init__(self)
        if not is_container(concat):
            concat = list(concat)
        if DEBUG:
            if not isinstance(sep, _Base):
                Log.error("Expecting SQL, not text")
            if any(not isinstance(s, _Base) for s in concat):
                Log.error("Can only join other SQL")
        self.sep = sep
        self.concat = concat

    def __iter__(self):
        if not self.concat:
            return
        it = self.concat.__iter__()
        vv = it.__next__()
        yield vv
        for v in it:
            for vv in self.sep:
                yield vv
            for vv in v:
                yield vv


class ConcatSQL(_Base):
    """
    ACTUAL SQL, DO NOT QUOTE THIS STRING
    """
    __slots__ = ["concat"]

    def __init__(self, concat):
        _Base.__init__(self)
        if not is_container(concat):
            concat = list(concat)
        if DEBUG and any(not isinstance(s, _Base) for s in concat):
            Log.error("Can only join other SQL")
        self.concat = concat

    def __iter__(self):
        for c in self.concat:
            for cc in c:
                yield cc


SQL_STAR = SQL(" * ")

SQL_AND = SQL(" AND ")
SQL_OR = SQL(" OR ")
SQL_NOT = SQL(" NOT ")
SQL_ON = SQL(" ON ")

SQL_CASE = SQL(" CASE ")
SQL_WHEN = SQL(" WHEN ")
SQL_THEN = SQL(" THEN ")
SQL_ELSE = SQL(" ELSE ")
SQL_END = SQL(" END ")

SQL_COMMA = SQL(", ")
SQL_UNION_ALL = SQL("\nUNION ALL\n")
SQL_UNION = SQL("\nUNION\n")
SQL_LEFT_JOIN = SQL("\nLEFT JOIN\n")
SQL_INNER_JOIN = SQL("\nJOIN\n")
SQL_EMPTY_STRING = SQL("''")
SQL_TRUE = SQL(" 1 ")
SQL_FALSE = SQL(" 0 ")
SQL_ONE = SQL(" 1 ")
SQL_ZERO = SQL(" 0 ")
SQL_NEG_ONE = SQL(" -1 ")
SQL_NULL = SQL(" NULL ")
SQL_IS_NULL = SQL(" IS NULL ")
SQL_IS_NOT_NULL = SQL(" IS NOT NULL ")
SQL_SELECT = SQL("\nSELECT\n")
SQL_CREATE = SQL("\nCREATE TABLE\n")
SQL_INSERT = SQL("\nINSERT INTO\n")
SQL_FROM = SQL("\nFROM\n")
SQL_WHERE = SQL("\nWHERE\n")
SQL_GROUPBY = SQL("\nGROUP BY\n")
SQL_ORDERBY = SQL("\nORDER BY\n")
SQL_VALUES = SQL("\nVALUES\n")
SQL_DESC = SQL(" DESC ")
SQL_ASC = SQL(" ASC ")
SQL_LIMIT = SQL("\nLIMIT\n")
SQL_UPDATE = SQL("\nUPDATE\n")
SQL_SET = SQL("\nSET\n")

SQL_CONCAT = SQL(" || ")
SQL_AS = SQL(" AS ")
SQL_SPACE = SQL(" ")
SQL_OP = SQL("(")
SQL_CP = SQL(")")
SQL_EQ = SQL(" = ")
SQL_DOT = SQL(".")


class DB(object):
    def quote_column(self, column_name, table=None):
        raise NotImplementedError()

    def db_type_to_json_type(self, type):
        raise NotImplementedError()


def sql_list(list_):
    return ConcatSQL((SQL_SPACE, _Join(SQL_COMMA, list_), SQL_SPACE))


def sql_iso(sql):
    return ConcatSQL((SQL_OP, sql, SQL_CP))


def sql_count(sql):
    return "COUNT(" + sql + ")"


def sql_concat_text(list_):
    """
    TEXT CONCATENATION WITH "||"
    """
    return _Join(SQL_CONCAT, [sql_iso(l) for l in list_])


def sql_alias(value, alias):
    return ConcatSQL((value, SQL_AS, alias))


def sql_coalesce(list_):
    return ConcatSQL((SQL("COALESCE("), _Join(SQL_COMMA, list_), SQL_CP))


