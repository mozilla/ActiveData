# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

import types
from copy import deepcopy

from mo_future import generator_types, first, is_text
from mo_imports import expect, delay_import

from mo_dots.utils import CLASS

Log = delay_import("mo_logs.Log")
datawrap, coalesce, list_to_data, to_data, from_data, Null = expect("datawrap", "coalesce", "list_to_data", "to_data", "from_data", "Null")


LIST = str("list")
_get = object.__getattribute__
_set = object.__setattr__
_emit_slice_warning = True


class FlatList(object):
    """
    ENCAPSULATES HANDING OF Nulls BY wrapING ALL MEMBERS AS NEEDED
    ENCAPSULATES FLAT SLICES ([::]) FOR USE IN WINDOW FUNCTIONS
    https://github.com/klahnakoski/mo-dots/tree/dev/docs#flatlist-is-flat
    """

    def __init__(self, vals=None):
        """ USE THE vals, NOT A COPY """
        # list.__init__(self)
        if vals == None:
            _set(self, LIST, [])
        elif vals.__class__ is FlatList:
            _set(self, LIST, vals.list)
        else:
            _set(self, LIST, vals)

    def __getitem__(self, index):
        if _get(index, CLASS) is slice:
            # IMPLEMENT FLAT SLICES (for i not in range(0, len(self)): assert self[i]==None)
            if index.step is not None:
                Log.error(
                    "slice step must be None, do not know how to deal with values"
                )
            length = len(_get(self, LIST))

            i = index.start
            if i is None:
                i = 0
            else:
                i = min(max(i, 0), length)
            j = index.stop
            if j is None:
                j = length
            else:
                j = max(min(j, length), 0)
            return FlatList(_get(self, LIST)[i:j])

        if not isinstance(index, int) or index < 0 or len(_get(self, LIST)) <= index:
            return Null
        return to_data(_get(self, LIST)[index])

    def __setitem__(self, key, value):
        _list = _get(self, LIST)
        if is_text(key):
            for v in _list:
                to_data(v)[key] = value
            return
        elif isinstance(key, int):
            if key <= len(_list):
                for key in range(len(_list), key):
                    _list.append(None)
            _list[key] = from_data(value)
        else:
            Log.error("can not set index of type {{type}}", type=key.__class__.__name__)

    def __setattr__(self, key, value):
        _list = _get(self, LIST)
        for v in _list:
            to_data(v)[key] = value
        return

    def __getattr__(self, key):
        if key in ["__json__", "__call__"]:
            raise AttributeError()
        return self.get(key)

    def get(self, key):
        """
        simple `select`
        """
        output = []
        for v in _get(self, LIST):
            element = coalesce(datawrap(v), Null).get(key)
            if element.__class__ == FlatList:
                output.extend(from_data(element))
            else:
                output.append(from_data(element))
        return list_to_data(output)

    def select(self, key):
        Log.error("Not supported.  Use `get()`")

    def filter(self, _filter):
        return FlatList(
            vals=[from_data(u) for u in (to_data(v) for v in _get(self, LIST)) if _filter(u)]
        )

    def __delslice__(self, i, j):
        Log.error(
            "Can not perform del on slice: modulo arithmetic was performed on the parameters.  You can try using clear()"
        )

    def __clear__(self):
        _set(self, LIST, [])

    def __iter__(self):
        temp = [to_data(v) for v in _get(self, LIST)]
        return iter(temp)

    def __contains__(self, item):
        return list.__contains__(_get(self, LIST), item)

    def append(self, val):
        _get(self, LIST).append(from_data(val))
        return self

    def __str__(self):
        return _get(self, LIST).__str__()

    def __len__(self):
        return _get(self, LIST).__len__()

    def __getslice__(self, i, j):
        global _emit_slice_warning

        if _emit_slice_warning:
            _emit_slice_warning = False
            Log.warning(
                "slicing is broken in Python 2.7: a[i:j] == a[i+len(a), j] sometimes. Use [start:stop:step] (see "
                "https://github.com/klahnakoski/mo-dots/tree/dev/docs#the-slice-operator-in-python27-is-inconsistent"
                ")"
            )
        return self[i:j:]

    def __list__(self):
        return self.list

    def copy(self):
        return FlatList(list(_get(self, LIST)))

    def __copy__(self):
        return FlatList(list(_get(self, LIST)))

    def __deepcopy__(self, memo):
        d = _get(self, LIST)
        return to_data(deepcopy(d, memo))

    def remove(self, x):
        _get(self, LIST).remove(x)
        return self

    def extend(self, values):
        lst = _get(self, LIST)
        for v in values:
            lst.append(from_data(v))
        return self

    def pop(self, index=None):
        if index is None:
            return to_data(_get(self, LIST).pop())
        else:
            return to_data(_get(self, LIST).pop(index))

    def __eq__(self, other):
        lst = _get(self, LIST)
        if other == None:
            return len(lst) == 0

        try:
            if len(lst) != len(other):
                return False
            return all([s == o for s, o in zip(lst, other)])
        except Exception:
            return False

    def __add__(self, value):
        if value == None:
            return self
        output = list(_get(self, LIST))
        output.extend(value)
        return FlatList(vals=output)

    def __or__(self, value):
        output = list(_get(self, LIST))
        output.append(value)
        return FlatList(vals=output)

    def __radd__(self, other):
        output = list(other)
        output.extend(_get(self, LIST))
        return FlatList(vals=output)

    def __iadd__(self, other):
        if is_list(other):
            self.extend(other)
        else:
            self.append(other)
        return self

    def right(self, num=None):
        """
        WITH SLICES BEING FLAT, WE NEED A SIMPLE WAY TO SLICE FROM THE RIGHT [-num:]
        """
        if num == None:
            return self
        if num <= 0:
            return Null

        return FlatList(_get(self, LIST)[-num:])

    def limit(self, num=None):
        """
        NOT REQUIRED, BUT EXISTS AS OPPOSITE OF right()
        """
        if num == None:
            return self
        if num <= 0:
            return Null

        return FlatList(_get(self, LIST)[:num])

    def not_right(self, num):
        """
        WITH SLICES BEING FLAT, WE NEED A SIMPLE WAY TO SLICE FROM THE LEFT [:-num:]
        """
        if num == None:
            return self
        if num <= 0:
            return Null

        return FlatList(_get(self, LIST)[:-num:])

    def not_left(self, num):
        """
        NOT REQUIRED, EXISTS AS OPPOSITE OF not_right()
        """
        if num == None:
            return self
        if num <= 0:
            return self

        return FlatList(_get(self, LIST)[num::])

    def last(self):
        """
        RETURN LAST ELEMENT IN FlatList [-1]
        """
        lst = _get(self, LIST)
        if lst:
            return to_data(lst[-1])
        return Null

    def map(self, oper, includeNone=True):
        if includeNone:
            return FlatList([oper(v) for v in _get(self, LIST)])
        else:
            return FlatList([oper(v) for v in _get(self, LIST) if v != None])


def last(values):
    if is_many(values):
        if not values:
            return Null
        if isinstance(values, FlatList):
            return values.last()
        elif is_list(values):
            if not values:
                return Null
            return values[-1]
        elif is_sequence(values):
            l = Null
            for i in values:
                l = i
            return l
        else:
            return first(values)

    return values


list_types = (list, FlatList)
container_types = (list, FlatList, set)
sequence_types = (list, FlatList, tuple) + generator_types
many_types = tuple(set(list_types + container_types + sequence_types))

# ITERATORS THAT ARE CONSIDERED PRIMITIVE
not_many_names = ("str", "unicode", "binary", "NullType", "NoneType", "dict", "Data")


def is_list(l):
    # ORDERED, AND CAN CHANGE CONTENTS
    return l.__class__ in list_types


def is_container(l):
    # CAN ADD AND REMOVE ELEMENTS
    return l.__class__ in container_types


def is_sequence(l):
    # HAS AN ORDER, INCLUDES GENERATORS
    return l.__class__ in sequence_types


def is_many(value):
    # REPRESENTS MULTIPLE VALUES
    # TODO: CLEAN UP THIS LOGIC
    # THIS IS COMPLICATED BECAUSE I AM UNSURE ABOUT ALL THE "PRIMITIVE TYPES"
    # I WOULD LIKE TO POSITIVELY CATCH many_types, BUT MAYBE IT IS EASIER TO DETECT: Iterable, BUT NOT PRIMITIVE
    # UNTIL WE HAVE A COMPLETE LIST, WE KEEP ALL THIS warning() CODE
    global many_types
    type_ = value.__class__
    if type_ in many_types:
        return True

    if issubclass(type_, types.GeneratorType):
        many_types = many_types + (type_,)
        Log.warning("is_many() can not detect generator {{type}}", type=type_.__name__)
        return True
    return False


