# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

from copy import copy, deepcopy
from decimal import Decimal

from mo_dots.lists import is_list, FlatList, is_sequence
from mo_dots.nones import Null, NullType
from mo_dots.utils import CLASS
from mo_dots.utils import get_logger
from mo_future import (
    generator_types,
    iteritems,
    long,
    none_type,
    text,
    MutableMapping,
    OrderedDict,
)
from mo_imports import expect

(
    _getdefault,
    coalesce,
    hash_value,
    listwrap,
    literal_field,
    from_data,
    to_data,
) = expect(
    "_getdefault",
    "coalesce",
    "hash_value",
    "listwrap",
    "literal_field",
    "from_data",
    "to_data",
)


_get = object.__getattribute__
_set = object.__setattr__
_new = object.__new__

SLOT = str("_internal_dict")
DEBUG = False


class Data(object):
    """
    Please see README.md
    """

    __slots__ = [SLOT]

    def __init__(self, *args, **kwargs):
        """
        CALLING Data(**something) WILL RESULT IN A COPY OF something, WHICH
        IS UNLIKELY TO BE USEFUL. USE to_data() INSTEAD
        """
        if DEBUG:
            d = self._internal_dict
            for k, v in kwargs.items():
                d[literal_field(k)] = from_data(v)
        else:
            if args:
                args0 = args[0]
                class_ = _get(args0, CLASS)
                if class_ is dict:
                    _set(self, SLOT, args0)
                elif class_ is Data:
                    _set(self, SLOT, _get(args0, SLOT))
                else:
                    _set(self, SLOT, dict(args0))
            elif kwargs:
                _set(self, SLOT, from_data(kwargs))
            else:
                _set(self, SLOT, {})

    def __bool__(self):
        d = self._internal_dict
        if _get(d, CLASS) is dict:
            return bool(d)
        else:
            return d != None

    def __nonzero__(self):
        d = self._internal_dict
        if _get(d, CLASS) is dict:
            return True if d else False
        else:
            return d != None

    def __contains__(self, item):
        value = Data.__getitem__(self, item)
        if _get(value, CLASS) in data_types or value:
            return True
        return False

    def __iter__(self):
        d = self._internal_dict
        return d.__iter__()

    def __getitem__(self, key):
        if key == None:
            return Null
        if key == ".":
            output = self._internal_dict
            if _get(output, CLASS) in data_types:
                return self
            else:
                return output

        key = text(key)
        d = self._internal_dict

        if key.find(".") >= 0:
            seq = _split_field(key)
            for n in seq:
                if _get(d, CLASS) is NullType:
                    d = NullType(d, n)  # OH DEAR, Null TREATS n AS PATH, NOT LITERAL
                elif is_list(d):
                    d = [_getdefault(dd, n) for dd in d]
                else:
                    d = _getdefault(d, n)  # EVERYTHING ELSE TREATS n AS LITERAL

            return to_data(d)
        else:
            o = d.get(key)

        if o == None:
            return NullType(d, key)
        return to_data(o)

    def __setitem__(self, key, value):
        if key == "":
            get_logger().error("key is empty string.  Probably a bad idea")
        if key == None:
            return Null
        if key == ".":
            # SOMETHING TERRIBLE HAPPENS WHEN value IS NOT A Mapping;
            # HOPEFULLY THE ONLY OTHER METHOD RUN ON self IS from_data()
            v = from_data(value)
            _set(self, SLOT, v)
            return v

        try:
            d = self._internal_dict
            value = from_data(value)
            if key.find(".") == -1:
                if value is None:
                    d.pop(key, None)
                else:
                    d[key] = value
                return self

            seq = _split_field(key)
            for k in seq[:-1]:
                d = _getdefault(d, k)
            if value == None:
                try:
                    d.pop(seq[-1], None)
                except Exception as _:
                    pass
            elif d == None:
                d[literal_field(seq[-1])] = value
            elif is_sequence(d):
                for dd in d:
                    from_data(dd)[seq[-1]] = value
            else:
                d[seq[-1]] = value
            return self
        except Exception as e:
            from mo_logs import Log

            Log.error("can not set key={{key}}", key=key, cause=e)

    def __getattr__(self, key):
        d = self._internal_dict
        v = d.get(key)
        t = _get(v, CLASS)

        # OPTIMIZED to_data()
        if t is dict:
            m = _new(Data)
            _set(m, SLOT, v)
            return m
        elif t in (none_type, NullType):
            return NullType(d, key)
        elif t is list:
            return FlatList(v)
        elif t in generator_types:
            return FlatList(list(from_data(vv) for vv in v))
        else:
            return v

    def __setattr__(self, key, value):
        d = self._internal_dict
        value = from_data(value)
        if value is None:
            d = self._internal_dict
            d.pop(key, None)
        else:
            d[key] = value
        return self

    def __add__(self, other):
        return _iadd(_iadd({}, self), other)

    def __radd__(self, other):
        return _iadd(_iadd({}, other), self)

    def __iadd__(self, other):
        return _iadd(self, other)

    def __or__(self, other):
        """
        RECURSIVE COALESCE OF DATA PROPERTIES
        """
        if not _get(other, CLASS) in data_types:
            get_logger().error("Expecting Data")

        d = self._internal_dict
        output = Data(**d)  # COPY
        output.__ior__(other)
        return output

    def __ror__(self, other):
        """
        RECURSIVE COALESCE OF DATA PROPERTIES
        """
        if not _get(other, CLASS) in data_types:
            get_logger().error("Expecting Data")

        return to_data(other).__or__(self)

    def __ior__(self, other):
        """
        RECURSIVE COALESCE OF DATA PROPERTIES
        """
        if not _get(other, CLASS) in data_types:
            get_logger().error("Expecting Data")
        d = self._internal_dict
        for ok, ov in other.items():
            if ov == None:
                continue

            sv = d.get(ok)
            if sv == None:
                d[ok] = ov
            elif isinstance(sv, Data):
                sv |= ov
            elif is_data(sv):
                wv = _new(Data)
                _set(wv, SLOT, sv)
                wv |= ov
        return self

    def __hash__(self):
        d = self._internal_dict
        return hash_value(d)

    def __eq__(self, other):
        if self is other:
            return True

        d = self._internal_dict
        if _get(d, CLASS) is not dict:
            return d == other

        if not d and other == None:
            return False

        if _get(other, CLASS) not in data_types:
            return False
        e = from_data(other)
        for k, v in d.items():
            if e.get(k) != v:
                return False
        for k, v in e.items():
            if d.get(k) != v:
                return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def get(self, key, default=None):
        d = self._internal_dict
        return d.get(key, default)

    def items(self):
        d = self._internal_dict
        return [
            (k, to_data(v))
            for k, v in d.items()
            if v != None or _get(v, CLASS) in data_types
        ]

    def leaves(self, prefix=None):
        """
        LIKE items() BUT RECURSIVE, AND ONLY FOR THE LEAVES (non dict) VALUES
        """
        return leaves(self, prefix)

    def iteritems(self):
        # LOW LEVEL ITERATION, NO WRAPPING
        d = self._internal_dict
        return ((k, to_data(v)) for k, v in iteritems(d))

    def pop(self, item, default=None):
        d = self._internal_dict
        return d.pop(item, default)

    def keys(self):
        d = self._internal_dict
        return set(d.keys())

    def values(self):
        d = self._internal_dict
        return listwrap(list(d.values()))

    def clear(self):
        get_logger().error("clear() not supported")

    def __len__(self):
        d = self._internal_dict
        return dict.__len__(d)

    def copy(self):
        d = self._internal_dict
        if _get(d, CLASS) is dict:
            return Data(**d)
        else:
            return copy(d)

    def __copy__(self):
        d = self._internal_dict
        if _get(d, CLASS) is dict:
            return Data(**self)
        else:
            return copy(d)

    def __deepcopy__(self, memo):
        d = self._internal_dict
        return to_data(deepcopy(d, memo))

    def __delitem__(self, key):
        if key.find(".") == -1:
            d = self._internal_dict
            d.pop(key, None)
            return

        d = self._internal_dict
        seq = _split_field(key)
        for k in seq[:-1]:
            d = d[k]
        d.pop(seq[-1], None)

    def __delattr__(self, key):
        key = text(key)
        d = self._internal_dict
        d.pop(key, None)

    def setdefault(self, k, d=None):
        if self[k] == None:
            self[k] = d
        return self

    def __str__(self):
        try:
            return dict.__str__(self._internal_dict)
        except Exception:
            return "{}"

    def __dir__(self):
        d = self._internal_dict
        return d.keys()

    def __repr__(self):
        try:
            return "Data(" + dict.__repr__(self._internal_dict) + ")"
        except Exception as e:
            return "Data()"


MutableMapping.register(Data)


def leaves(value, prefix=None):
    """
    LIKE items() BUT RECURSIVE, AND ONLY FOR THE LEAVES (non dict) VALUES
    SEE leaves_to_data FOR THE INVERSE

    :param value: THE Mapping TO TRAVERSE
    :param prefix:  OPTIONAL PREFIX GIVEN TO EACH KEY
    :return: Data, WHICH EACH KEY BEING A PATH INTO value TREE
    """
    prefix = coalesce(prefix, "")
    output = []
    for k, v in value.items():
        try:
            if _get(v, CLASS) in data_types:
                output.extend(leaves(v, prefix=prefix + literal_field(k) + "."))
            else:
                output.append((prefix + literal_field(k), from_data(v)))
        except Exception as e:
            get_logger().error("Do not know how to handle", cause=e)
    return output


def _split_field(field):
    """
    SIMPLE SPLIT, NO CHECKS
    """
    return [k.replace("\a", ".") for k in field.replace("\\.", "\a").split(".")]


def _str(value, depth):
    """
    FOR DEBUGGING POSSIBLY RECURSIVE STRUCTURES
    """
    output = []
    if depth > 0 and _get(value, CLASS) in data_types:
        for k, v in value.items():
            output.append(str(k) + "=" + _str(v, depth - 1))
        return "{" + ",\n".join(output) + "}"
    elif depth > 0 and is_list(value):
        for v in value:
            output.append(_str(v, depth - 1))
        return "[" + ",\n".join(output) + "]"
    else:
        return str(type(value))


def _iadd(self, other):
    """
    RECURSIVE ADDITION OF DATA PROPERTIES
    * LISTS ARE CONCATENATED
    * SETS ARE UNIONED
    * NUMBERS ARE ADDED
    """

    if not _get(other, CLASS) in data_types:
        get_logger().error("Expecting Data")
    d = from_data(self)
    for ok, ov in other.items():
        sv = d.get(ok)
        if sv == None:
            d[ok] = deepcopy(ov)
        elif isinstance(ov, (Decimal, float, long, int)):
            if _get(sv, CLASS) in data_types:
                get_logger().error(
                    "can not add {{stype}} with {{otype}",
                    stype=_get(sv, CLASS).__name__,
                    otype=_get(ov, CLASS).__name__,
                )
            elif is_list(sv):
                d[ok].append(ov)
            else:
                d[ok] = sv + ov
        elif is_list(ov):
            d[ok] = listwrap(sv) + ov
        elif _get(ov, CLASS) in data_types:
            if _get(sv, CLASS) in data_types:
                _iadd(sv, ov)
            elif is_list(sv):
                d[ok].append(ov)
            else:
                get_logger().error(
                    "can not add {{stype}} with {{otype}",
                    stype=_get(sv, CLASS).__name__,
                    otype=_get(ov, CLASS).__name__,
                )
        else:
            if _get(sv, CLASS) in data_types:
                get_logger().error(
                    "can not add {{stype}} with {{otype}",
                    stype=_get(sv, CLASS).__name__,
                    otype=_get(ov, CLASS).__name__,
                )
            else:
                d[ok].append(ov)
    return self


data_types = (Data, dict, OrderedDict)  # TYPES TO HOLD DATA


def register_data(type_):
    """
    :param type_:  ADD OTHER TYPE THAT HOLDS DATA
    :return:
    """
    global data_types
    data_types = tuple(set(data_types + (type_,)))


def is_data(d):
    """
    :param d:
    :return: True IF d IS A TYPE THAT HOLDS DATA
    """
    return d.__class__ in data_types


