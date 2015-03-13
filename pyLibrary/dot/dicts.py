# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from __future__ import division
from copy import deepcopy
from types import NoneType
from pyLibrary.dot import split_field, _getdefault, hash_value, literal_field, nvl

_get = object.__getattribute__
_set = object.__setattr__

DEBUG = False


class Dict(dict):
    """
    Please see README.md
    """

    def __init__(self, **map):
        """
        CALLING Dict(**something) WILL RESULT IN A COPY OF something, WHICH
        IS UNLIKELY TO BE USEFUL. USE wrap() INSTEAD
        """
        dict.__init__(self)
        if not map:
            return

        if DEBUG:
            d = _get(self, "__dict__")
            for k, v in map.items():
                d[literal_field(k)] = unwrap(v)
        else:
            d = _get(self, "__dict__")
            for k, v in map.items():
                if v != None:
                    d[literal_field(k)] = unwrap(v)

    def __bool__(self):
        return True

    def __nonzero__(self):
        d = _get(self, "__dict__")
        return True if d else False

    def __str__(self):
        try:
            return "Dict("+dict.__str__(_get(self, "__dict__"))+")"
        except Exception, e:
            return "{}"

    def __repr__(self):
        try:
            return "Dict("+dict.__repr__(_get(self, "__dict__"))+")"
        except Exception, e:
            return "Dict{}"

    def __contains__(self, item):
        if Dict.__getitem__(self, item):
            return True
        return False

    def __iter__(self):
        return _get(self, "__dict__").__iter__()

    def __getitem__(self, key):
        if key == None:
            return Null
        if isinstance(key, str):
            key = key.decode("utf8")

        d = _get(self, "__dict__")

        if key.find(".") >= 0:
            seq = split_field(key)
            for n in seq:
                d = _getdefault(d, n)
            return wrap(d)

        o = d.get(key)
        if o == None:
            return NullType(d, key)
        return wrap(o)

    def __setitem__(self, key, value):
        if key == "":
            from pyLibrary.debugs.logs import Log

            Log.error("key is empty string.  Probably a bad idea")
        if isinstance(key, str):
            key = key.decode("utf8")

        try:
            d = _get(self, "__dict__")
            value = unwrap(value)
            if key.find(".") == -1:
                if value is None:
                    d.pop(key, None)
                else:
                    d[key] = value
                return self

            seq = split_field(key)
            for k in seq[:-1]:
                d = _getdefault(d, k)
            if value == None:
                d.pop(seq[-1], None)
            else:
                d[seq[-1]] = value
            return self
        except Exception, e:
            raise e

    def __getattribute__(self, key):
        try:
            output = _get(self, key)
            return wrap(output)
        except Exception:
            d = _get(self, "__dict__")
            if isinstance(key, unicode):
                from pyLibrary.debugs.logs import Log

                Log.error("not expected")

            return NullType(d, key)

    def __setattr__(self, key, value):
        if isinstance(key, str):
            ukey = key.decode("utf8")
        else:
            ukey = key

        value = unwrap(value)
        if value is None:
            d = _get(self, "__dict__")
            d.pop(key, None)
        else:
            _set(self, ukey, value)
        return self

    def __hash__(self):
        d = _get(self, "__dict__")
        return hash_value(d)

    def __eq__(self, other):
        if self is other:
            return True

        d = _get(self, "__dict__")
        if not d and other == None:
            return True

        if not isinstance(other, dict):
            return False
        e = unwrap(other)
        d = _get(self, "__dict__")
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
        d = _get(self, "__dict__")
        return d.get(key, default)

    def items(self):
        d = _get(self, "__dict__")
        return [(k, wrap(v)) for k, v in d.items() if v != None]

    def leaves(self, prefix=None):
        """
        LIKE items() BUT RECURSIVE, AND ONLY FOR THE LEAVES (non dict) VALUES
        """
        prefix = nvl(prefix, "")
        output = []
        for k, v in self.items():
            if isinstance(v, dict):
                output.extend(wrap(v).leaves(prefix=prefix + literal_field(k) + "."))
            else:
                output.append((prefix + literal_field(k), v))
        return output

    def all_items(self):
        """
        GET ALL KEY-VALUES OF LEAF NODES IN Dict
        """
        d = _get(self, "__dict__")
        output = []
        for k, v in d.items():
            if isinstance(v, dict):
                _all_items(output, k, v)
            else:
                output.append((k, v))
        return output

    def iteritems(self):
        # LOW LEVEL ITERATION, NO WRAPPING
        d = _get(self, "__dict__")
        return d.iteritems()

    def keys(self):
        d = _get(self, "__dict__")
        return set(d.keys())

    def values(self):
        d = _get(self, "__dict__")
        return (wrap(v) for v in d.values())

    def copy(self):
        """
        SHALLOW COPY
        """
        d = _get(self, "__dict__")
        return wrap(d.copy())

    def __copy__(self):
        d = _get(self, "__dict__")
        return wrap(d.copy())

    def __deepcopy__(self, memo):
        d = _get(self, "__dict__")
        return wrap(deepcopy(d, memo))

    def __delitem__(self, key):
        if isinstance(key, str):
            key = key.decode("utf8")

        if key.find(".") == -1:
            d = _get(self, "__dict__")
            d.pop(key, None)
            return

        d = _get(self, "__dict__")
        seq = split_field(key)
        for k in seq[:-1]:
            d = d[k]
        d.pop(seq[-1], None)

    def __delattr__(self, key):
        if isinstance(key, str):
            key = key.decode("utf8")

        d = _get(self, "__dict__")
        d.pop(key, None)

    def setdefault(self, k, d=None):
        if self[k] == None:
            self[k] = d
        return self


# KEEP TRACK OF WHAT ATTRIBUTES ARE REQUESTED, MAYBE SOME (BUILTIN) ARE STILL USEFUL
requested = set()


def _all_items(output, key, d):
    for k, v in d:
        if isinstance(v, dict):
            _all_items(output, key+"."+k, v)
        else:
            output.append((key+"."+k, v))


def _str(value, depth):
    """
    FOR DEBUGGING POSSIBLY RECURSIVE STRUCTURES
    """
    output = []
    if depth >0 and isinstance(value, dict):
        for k, v in value.items():
            output.append(str(k) + "=" + _str(v, depth - 1))
        return "{" + ",\n".join(output) + "}"
    elif depth >0 and isinstance(value, list):
        for v in value:
            output.append(_str(v, depth-1))
        return "[" + ",\n".join(output) + "]"
    else:
        return str(type(value))





from pyLibrary.dot.nones import Null, NullType
from pyLibrary.dot import unwrap, wrap
