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
from pyLibrary.dot import split_field, _getdefault, hash_value, literal_field, nvl

_get = object.__getattribute__
_set = object.__setattr__

DEBUG = False


class Dict(dict):
    """
    Dict is used to declare an instance of an anonymous type, and has good
    features for manipulating JSON.  Anonymous types are necessary when
    writing sophisticated list comprehensions, or queries, and to keep them
    readable.  In many ways, dict() can act as an anonymous type, but it does
    not have the features listed here.

    0) a.b==a["b"]
    1) by allowing dot notation, the IDE does tab completion and my spelling
       mistakes get found at "compile time"
    2) it deals with missing keys gracefully, so I can put it into set
       operations (database operations) without raising exceptions
       a = wrap({})
       > a == {}
       a.b == None
       > True
       a.b.c == None
       > True
       a[None] == None
       > True
    2b) missing keys is important when dealing with JSON, which is often almost
        anything
    2c) you loose the ability to perform <code>a is None</code> checks, must
        always use <code>a == None</code> instead
    3) remove an attribute by assigning Null:  setting a
    4) you can access paths as a variable:   a["b.c"]==a.b.c
    5) you can set paths to values, missing dicts along the path are created:
       a = wrap({})
       > a == {}
       a["b.c"] = 42
       > a == {"b": {"c": 42}}
    6) attribute names (keys) are corrected to unicode - it appears Python
       object.getattribute() is called with str() even when using
       <code>from __future__ import unicode_literals</code>

    More on missing values: http://www.np.org/NA-overview.html
    it only considers the legitimate-field-with-missing-value (Statistical Null)
    and does not look at field-does-not-exist-in-this-context (Database Null)

    The Dict is a common pattern in many frameworks even though it goes by
    different names, some examples are:

    * jinja2.environment.Environment.getattr()
    * argparse.Environment() - code performs setattr(e, name, value) on
      instances of Environment to provide dot(.) accessors
    * collections.namedtuple() - gives attribute names to tuple indicies
      effectively providing <code>a.b</code> rather than <code>a["b"]</code>
      offered by dicts
    * DotDict allows dot notation, and path setting: https://github.com/mozilla/configman/blob/master/configman/dotdict.py
    * C# Linq requires anonymous types to avoid large amounts of boilerplate code.
    * D3 has many of these conventions ["The function's return value is
      then used to set each element's attribute. A null value will remove the
      specified attribute."](https://github.com/mbostock/d3/wiki/Selections#attr)


    http://www.saltycrane.com/blog/2012/08/python-data-object-motivated-desire-mutable-namedtuple-default-values/

    """

    #  http://www.saltycrane.com/
    def __init__(self, **map):
        """
        CALLING Dict(**something) WILL RESULT IN A COPY OF something, WHICH IS UNLIKELY TO BE USEFUL
        USE wrap() INSTEAD
        """
        dict.__init__(self)
        if DEBUG:
            d = _get(self, "__dict__")
            for k, v in map.items():
                d[literal_field(k)] = unwrap(v)
        else:
            if map:
                _set(self, "__dict__", map)

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

        o = d.get(key, None)
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
            if isinstance(key, str):
                key = key.decode("utf8")

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
        d = _get(self, "__dict__")
        if not d and other == None:
            return True

        if not isinstance(other, dict):
            return False
        e = unwrap(other)
        d = _get(self, "__dict__")
        for k, v in d.items():
            if e.get(k, None) != v:
                return False
        for k, v in e.items():
            if d.get(k, None) != v:
                return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def get(self, key, default):
        d = _get(self, "__dict__")
        return d.get(key, default)

    def items(self):
        d = _get(self, "__dict__")
        return ((k, wrap(v)) for k, v in d.items())

    def leaves(self, prefix=None):
        """
        LIKE items() BUT RECURSIVE, AND ONLY FOR THE LEAVES (non dict) VALUES
        """
        prefix = nvl(prefix, "")
        output = []
        for k, v in self.items():
            if isinstance(v, dict):
                output.extend(wrap(v).leaves(prefix=prefix+literal_field(k)+"."))
            else:
                output.append((prefix+literal_field(k), v))
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
