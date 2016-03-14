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
from __future__ import absolute_import

from pyLibrary.dot import split_field, _setdefault

_get = object.__getattribute__
_set = object.__setattr__
_zero_list = []


class NullType(object):
    """
    Structural Null provides closure under the dot (.) operator
        Null[x] == Null
        Null.x == Null

    Null INSTANCES WILL TRACK THEIR OWN DEREFERENCE PATH SO
    ASSIGNMENT CAN BE DONE
    """

    def __init__(self, obj=None, key=None):
        """
        obj - VALUE BEING DEREFERENCED
        key - THE dict ITEM REFERENCE (DOT(.) IS NOT ESCAPED)
        """
        d = _get(self, "__dict__")
        d["_obj"] = obj
        d["__key__"] = key

    def __bool__(self):
        return False

    def __nonzero__(self):
        return False

    def __add__(self, other):
        if isinstance(other, list):
            return other
        return Null

    def __radd__(self, other):
        return Null

    def __call__(self, *args, **kwargs):
        return Null

    def __iadd__(self, other):
        try:
            d = _get(self, "__dict__")
            o = d["_obj"]
            if o is None:
                return self
            key = d["__key__"]

            _assign(o, [key], other)
        except Exception, e:
            raise e
        return other

    def __sub__(self, other):
        return Null

    def __rsub__(self, other):
        return Null

    def __neg__(self):
        return Null

    def __mul__(self, other):
        return Null

    def __rmul__(self, other):
        return Null

    def __div__(self, other):
        return Null

    def __rdiv__(self, other):
        return Null

    def __truediv__(self, other):
        return Null

    def __rtruediv__(self, other):
        return Null

    def __gt__(self, other):
        return False

    def __ge__(self, other):
        return False

    def __le__(self, other):
        return False

    def __lt__(self, other):
        return False

    def __eq__(self, other):
        return other is None or isinstance(other, NullType)

    def __ne__(self, other):
        return other is not None and not isinstance(other, NullType)

    def __getitem__(self, key):
        if isinstance(key, slice):
            return Null
        elif isinstance(key, str):
            key = key.decode("utf8")
        elif isinstance(key, int):
            return NullType(self, key)

        path = split_field(key)
        output = self
        for p in path:
            output = NullType(output, p)
        return output

    def __or__(self, other):
        if other is True:
            return True
        return Null

    def __and__(self, other):
        if other is False:
            return False
        return Null

    def __xor__(self, other):
        return Null

    def __len__(self):
        return 0

    def __iter__(self):
        return _zero_list.__iter__()

    def __deepcopy__(self, memo):
        return None

    def last(self):
        """
        IN CASE self IS INTERPRETED AS A list
        """
        return Null

    def right(self, num=None):
        return Null

    def __getattribute__(self, key):
        try:
            output = _get(self, key)
            return output
        except Exception, e:
            return NullType(self, key)

    def __setattr__(self, key, value):
        NullType.__setitem__(self, key, value)

    def __setitem__(self, key, value):
        try:
            d = _get(self, "__dict__")
            o = d["_obj"]
            path = d["__key__"]
            if path is None:
                return   # NO NEED TO DO ANYTHING

            seq = [path] + split_field(key)
            _assign(o, seq, value)
        except Exception, e:
            raise e

    def keys(self):
        return set()

    def items(self):
        return []

    def pop(self, key, default=None):
        return Null

    def __str__(self):
        return "None"

    def __repr__(self):
        return "Null"

    def __hash__(self):
        return hash(None)



Null = NullType()   # INSTEAD OF None!!!


def _assign(obj, path, value, force=True):
    """
    value IS ASSIGNED TO obj[self.path][key]
    path IS AN ARRAY OF PROPERTY NAMES
    force=False IF YOU PREFER TO use setDefault()
    """
    if isinstance(obj, NullType):
        d = _get(obj, "__dict__")
        o = d["_obj"]
        p = d["__key__"]
        s = [p]+path
        return _assign(o, s, value)

    path0 = path[0]

    if len(path) == 1:
        if force:
            obj[path0] = value
        else:
            _setdefault(obj, path0, value)
        return

    old_value = obj.get(path0)
    if old_value == None:
        if value == None:
            return
        else:
            old_value = {}
            obj[path0] = old_value
    _assign(old_value, path[1:], value)

