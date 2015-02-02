# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from __future__ import division
from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap, unwrap, tuplewrap

class UniqueIndex(object):
    """
    DEFINE A SET OF ATTRIBUTES THAT UNIQUELY IDENTIFIES EACH OBJECT IN A list.
    THIS ALLOWS set-LIKE COMPARISIONS (UNION, INTERSECTION, DIFFERENCE, ETC) WHILE
    STILL MAINTAINING list-LIKE FEATURES
    """

    def __init__(self, keys, fail_on_dup=True):
        self._data = {}
        self._keys = tuplewrap(keys)
        self.count = 0
        self.fail_on_dup = fail_on_dup

    def __getitem__(self, key):
        try:
            key = value2key(self._keys, key)
            d = self._data.get(key, None)
            return wrap(d)
        except Exception, e:
            Log.error("something went wrong", e)

    def __setitem__(self, key, value):
        try:
            key = value2key(self._keys, key)
            d = self._data.get(key, None)
            if d != None:
                Log.error("key already filled")

            self._data[key] = unwrap(value)
            self.count += 1

        except Exception, e:
            Log.error("something went wrong", e)


    def add(self, val):
        key = value2key(self._keys, val)
        d = self._data.get(key, None)
        if d is None:
            self._data[key] = unwrap(val)
            self.count += 1
        elif d is not val:
            if self.fail_on_dup:
                Log.error("key {{key|json}} already filled", {"key":key})
            else:
                Log.warning("key {{key|json}} already filled\nExisting\n{{existing|json|indent}}\nValue\n{{value|json|indent}}", {
                    "key": key,
                    "existing": d,
                    "value": val
                })


    def __contains__(self, key):
        return self[key] != None

    def __iter__(self):
        return (wrap(v) for v in self._data.itervalues())

    def __sub__(self, other):
        output = UniqueIndex(self._keys)
        for v in self:
            if v not in other:
                output.add(v)
        return output

    def __and__(self, other):
        output = UniqueIndex(self._keys)
        for v in self:
            if v in other: output.add(v)
        return output

    def __or__(self, other):
        output = UniqueIndex(self._keys)
        for v in self: output.add(v)
        for v in other: output.add(v)
        return output

    def __len__(self):
        if self.count == 0:
            for d in self:
                self.count += 1
        return self.count

    def subtract(self, other):
        return self.__sub__(other)

    def intersect(self, other):
        return self.__and__(other)

def value2key(keys, val):
    if len(keys)==1:
        if isinstance(val, dict):
            return val[keys[0]]
        elif isinstance(val, (list, tuple)):
            return val[0]
        else:
            return val
    else:
        if isinstance(val, dict):
            return wrap({k: val[k] for k in keys})
        elif isinstance(val, (list, tuple)):
            return wrap(dict(zip(keys, val)))
        else:
            Log.error("do not know what to do here")
