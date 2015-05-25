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
from __future__ import absolute_import
from collections import Mapping
from pyLibrary.debugs.logs import Log

from pyLibrary.queries.unique_index import UniqueIndex
from pyLibrary.dot import wrap, unwrap, tuplewrap


class Index(object):
    """
    USING DATABASE TERMINOLOGY, THIS IS A NON-UNIQUE INDEX
    """

    def __init__(self, data, keys=None):
        if keys is None:
            keys=data
            data=None

        self._data = {}
        self._keys = tuplewrap(keys)
        self.count = 0

        if data:
            for i, d in enumerate(data):
                self.add(d)

    def __getitem__(self, key):
        try:
            if isinstance(key, (list, tuple)) and len(key) < len(self._keys):
                # RETURN ANOTHER Index
                filter_key = tuple(self._keys[0:len(key):])
                key = value2key(filter_key, key)
                key = key[:len(filter_key)]
                d = self._data
                for k in key:
                    d = d.get(k, {})
                output = Index(filter_key)
                output._data = d
                return output

            key = value2key(self._keys, key)
            d = self._data
            for k in key:
                d = d.get(k, {})
            return wrap(list(d))
        except Exception, e:
            Log.error("something went wrong", e)

    def __setitem__(self, key, value):
        raise NotImplementedError


    def add(self, val):
        key = value2key(self._keys, val)
        d = self._data
        for k in key[:-1]:
            e = d.get(k)
            if e is None:
                e = {}
                d[k] = e
            d = e
        k = key[-1]
        e = d.get(k)
        if e is None:
            e = []
            d[k] = e
        e.append(unwrap(val))
        self.count += 1


    def __contains__(self, key):
        expected = True if self[key] else False
        testing = self._test_contains(key)

        if testing==expected:
            return testing
        else:
            Log.error("not expected")

    def _test_contains(self, key):
        try:
            if isinstance(key, (list, tuple)) and len(key) < len(self._keys):
                # RETURN ANOTHER Index
                length = len(key)
                key = value2key(self._keys[0:length:], key)
                d = self._data
                for k in key[:length]:
                    try:
                        d = d[k]
                    except Exception, e:
                        return False
                return True

            key = value2key(self._keys, key)
            d = self._data
            for k in key:
                try:
                    d = d[k]
                except Exception, e:
                    return False
            return True
        except Exception, e:
            Log.error("something went wrong", e)




    def __nonzero__(self):
        if self._data.keys():
            return True
        else:
            return False

    def __iter__(self):
        def iter(data, depth):
            if depth == 0:
                for v in data:
                    yield wrap(v)
                return

            for v in data.values():
                for v1 in iter(v, depth - 1):
                    yield wrap(v1)

        return iter(self._data, len(self._keys))

    def __sub__(self, other):
        output = UniqueIndex(self._keys)
        for v in self:
            if v not in other:
                output.add(v)
        return output

    def __and__(self, other):
        output = UniqueIndex(self._keys)
        for v in self:
            if v in other:
                output.add(v)
        return output

    def __or__(self, other):
        output = UniqueIndex(self._keys)
        for v in self:
            output.add(v)
        for v in other:
            output.add(v)
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
    if len(keys) == 1:
        if isinstance(val, Mapping):
            return val[keys[0]],
        elif isinstance(val, (list, tuple)):
            return val[0],
        return val,
    else:
        if isinstance(val, Mapping):
            return tuple(val[k] for k in keys)
        elif isinstance(val, (list, tuple)):
            return tuple(val)
        else:
            Log.error("do not know what to do here")
