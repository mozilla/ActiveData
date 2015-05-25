# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with self file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from collections import Mapping
import re
from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.queries.unique_index import UniqueIndex
from pyLibrary.dot import coalesce, Dict, set_default, Null
from pyLibrary.dot.lists import DictList
from pyLibrary.dot import wrap, unwrap
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import Duration

ALGEBRAIC = {"time", "duration", "numeric", "count", "datetime"}  # DOMAINS THAT HAVE ALGEBRAIC OPERATIONS DEFINED
KNOWN = {"set", "boolean", "duration", "time", "numeric"}  # DOMAINS THAT HAVE A KNOWN NUMBER FOR PARTS AT QUERY TIME
PARTITION = {"uid", "set", "boolean"}  # DIMENSIONS WITH CLEAR PARTS




class Domain(object):
    __slots__ = ["name", "type", "value", "key", "label", "end", "isFacet", "where", "dimension", "primitive"]

    def __new__(cls, **desc):
        if cls == Domain:
            try:
                return name_to_type[desc.get("type")](**desc)
            except Exception, e:
                Log.error("Do not know domain of type {{type}}", type=desc.get("type"), cause=e)
        else:
            return object.__new__(cls)

    def __init__(self, **desc):
        desc = wrap(desc)
        self._set_slots_to_none(self.__class__)
        set_default(self, desc)
        self.name = coalesce(desc.name, desc.type)
        self.isFacet = coalesce(desc.isFacet, False)

    def _set_slots_to_none(self, cls):
        """
        WHY ARE SLOTS NOT ACCESIBLE UNTIL WE ASSIGN TO THEM?
        """
        if hasattr(cls, "__slots__"):
            for s in cls.__slots__:
                self.__setattr__(s, None)
        for b in cls.__bases__:
            self._set_slots_to_none(b)


    def __copy__(self):
        return self.__class__(**self.as_dict())

    def copy(self):
        return self.__class__(**self.as_dict())

    def as_dict(self):
        return wrap({
            "name": self.name,
            "type": self.type,
            "value": self.value,
            "key": self.key,
            "isFacet": self.isFacet,
            "where": self.where,
            "dimension": self.dimension
        })

    def __json__(self):
        return convert.value2json(self.as_dict())

    @property
    def __all_slots__(self):
        return self._all_slots(self.__class__)

    def _all_slots(self, cls):
        output = set(getattr(cls, '__slots__', []))
        for b in cls.__bases__:
            output |= self._all_slots(b)
        return output

    def getDomain(self):
        Log.error("Not implemented")


class ValueDomain(Domain):
    __slots__ = ["NULL"]

    def __init__(self, **desc):
        Domain.__init__(self, **desc)
        self.NULL = None

    def compare(self, a, b):
        return value_compare(a, b)

    def getCanonicalPart(self, part):
        return part

    def getPartByKey(self, key):
        return key

    def getKey(self, part):
        return part

    def getEnd(self, value):
        return value


class DefaultDomain(Domain):
    """
    DOMAIN IS A LIST OF OBJECTS, EACH WITH A value PROPERTY
    """

    __slots__ = ["NULL", "partitions", "map", "limit"]

    def __init__(self, **desc):
        Domain.__init__(self, **desc)

        self.NULL = Null
        self.partitions = DictList()
        self.map = dict()
        self.map[None] = self.NULL

    def compare(self, a, b):
        return value_compare(a.value, b.value)

    def getCanonicalPart(self, part):
        return self.getPartByKey(part.value)

    def getPartByKey(self, key):
        canonical = self.map.get(key)
        if canonical:
            return canonical

        canonical = Dict(name=key, value=key)

        self.partitions.append(canonical)
        self.map[key] = canonical
        return canonical

    def getKey(self, part):
        return part.value

    def getEnd(self, part):
        return part.value

    def getLabel(self, part):
        return part.value

    def as_dict(self):
        output = Domain.as_dict(self)
        output.partitions = self.partitions
        return output


class SimpleSetDomain(Domain):
    """
    DOMAIN IS A LIST OF OBJECTS, EACH WITH A value PROPERTY
    """

    __slots__ = ["NULL", "partitions", "map", "order"]

    def __init__(self, **desc):
        Domain.__init__(self, **desc)
        desc = wrap(desc)

        self.type = "set"
        self.order = {}
        self.NULL = Null
        self.partitions = DictList()
        self.primitive = True  # True IF DOMAIN IS A PRIMITIVE VALUE SET

        if isinstance(self.key, set):
            Log.error("problem")

        if isinstance(desc.partitions[0], basestring):
            # ASSUME PARTS ARE STRINGS, CONVERT TO REAL PART OBJECTS
            self.key = "value"
            self.map = {}
            self.order[None] = len(desc.partitions)
            for i, p in enumerate(desc.partitions):
                part = {"name": p, "value": p, "dataIndex": i}
                self.partitions.append(part)
                self.map[p] = part
                self.order[p] = i
            self.label = coalesce(self.label, "name")
            self.primitive = True
            return

        if desc.partitions and desc.dimension.fields and len(desc.dimension.fields) > 1:
            self.key = desc.key
            self.map = UniqueIndex(keys=desc.dimension.fields)
        elif desc.partitions and isinstance(desc.key, (list, set)):
            # TODO: desc.key CAN BE MUCH LIKE A SELECT, WHICH UniqueIndex CAN NOT HANDLE
            self.key = desc.key
            self.map = UniqueIndex(keys=desc.key)
        elif desc.partitions and isinstance(desc.partitions[0][desc.key], Mapping):
            self.key = desc.key
            self.map = UniqueIndex(keys=desc.key)
            # self.key = UNION(set(d[desc.key].keys()) for d in desc.partitions)
            # self.map = UniqueIndex(keys=self.key)
        elif len(desc.partitions) == 0:
            # CREATE AN EMPTY DOMAIN
            self.key = "value"
            self.map = {}
            self.order[None] = 0
            self.label = coalesce(self.label, "name")
            return
        elif desc.key == None:
            if desc.partitions and len(set(desc.partitions.value)) == len(desc.partitions):
                # TRY A COMMON KEY CALLED "value".  IT APPEARS UNIQUE
                self.key = "value"
                self.map = dict()
                self.map[None] = self.NULL
                self.order[None] = len(desc.partitions)
                for i, p in enumerate(desc.partitions):
                    self.map[p[self.key]] = p
                    self.order[p[self.key]] = i
                self.primitive = False
            else:
                Log.error("Domains must have keys")
        elif self.key:
            self.key = desc.key
            self.map = dict()
            self.map[None] = self.NULL
            self.order[None] = len(desc.partitions)
            for i, p in enumerate(desc.partitions):
                self.map[p[self.key]] = p
                self.order[p[self.key]] = i
            self.primitive = False
        elif all(p.esfilter for p in self.partitions):
            # EVERY PART HAS AN esfilter DEFINED, SO USE THEM
            for i, p in enumerate(self.partitions):
                p.dataIndex = i

        else:
            Log.error("Can not hanldle")

        self.label = coalesce(self.label, "name")

        if hasattr(desc.partitions, "__iter__"):
            self.partitions = list(desc.partitions)
        else:
            Log.error("expecting a list of partitions")

    def compare(self, a, b):
        return value_compare(self.getKey(a), self.getKey(b))

    def getCanonicalPart(self, part):
        return self.getPartByKey(part.value)

    def getIndexByKey(self, key):
        try:
            output = self.order.get(key)
            if output is None:
                return len(self.partitions)
            return output
        except Exception, e:
            Log.error("problem", e)


    def getPartByKey(self, key):
        try:
            canonical = self.map.get(key)
            if not canonical:
                return self.NULL
            return canonical
        except Exception, e:
            Log.error("problem", e)

    def getPartByIndex(self, index):
        return self.partitions[index]

    def getKeyByIndex(self, index):
        return self.partitions[index][self.key]

    def getKey(self, part):
        return part[self.key]

    def getEnd(self, part):
        if self.value:
            return part[self.value]
        else:
            return part

    def getLabel(self, part):
        return part[self.label]

    def as_dict(self):
        output = Domain.as_dict(self)
        output.partitions = self.partitions
        return output


class SetDomain(Domain):
    __slots__ = ["NULL", "partitions", "map", "order"]

    def __init__(self, **desc):
        Domain.__init__(self, **desc)
        desc = wrap(desc)

        self.type = "set"
        self.order = {}
        self.NULL = Null
        self.partitions = DictList()

        if isinstance(self.key, set):
            Log.error("problem")

        if isinstance(desc.partitions[0], basestring):
            # ASSMUE PARTS ARE STRINGS, CONVERT TO REAL PART OBJECTS
            self.key = ("value", )
            self.order[None] = len(desc.partitions)
            for i, p in enumerate(desc.partitions):
                part = {"name": p, "value": p}
                self.partitions.append(part)
                self.map[p] = part
                self.order[p] = i
        elif desc.partitions and desc.dimension.fields and len(desc.dimension.fields) > 1:
            self.key = desc.key
            self.map = UniqueIndex(keys=desc.dimension.fields)
        elif desc.partitions and isinstance(desc.key, (list, set)):
            # TODO: desc.key CAN BE MUCH LIKE A SELECT, WHICH UniqueIndex CAN NOT HANDLE
            self.key = desc.key
            self.map = UniqueIndex(keys=desc.key)
        elif desc.partitions and isinstance(desc.partitions[0][desc.key], Mapping):
            self.key = desc.key
            self.map = UniqueIndex(keys=desc.key)
            # self.key = UNION(set(d[desc.key].keys()) for d in desc.partitions)
            # self.map = UniqueIndex(keys=self.key)
        elif desc.key == None:
            Log.error("Domains must have keys")
        elif self.key:
            self.key = desc.key
            self.map = dict()
            self.map[None] = self.NULL
            self.order[None] = len(desc.partitions)
            for i, p in enumerate(desc.partitions):
                self.map[p[self.key]] = p
                self.order[p[self.key]] = i
        elif all(p.esfilter for p in self.partitions):
            # EVERY PART HAS AN esfilter DEFINED, SO USE THEM
            for i, p in enumerate(self.partitions):
                p.dataIndex = i

        else:
            Log.error("Can not hanldle")

        self.label = coalesce(self.label, "name")

        if isinstance(desc.partitions, list):
            self.partitions = desc.partitions.copy()
        else:
            Log.error("expecting a list of partitions")

    def compare(self, a, b):
        return value_compare(self.getKey(a), self.getKey(b))

    def getCanonicalPart(self, part):
        return self.getPartByKey(part.value)

    def getIndexByKey(self, key):
        try:
            output = self.order.get(key)
            if output is None:
                return len(self.partitions)
            return output
        except Exception, e:
            Log.error("problem", e)


    def getPartByKey(self, key):
        try:
            canonical = self.map.get(key, None)
            if not canonical:
                return self.NULL
            return canonical
        except Exception, e:
            Log.error("problem", e)

    def getKey(self, part):
        return part[self.key]

    def getKeyByIndex(self, index):
        return self.partitions[index][self.key]

    def getEnd(self, part):
        if self.value:
            return part[self.value]
        else:
            return part

    def getLabel(self, part):
        return part[self.label]

    def as_dict(self):
        output = Domain.as_dict(self)
        output.partitions = self.partitions
        return output




class TimeDomain(Domain):
    __slots__ = ["max", "min", "interval", "partitions", "NULL"]

    def __init__(self, **desc):
        Domain.__init__(self, **desc)
        self.type = "time"
        self.NULL = Null
        self.min = Date(self.min)
        self.max = Date(self.max)
        self.interval = Duration(self.interval)

        if self.partitions:
            # IGNORE THE min, max, interval
            if not self.key:
                Log.error("Must have a key value")

            Log.error("not implemented yet")

            # VERIFY PARTITIONS DO NOT OVERLAP
            return
        elif not all([self.min, self.max, self.interval]):
            Log.error("Can not handle missing parameter")

        self.key = "min"
        self.partitions = wrap([{"min": v, "max": v + self.interval, "dataIndex":i} for i, v in enumerate(Date.range(self.min, self.max, self.interval))])

    def compare(self, a, b):
        return value_compare(a, b)

    def getCanonicalPart(self, part):
        return self.getPartByKey(part[self.key])

    def getIndexByKey(self, key):
        for p in self.partitions:
            if p.min <= key < p.max:
                return p.dataIndex
        return len(self.partitions)

    def getPartByKey(self, key):
        for p in self.partitions:
            if p.min <= key < p.max:
                return p
        return self.NULL

    def getKey(self, part):
        return part[self.key]

    def getKeyByIndex(self, index):
        return self.partitions[index][self.key]

    def as_dict(self):
        output = Domain.as_dict(self)

        output.partitions = self.partitions
        output.min = self.min
        output.max = self.max
        output.interval = self.interval
        return output


class DurationDomain(Domain):
    __slots__ = ["max", "min", "interval", "partitions", "NULL"]

    def __init__(self, **desc):
        Domain.__init__(self, **desc)
        self.type = "duration"
        self.NULL = Null
        self.min = Duration(self.min)
        self.max = Duration(self.max)
        self.interval = Duration(self.interval)

        if self.partitions:
            # IGNORE THE min, max, interval
            if not self.key:
                Log.error("Must have a key value")

            Log.error("not implemented yet")

            # VERIFY PARTITIONS DO NOT OVERLAP
            return
        elif not all([self.min, self.max, self.interval]):
            Log.error("Can not handle missing parameter")

        self.key = "min"
        self.partitions = wrap([{"min": v, "max": v + self.interval, "dataIndex":i} for i, v in enumerate(Duration.range(self.min, self.max, self.interval))])

    def compare(self, a, b):
        return value_compare(a, b)

    def getCanonicalPart(self, part):
        return self.getPartByKey(part[self.key])

    def getIndexByKey(self, key):
        for p in self.partitions:
            if p.min <= key < p.max:
                return p.dataIndex
        return len(self.partitions)

    def getPartByKey(self, key):
        for p in self.partitions:
            if p.min <= key < p.max:
                return p
        return self.NULL

    def getKey(self, part):
        return part[self.key]

    def getKeyByIndex(self, index):
        return self.partitions[index][self.key]

    def as_dict(self):
        output = Domain.as_dict(self)

        output.partitions = self.partitions
        output.min = self.min
        output.max = self.max
        output.interval = self.interval
        return output


class RangeDomain(Domain):
    __slots__ = ["max", "min", "interval", "partitions", "NULL"]

    def __init__(self, **desc):
        Domain.__init__(self, **desc)
        self.type = "range"
        self.NULL = Null
        self.min = self.min
        self.max = self.max
        self.interval = self.interval

        if self.partitions:
            # IGNORE THE min, max, interval
            if not self.key:
                Log.error("Must have a key value")

            Log.error("not implemented yet")

            # VERIFY PARTITIONS DO NOT OVERLAP
            return
        elif any([self.min == None, self.max == None, self.interval == None]):
            Log.error("Can not handle missing parameter")

        self.key = "min"
        self.partitions = wrap([{"min": v, "max": v + self.interval, "dataIndex": i} for i, v in enumerate(range(self.min, self.max, self.interval))])

    def compare(self, a, b):
        return value_compare(a, b)

    def getCanonicalPart(self, part):
        return self.getPartByKey(part[self.key])

    def getIndexByKey(self, key):
        for p in self.partitions:
            if p.min <= key < p.max:
                return p.dataIndex
        return len(self.partitions)

    def getPartByKey(self, key):
        for p in self.partitions:
            if p.min <= key < p.max:
                return p
        return self.NULL

    def getKey(self, part):
        return part[self.key]

    def getKeyByIndex(self, index):
        return self.partitions[index][self.key]

    def as_dict(self):
        output = Domain.as_dict(self)

        output.partitions = self.partitions
        output.min = self.min
        output.max = self.max
        output.interval = self.interval
        return output



def value_compare(a, b):
    if a == None:
        if b == None:
            return 0
        return -1
    elif b == None:
        return 1

    if a > b:
        return 1
    elif a < b:
        return -1
    else:
        return 0


keyword_pattern = re.compile(r"\w+(?:\.\w+)*")


def is_keyword(value):
    if not value or not isinstance(value, basestring):
        return False  # _a._b
    return keyword_pattern.match(value).group(0) == value


name_to_type = {
    "value": ValueDomain,
    "default": DefaultDomain,
    "set": SimpleSetDomain,
    "uid": DefaultDomain,
    "time": TimeDomain,
    "duration": DurationDomain,
    "range": RangeDomain
}

