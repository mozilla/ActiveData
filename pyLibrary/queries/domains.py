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
import re
from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.queries.unique_index import UniqueIndex
from pyLibrary.dot import nvl, Dict, set_default
from pyLibrary.dot.lists import DictList
from pyLibrary.dot import wrap, unwrap

ALGEBRAIC = {"time", "duration", "numeric", "count", "datetime"}  # DOMAINS THAT HAVE ALGEBRAIC OPERATIONS DEFINED
KNOWN = {"set", "boolean", "duration", "time", "numeric"}    # DOMAINS THAT HAVE A KNOWN NUMBER FOR PARTS AT QUERY TIME
PARTITION = {"uid", "set", "boolean"}    # DIMENSIONS WITH CLEAR PARTS


class Domain(object):
    def __new__(cls, **desc):
        desc = wrap(desc)
        if desc.type == "value":
            return ValueDomain(**unwrap(desc))
        elif desc.type == "default":
            return DefaultDomain(**unwrap(desc))
        elif desc.type == "set":
            return SimpleSetDomain(**unwrap(desc))
        elif desc.type == "uid":
            return DefaultDomain(**unwrap(desc))
        else:
            Log.error("Do not know domain of type {{type}}", {"type": desc.type})

    def __init__(self, **desc):
        desc = wrap(desc)
        self.name = nvl(desc.name, desc.type)
        self.type = desc.type
        self.min = desc.min
        self.max = desc.max
        self.interval = desc.interval
        self.value = desc.value
        self.key = desc.key
        self.label = desc.label
        self.end = desc.end
        self.isFacet = nvl(desc.isFacet, False)
        self.dimension = desc.dimension

    def __copy__(self):
        return Domain(**unwrap(self.dict))

    def copy(self):
        return Domain(**unwrap(self.dict))


    @property
    def dict(self):
        return Dict(
            type=self.type,
            name=self.name,
            partitions=self.partitions,
            min=self.min,
            max=self.max,
            interval=self.interval,
            value=self.value,
            key=self.key,
            label=self.label,
            end=self.end,
            isFacet=self.isFacet
        )

    def __json__(self):
        return convert.value2json(self.dict)


class ValueDomain(Domain):
    def __new__(cls, **desc):
        return object.__new__(ValueDomain)

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

    def __new__(cls, **desc):
        return object.__new__(DefaultDomain)

    def __init__(self, **desc):
        Domain.__init__(self, **desc)

        self.NULL = Dict(value=None)
        self.partitions = DictList()
        self.map = dict()
        self.map[None] = self.NULL

    def compare(self, a, b):
        return value_compare(a.value, b.value)

    def getCanonicalPart(self, part):
        return self.getPartByKey(part.value)

    def getPartByKey(self, key):
        canonical = self.map.get(key, None)
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


class SimpleSetDomain(Domain):
    """
    DOMAIN IS A LIST OF OBJECTS, EACH WITH A value PROPERTY
    """

    def __new__(cls, **desc):
        return object.__new__(SimpleSetDomain)

    def __init__(self, **desc):
        Domain.__init__(self, **desc)
        desc = wrap(desc)

        self.type = "set"
        self.order = {}
        self.NULL = Dict(value=None)
        self.partitions = DictList()

        self.esfilter = None
        set_default(self, desc)
        if isinstance(self.key, set):
            Log.error("problem")

        if isinstance(desc.partitions[0], basestring):
            # ASSMUE PARTS ARE STRINGS, CONVERT TO REAL PART OBJECTS
            self.key = ("value", )
            self.order[None]=len(desc.partitions)
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
        elif desc.partitions and isinstance(desc.partitions[0][desc.key], dict):
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
            #EVERY PART HAS AN esfilter DEFINED, SO USE THEM
            for i, p in enumerate(self.partitions):
                p.dataIndex=i

        else:
            Log.error("Can not hanldle")

        self.label = nvl(self.label, "name")

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
            output = self.order.get(key, None)
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

    def getEnd(self, part):
        if self.value:
            return part[self.value]
        else:
            return part

    def getLabel(self, part):
        return part[self.label]


class SetDomain(Domain):

    def __new__(cls, **desc):
        return object.__new__(SetDomain)

    def __init__(self, **desc):
        Domain.__init__(self, **desc)
        desc = wrap(desc)

        self.type = "set"
        self.order = {}
        self.NULL = Dict(value=None)
        self.partitions = DictList()

        self.esfilter = None
        set_default(self, desc)
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
        elif desc.partitions and isinstance(desc.partitions[0][desc.key], dict):
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

        self.label = nvl(self.label, "name")

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
            output = self.order.get(key, None)
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

    def getEnd(self, part):
        if self.value:
            return part[self.value]
        else:
            return part

    def getLabel(self, part):
        return part[self.label]



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
    if value == None:
        return False
    return True if keyword_pattern.match(value) else False


