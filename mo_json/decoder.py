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

from mo_dots import Null, FlatList, wrap, unwrap
from mo_json.encoder import UnicodeBuilder, use_pypy, pypy_json_encode

DEBUG = False

# PARSE MODES
ARRAY = 1   # PARSING INSIDE AN ARRAY
VALUE = 3   # PARSING PROPERTY VALUE
OBJECT = 4  # PARSING PROPERTY NAME


def decode(json):
    """
    THIS IS CURRENTLY 50% SLOWER THAN PyPy DEFAULT IMPLEMENTATION

    THE INTENT IS TO NEVER ACTUALLY PARSE ARRAYS OF PRIMITIVE VALUES, RATHER FIND
    THE START AND END OF THOSE ARRAYS AND SIMPLY STRING COPY THEM TO THE
    INEVITABLE JSON OUTPUT
    """
    var = ""
    curr = FlatList()
    mode = ARRAY
    stack = FlatList()
    # FIRST PASS SIMPLY GETS STRUCTURE
    i = 0
    while i < len(json):
        c = json[i]
        i += 1
        if mode == ARRAY:
            if c in [" ", "\t", "\n", "\r", ","]:
                pass
            elif c == "]":
                curr = stack.pop()
                if isinstance(curr, Mapping):
                    mode = OBJECT
                else:
                    mode = ARRAY
            elif c == "[":
                i, arr = jump_array(i, json)
                if arr is None:
                    arr = []
                    stack.append(curr)
                    curr.append(arr)
                    curr = arr
                    mode = ARRAY
                else:
                    curr.append(arr)
            elif c == "{":
                obj = {}
                stack.append(curr)
                curr.append(obj)
                curr = obj
                mode = OBJECT
            elif c == "\"":
                i, val = fast_parse_string(i, json)
                curr.children.append(val)
            else:
                i, val = parse_const(i, json)
        elif mode == OBJECT:
            if c in [" ", "\t", "\n", "\r", ","]:
                pass
            elif c == ":":
                mode = VALUE
            elif c == "}":
                curr = stack.pop()
                if isinstance(curr, Mapping):
                    mode = OBJECT
                else:
                    mode = ARRAY
            elif c == "\"":
                i, var = fast_parse_string(i, json)
        elif mode == VALUE:
            if c in [" ", "\t", "\n", "\r"]:
                pass
            elif c == "}":
                curr = stack.pop()
                if isinstance(curr, Mapping):
                    mode = OBJECT
                else:
                    mode = ARRAY
            elif c == "[":
                i, arr = jump_array(i, json)
                if arr is None:
                    arr = []
                    stack.append(curr)
                    curr[var] = arr
                    curr = arr
                    mode = ARRAY
                else:
                    curr[var] = arr
                    mode = OBJECT
            elif c == "{":
                obj = {}
                stack.append(curr)
                curr[var] = obj
                curr = obj
                mode = OBJECT
            elif c == "\"":
                i, val = fast_parse_string(i, json)
                curr[var] = val
                mode = OBJECT
            else:
                i, val = parse_const(i, json)
                curr[var] = val
                mode = OBJECT

    return curr[0]




def fast_parse_string(i, json):
    simple = True
    j = i
    while True:
        c = json[j]
        j += 1
        if c == "\"":
            if simple:
                return j, json[i:j-1]
            else:
                return parse_string(i, json)
        elif c == "\\":
            simple = False
            c = json[j]
            if c == "u":
                j += 5
            elif c in ["\"", "\\", "/", "b", "n", "f", "n", "t"]:
                j += 1
            else:
                pass


ESC = {
    "\"": "\"",
    "\\": "\\",
    "/": "/",
    "b": "\b",
    "r": "\r",
    "f": "\f",
    "n": "\n",
    "t": "\t"
}


def parse_string(i, json):
    j = i
    output = UnicodeBuilder()
    while True:
        c = json[j]
        if c == "\"":
            return j + 1, output.build()
        elif c == "\\":
            j += 1
            c = json[j]
            if c == "u":
                n = json[j:j + 4].decode('hex').decode('utf-8')
                output.append(n)
                j += 4
            else:
                try:
                    output.append(ESC[c])
                except Exception as e:
                    output.append("\\")
                    output.append(c)
        else:
            output.append(c)
        j += 1



def parse_array(i, json):
    """
    ARRAY OF PRIMITIVES ARE SKIPPED, THIS IS WHERE WE PARSE THEM
    """
    output = []
    val = None
    while True:
        c = json[i]
        i += 1
        if c in [" ", "\n", "\r", "\t"]:
            pass
        elif c == ",":
            output.append(val)
            val = Null
        elif c == "]":
            if val is not None:
                output.append(val)
            return i, output
        elif c == "[":
            i, val = parse_array(i, json)
        elif c == "\"":
            i, val = parse_string(i, json)
        else:
            i, val = parse_const(i, json)


def jump_string(i, json):
    while True:
        c = json[i]
        i += 1
        if c == "\"":
            return i
        elif c == "\\":
            c = json[i]
            if c == "u":
                i += 5
            elif c in ["\"", "\\", "/", "b", "n", "f", "n", "t"]:
                i += 1
            else:
                pass


def jump_array(i, json):
    j = i
    empty = True
    depth = 0
    while True:
        c = json[j]
        j += 1
        if c == "{":
            return i, None
        elif c == "[":
            depth += 1
        elif c == "]":
            if depth == 0:
                if empty:
                    return j, []
                else:
                    return j, JSONList(json, i-1, j)
            else:
                depth -= 1
        elif c == "\"":
            empty = False
            j = jump_string(j, json)
        elif c not in [" ", "\t", "\r", "\n"]:
            empty = False

def parse_const(i, json):
    try:
        j = i
        mode = int
        while True:
            c = json[j]
            if c in [" ", "\t", "\n", "\r", ",", "}", "]"]:
                const = json[i-1:j]
                try:
                    val = {
                        "0": 0,
                        "-1": -1,
                        "1": 1,
                        "true": True,
                        "false": False,
                        "null": None
                    }[const]
                except Exception:
                    val = mode(const)

                return j, val
            elif c in [".", "e", "E"]:
                mode = float
            j += 1
    except Exception as e:
        from mo_logs import Log

        Log.error("Can not parse const", e)

class JSONList(object):
    def __init__(self, json, s, e):
        self.json = json
        self.start = s
        self.end = e
        self.list = None

    def _convert(self):
        if self.list is None:
            i, self.list = parse_array(self.start+1, self.json)

    def __getitem__(self, index):
        self._convert()
        if isinstance(index, slice):
            # IMPLEMENT FLAT SLICES (for i not in range(0, len(self)): assert self[i]==None)
            if index.step is not None:
                from mo_logs import Log

                Log.error("slice step must be None, do not know how to deal with values")
            length = len(self.list)

            i = index.start
            i = min(max(i, 0), length)
            j = index.stop
            if j is None:
                j = length
            else:
                j = max(min(j, length), 0)
            return FlatList(self.list[i:j])

        if index < 0 or len(self.list) <= index:
            return Null
        return wrap(self.list[index])

    def __setitem__(self, i, y):
        self._convert()
        self.json = None
        self.list[i] = unwrap(y)

    def __iter__(self):
        self._convert()
        return (wrap(v) for v in self.list)

    def __contains__(self, item):
        self._convert()
        return list.__contains__(self.list, item)

    def append(self, val):
        self._convert()
        self.json = None
        self.list.append(unwrap(val))
        return self

    def __str__(self):
        return self.json[self.start:self.end]

    def __len__(self):
        self._convert()
        return self.list.__len__()

    def __getslice__(self, i, j):
        from mo_logs import Log

        Log.error("slicing is broken in Python 2.7: a[i:j] == a[i+len(a), j] sometimes.  Use [start:stop:step]")

    def copy(self):
        if self.list is not None:
            return list(self.list)
        return JSONList(self.json, self.start, self.end)

    def remove(self, x):
        self._convert()
        self.json = None
        self.list.remove(x)
        return self

    def extend(self, values):
        self._convert()
        self.json = None
        for v in values:
            self.list.append(unwrap(v))
        return self

    def pop(self):
        self._convert()
        self.json = None
        return wrap(self.list.pop())

    def __add__(self, value):
        self._convert()
        output = list(self.list)
        output.extend(value)
        return FlatList(vals=output)

    def __or__(self, value):
        self._convert()
        output = list(self.list)
        output.append(value)
        return FlatList(vals=output)

    def __radd__(self, other):
        self._convert()
        output = list(other)
        output.extend(self.list)
        return FlatList(vals=output)

    def right(self, num=None):
        """
        WITH SLICES BEING FLAT, WE NEED A SIMPLE WAY TO SLICE FROM THE RIGHT
        """
        self._convert()
        if num == None:
            return FlatList([self.list[-1]])
        if num <= 0:
            return Null
        return FlatList(self.list[-num])

    def not_right(self, num):
        """
        WITH SLICES BEING FLAT, WE NEED A SIMPLE WAY TO SLICE FROM THE LEFT [:-num:]
        """
        self._convert()
        if num == None:
            return FlatList([self.list[:-1:]])
        if num <= 0:
            return Null
        return FlatList(self.list[:-num:])

    def last(self):
        """
        RETURN LAST ELEMENT IN FlatList
        """
        self._convert()
        if self.list:
            return wrap(self.list[-1])
        return Null

    def map(self, oper, includeNone=True):
        self._convert()
        if includeNone:
            return FlatList([oper(v) for v in self.list])
        else:
            return FlatList([oper(v) for v in self.list if v != None])


if use_pypy:
    json_decoder = decode
else:
    import json

    builtin_json_decoder = json.JSONDecoder().decode
    json_decoder = builtin_json_decoder

if DEBUG:
    json_decoder = decode
