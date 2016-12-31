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

import json
from types import GeneratorType

from pyLibrary.debugs.logs import Log
from pyDots import split_field
from pyLibrary.env.files import File
from pyLibrary.env.http import MIN_READ_SIZE


DEBUG = False
WHITESPACE = b" \n\r\t"
CLOSE = {
    b"{": b"}",
    b"[": "]"
}
NO_VARS = set()

json_decoder = json.JSONDecoder().decode


def parse(json, path, expected_vars=NO_VARS):
    """
    INTENDED TO TREAT JSON AS A STREAM; USING MINIMAL MEMORY WHILE IT ITERATES
    THROUGH THE STRUCTURE.  ASSUMING THE JSON IS LARGE, AND HAS A HIGH LEVEL
    ARRAY STRUCTURE, IT WILL yield EACH OBJECT IN THAT ARRAY.  NESTED ARRAYS
    ARE HANDLED BY REPEATING THE PARENT PROPERTIES FOR EACH MEMBER OF THE
    NESTED ARRAY. DEEPER NESTED PROPERTIES ARE TREATED AS PRIMITIVE VALUES;
    THE STANDARD JSON DECODER IS USED.

    LARGE MANY-PROPERTY OBJECTS CAN BE HANDLED BY `items()`

    :param json: SOME STRING-LIKE STRUCTURE THAT CAN ASSUME WE LOOK AT ONE
                 CHARACTER AT A TIME, IN ORDER
    :param path: AN ARRAY OF DOT-SEPARATED STRINGS INDICATING THE
                 NESTED ARRAY BEING ITERATED.
    :param expected_vars: REQUIRED PROPERTY NAMES, USED TO DETERMINE IF
                          MORE-THAN-ONE PASS IS REQUIRED
    :return: RETURNS AN ITERATOR OVER ALL OBJECTS FROM NESTED path IN LEAF FORM
    """
    if hasattr(json, "read"):
        # ASSUME IT IS A STREAM
        temp = json
        def get_more():
            return temp.read(MIN_READ_SIZE)
        json = List_usingStream(get_more)
    elif hasattr(json, "__call__"):
        json = List_usingStream(json)
    elif isinstance(json, GeneratorType):
        json = List_usingStream(json.next)
    else:
        Log.error("Expecting json to be a stream, or a function that will return more bytes")


    def _decode(index, parent_path, path, name2index, expected_vars=NO_VARS):
        c, index = skip_whitespace(index)

        if not path:
            if c != b"[":
                # TREAT VALUE AS SINGLE-VALUE ARRAY
                yield _decode_token(index, c, parent_path, path, name2index, None, expected_vars)
            else:
                c, index = skip_whitespace(index)
                if c == b']':
                    return  # EMPTY ARRAY

                while True:
                    value, index = _decode_token(index, c, parent_path, path, name2index, None, expected_vars)
                    c, index = skip_whitespace(index)
                    if c == b']':
                        yield value, index
                        return
                    elif c == b',':
                        c, index = skip_whitespace(index)
                        yield value, index

        else:
            if c != b'{':
                Log.error("Expecting all objects to at least have {{path}}", path=path[0])

            for j, i in _decode_object(index, parent_path, path, name2index, expected_vars=expected_vars):
                yield j, i

    def _decode_token(index, c, full_path, path, name2index, destination, expected_vars):
        if c == b'{':
            if not expected_vars:
                index = jump_to_end(index, c)
                value = None
            elif expected_vars[0] == ".":
                json.mark(index-1)
                index = jump_to_end(index, c)
                value = json_decoder(json.release(index).decode("utf8"))
            else:
                count = 0
                for v, i in _decode_object(index, full_path, path, name2index, destination, expected_vars=expected_vars):
                    index = i
                    value = v
                    count += 1
                if count != 1:
                    Log.error("Expecting object, nothing nested")
        elif c == b'[':
            if not expected_vars:
                index = jump_to_end(index, c)
                value = None
            else:
                json.mark(index - 1)
                index = jump_to_end(index, c)
                value = json_decoder(json.release(index).decode("utf8"))
        else:
            if expected_vars and expected_vars[0] == ".":
                value, index = simple_token(index, c)
            else:
                index = jump_to_end(index, c)
                value = None

        return value, index

    def _decode_object(index, parent_path, path, name2index, destination=None, expected_vars=NO_VARS):
        """
        :param index:
        :param parent_path:  LIST OF PROPERTY NAMES
        :param path:         ARRAY OF (LIST OF PROPERTY NAMES)
        :param name2index:
        :param destination:
        :param expected_vars:
        :return:
        """
        if destination is None:
            destination = {}

        nested_done = False
        while True:
            c, index = skip_whitespace(index)
            if c == b',':
                continue
            elif c == b'"':
                name, index = simple_token(index, c)

                c, index = skip_whitespace(index)
                if c != b':':
                    Log.error("Expecting colon")
                c, index = skip_whitespace(index)

                child_expected = needed(name, expected_vars)
                if child_expected and nested_done:
                    Log.error("Expected property found after nested json.  Iteration failed.")

                full_path = parent_path + [name]
                if path and all(p == f for p, f in zip(path[0], full_path)):
                    # THE NESTED PROPERTY WE ARE LOOKING FOR
                    if len(path[0]) == len(full_path):
                        new_path = path[1:]
                    else:
                        new_path = path

                    nested_done = True
                    for j, i in _decode(index - 1, full_path, new_path, name2index, expected_vars=child_expected):
                        index = i
                        j = {name: j}
                        for k, v in destination.items():
                            j.setdefault(k, v)
                        yield j, index
                    continue

                if child_expected:
                    # SOME OTHER PROPERTY
                    value, index = _decode_token(index, c, full_path, path, name2index, None, expected_vars=child_expected)
                    destination[name] = value
                else:
                    # WE DO NOT NEED THIS VALUE
                    index = jump_to_end(index, c)
                    continue


            elif c == "}":
                break

        if not nested_done:
            yield destination, index

    def jump_to_end(index, c):
        """
        DO NOT PROCESS THIS JSON OBJECT, JUST RETURN WHERE IT ENDS
        """
        if c=='"':
            while True:
                c = json[index]
                index += 1
                if c == b'\\':
                    index += 1
                elif c == b'"':
                    break
            return index
        elif c not in b"[{":
            while True:
                c = json[index]
                index += 1
                if c in b',]}':
                    break
            return index - 1

        # OBJECTS AND ARRAYS ARE MORE INVOLVED
        stack = [None] * 1024
        stack[0] = CLOSE[c]
        i = 0  # FOR INDEXING THE STACK
        while True:
            c = json[index]
            index += 1

            if c == b'"':
                while True:
                    c = json[index]
                    index += 1
                    if c == b'\\':
                        index += 1
                    elif c == b'"':
                        break
            elif c in b'[{':
                i += 1
                stack[i] = CLOSE[c]
            elif c == stack[i]:
                i -= 1
                if i == -1:
                    return index  # FOUND THE MATCH!  RETURN
            elif c in b']}':
                Log.error("expecting {{symbol}}", symbol=stack[i])

    def simple_token(index, c):
        if c == b'"':
            json.mark(index - 1)
            while True:
                c = json[index]
                index += 1
                if c == b"\\":
                    index += 1
                elif c == b'"':
                    break
            return json_decoder(json.release(index).decode("utf8")), index
        elif c in b"{[":
            Log.error("Expecting a primitive value")
        elif c == b"t" and json.slice(index, index + 3) == "rue":
            return True, index + 3
        elif c == b"n" and json.slice(index, index + 3) == "ull":
            return None, index + 3
        elif c == b"f" and json.slice(index, index + 4) == "alse":
            return False, index + 4
        else:
            json.mark(index-1)
            while True:
                c = json[index]
                if c in b',]}':
                    break
                index += 1
            return float(json.release(index)), index

    def skip_whitespace(index):
        """
        RETURN NEXT NON-WHITESPACE CHAR, AND ITS INDEX
        """
        c = json[index]
        while c in WHITESPACE:
            index += 1
            c = json[index]
        return c, index + 1

    for j, i in _decode(0, [], map(split_field, listwrap(path)), {}, expected_vars=expected_vars):
        yield j



def listwrap(value):
    if value == None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def needed(name, required):
    """
    RETURN SUBSET IF name IN REQUIRED
    """
    output = []
    for r in required:
        if r==name:
            output.append(".")
        elif r.startswith(name+"."):
            output.append(r[len(name)+1:])
    return output


class List_usingStream(object):
    """
    EXPECTING A FUNCTION
    """
    def __init__(self, get_more_bytes):
        """
        get_more_bytes() SHOULD RETURN AN ARRAY OF BYTES OF ANY SIZE
        """
        if not hasattr(get_more_bytes, "__call__"):
            Log.error("Expecting a function that will return bytes")

        self.get_more = get_more_bytes
        self.start = 0
        self._mark = -1
        self.buffer = self.get_more()
        self.buffer_length = len(self.buffer)
        pass

    def __getitem__(self, index):
        offset = index - self.start
        try:
            return self.buffer[offset]
        except IndexError:
            pass

        if offset < 0:
            Log.error("Can not go in reverse on stream index=={{index}}", index=index)

        if self._mark == -1:
            self.start += self.buffer_length
            offset = index - self.start
            self.buffer = self.get_more()
            self.buffer_length = len(self.buffer)
            while self.buffer_length <= offset:
                more = self.get_more()
                self.buffer += more
                self.buffer_length = len(self.buffer)
            return self.buffer[offset]

        needless_bytes = self._mark - self.start
        if needless_bytes:
            self.start = self._mark
            offset = index - self.start
            self.buffer = self.buffer[needless_bytes:]
            self.buffer_length = len(self.buffer)

        while self.buffer_length <= offset:
            more = self.get_more()
            self.buffer += more
            self.buffer_length = len(self.buffer)

        try:
            return self.buffer[offset]
        except Exception, e:
            Log.error("error", cause=e)

    def slice(self, start, stop):
        self.mark(start)
        return self.release(stop)

    def mark(self, index):
        """
        KEEP THIS index IN MEMORY UNTIL release()
        """
        if index < self.start:
            Log.error("Can not go in reverse on stream")
        if self._mark != -1:
            Log.error("Not expected")
        self._mark = index

    def release(self, end):
        if self._mark == -1:
            Log.error("Must mark() this stream before release")

        end_offset = end - self.start
        while self.buffer_length < end_offset:
            self.buffer += self.get_more()
            self.buffer_length = len(self.buffer)

        output = self.buffer[self._mark - self.start:end_offset]
        self._mark = -1
        return output
