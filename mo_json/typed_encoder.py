# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import json
import re
import time
from collections import deque, Mapping
from datetime import datetime, date, timedelta
from decimal import Decimal

from mo_logs import Log
from mo_logs.strings import utf82unicode
from mo_times.dates import Date
from mo_times.durations import Duration
from mo_dots import Data, FlatList, NullType

from mo_json import ESCAPE_DCT, float2json
from mo_json.encoder import pretty_json, problem_serializing, _repr, UnicodeBuilder

json_decoder = json.JSONDecoder().decode
append = UnicodeBuilder.append


def typed_encode(value):
    """
    pypy DOES NOT OPTIMIZE GENERATOR CODE WELL
    """
    try:
        _buffer = UnicodeBuilder(1024)
        _typed_encode(value, _buffer)
        output = _buffer.build()
        return output
    except Exception as e:
        # THE PRETTY JSON WILL PROVIDE MORE DETAIL ABOUT THE SERIALIZATION CONCERNS
        from mo_logs import Log

        Log.warning("Serialization of JSON problems", e)
        try:
            return pretty_json(value)
        except Exception, f:
            Log.error("problem serializing object", f)


def _typed_encode(value, _buffer):
    try:
        if value is None:
            append(_buffer, u'{}')
            return
        elif value is True:
            append(_buffer, u'{"$boolean": true}')
            return
        elif value is False:
            append(_buffer, u'{"$boolean": false}')
            return

        _type = value.__class__
        if _type in (dict, Data):
            if value:
                _dict2json(value, _buffer)
            else:
                append(_buffer, u'{"$exists": "."}')
        elif _type is str:
            append(_buffer, u'{"$string": "')
            try:
                v = utf82unicode(value)
            except Exception as e:
                raise problem_serializing(value, e)

            for c in v:
                append(_buffer, ESCAPE_DCT.get(c, c))
            append(_buffer, u'"}')
        elif _type is unicode:
            append(_buffer, u'{"$string": "')
            for c in value:
                append(_buffer, ESCAPE_DCT.get(c, c))
            append(_buffer, u'"}')
        elif _type in (int, long, Decimal):
            append(_buffer, u'{"$number": ')
            append(_buffer, float2json(value))
            append(_buffer, u'}')
        elif _type is float:
            append(_buffer, u'{"$number": ')
            append(_buffer, float2json(value))
            append(_buffer, u'}')
        elif _type in (set, list, tuple, FlatList):
            _list2json(value, _buffer)
        elif _type is date:
            append(_buffer, u'{"$number": ')
            append(_buffer, float2json(time.mktime(value.timetuple())))
            append(_buffer, u'}')
        elif _type is datetime:
            append(_buffer, u'{"$number": ')
            append(_buffer, float2json(time.mktime(value.timetuple())))
            append(_buffer, u'}')
        elif _type is Date:
            append(_buffer, u'{"$number": ')
            append(_buffer, float2json(time.mktime(value.value.timetuple())))
            append(_buffer, u'}')
        elif _type is timedelta:
            append(_buffer, u'{"$number": ')
            append(_buffer, float2json(value.total_seconds()))
            append(_buffer, u'}')
        elif _type is Duration:
            append(_buffer, u'{"$number": ')
            append(_buffer, float2json(value.seconds))
            append(_buffer, u'}')
        elif _type is NullType:
            append(_buffer, u"null")
        elif hasattr(value, '__json__'):
            j = value.__json__()
            t = json2typed(j)
            append(_buffer, t)
        elif hasattr(value, '__iter__'):
            _iter2json(value, _buffer)
        else:
            from mo_logs import Log

            Log.error(_repr(value) + " is not JSON serializable")
    except Exception as e:
        from mo_logs import Log

        Log.error(_repr(value) + " is not JSON serializable", e)


def _list2json(value, _buffer):
    if not value:
        append(_buffer, u"[]")
    else:
        sep = u"["
        for v in value:
            append(_buffer, sep)
            sep = u", "
            _typed_encode(v, _buffer)
        append(_buffer, u"]")


def _iter2json(value, _buffer):
    append(_buffer, u"[")
    sep = u""
    for v in value:
        append(_buffer, sep)
        sep = u", "
        _typed_encode(v, _buffer)
    append(_buffer, u"]")


def _dict2json(value, _buffer):
    prefix = u'{"$exists": ".", '
    for k, v in value.iteritems():
        append(_buffer, prefix)
        prefix = u", "
        if isinstance(k, str):
            k = utf82unicode(k)
        if not isinstance(k, unicode):
            Log.error("Expecting property name to be a string")
        append(_buffer, json.dumps(encode_property(k)))
        append(_buffer, u": ")
        _typed_encode(v, _buffer)
    append(_buffer, u"}")


VALUE = 0
PRIMITIVE = 1
BEGIN_OBJECT = 2
OBJECT = 3
KEYWORD = 4
STRING = 6
ESCAPE = 5



def json2typed(json):
    """
    every ': {' gets converted to ': {"$exists": ".", '
    every ': <value>' gets converted to '{"$value": <value>}'
    """
    # MODE VALUES
    #

    context = deque()
    output = UnicodeBuilder(1024)
    mode = VALUE
    for c in json:
        if c in "\t\r\n ":
            append(output, c)
        elif mode == VALUE:
            if c == "{":
                context.append(mode)
                mode = BEGIN_OBJECT
                append(output, '{"$exists": "."')
                continue
            elif c == '[':
                context.append(mode)
                mode = VALUE
            elif c == ",":
                mode = context.pop()
                if mode != OBJECT:
                    context.append(mode)
                    mode = VALUE
            elif c in "]":
                mode = context.pop()
            elif c in "}":
                mode = context.pop()
                mode = context.pop()
            elif c == '"':
                context.append(mode)
                mode = STRING
                append(output, '{"$string": ')
            else:
                mode = PRIMITIVE
                append(output, '{"$number": ')
            append(output, c)
        elif mode == PRIMITIVE:
            if c == ",":
                append(output, '}')
                mode = context.pop()
                if mode == 0:
                    context.append(mode)
            elif c == "]":
                append(output, '}')
                mode = context.pop()
            elif c == "}":
                append(output, '}')
                mode = context.pop()
                mode = context.pop()
            append(output, c)
        elif mode == BEGIN_OBJECT:
            if c == '"':
                context.append(OBJECT)
                context.append(KEYWORD)
                mode = STRING
                append(output, ', ')
            elif c == "}":
                mode = context.pop()
            else:
                Log.error("not expected")
            append(output, c)
        elif mode == KEYWORD:
            append(output, c)
            if c == ':':
                mode = VALUE
            else:
                Log.error("Not expected")
        elif mode == STRING:
            append(output, c)
            if c == '"':
                mode = context.pop()
                if mode != KEYWORD:
                    append(output, '}')
            elif c == '\\':
                context.append(mode)
                mode = ESCAPE
        elif mode == ESCAPE:
            mode = context.pop()
            append(output, c)
        elif mode == OBJECT:
            if c == '"':
                context.append(mode)
                context.append(KEYWORD)
                mode = STRING
            elif c == ",":
                pass
            elif c == '}':
                mode = context.pop()
            else:
                Log.error("not expected")

            append(output, c)

    if mode == PRIMITIVE:
        append(output, "}")
    return output.build()


def encode_property(name):
    return name.replace(",", "\\,").replace(".", ",")


def decode_property(encoded):
    return encoded.replace("\\,", "\a").replace(",", ".").replace("\a", ",")


def untyped(value):
    return _untype(value)


def _untype(value):
    if isinstance(value, Mapping):
        output = {}

        for k,v in value.items():
            if k=="$exists":
                continue
            elif k.startswith("$'"):
                return v
            else:
                output[k]=_untype(v)
        return output
    elif isinstance(value, list):
        return [_untype(v) for v in value]
    else:
        Log.error("expected full typing")

