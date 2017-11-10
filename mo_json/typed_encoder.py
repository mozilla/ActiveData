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
import time
from collections import deque, Mapping
from datetime import datetime, date, timedelta
from decimal import Decimal

from future.utils import text_type

from mo_dots import Data, FlatList, NullType, split_field, join_field
from mo_json import ESCAPE_DCT, float2json
from mo_json.encoder import pretty_json, problem_serializing, _repr, UnicodeBuilder, COMMA
from mo_logs import Log
from mo_logs.strings import utf82unicode
from mo_times.dates import Date
from mo_times.durations import Duration


TYPE_PREFIX = "__type__"
BOOLEAN_TYPE = TYPE_PREFIX+"boolean"
NUMBER_TYPE = TYPE_PREFIX+"number"
STRING_TYPE = TYPE_PREFIX+"string"
NESTED_TYPE = TYPE_PREFIX+"nested"
EXISTS_TYPE = TYPE_PREFIX+"exists"

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
        except Exception as f:
            Log.error("problem serializing object", f)


def _typed_encode(value, _buffer):
    try:
        if value is None:
            append(_buffer, u'{}')
            return
        elif value is True:
            append(_buffer, u'{"'+BOOLEAN_TYPE+'": true}')
            return
        elif value is False:
            append(_buffer, u'{"'+BOOLEAN_TYPE+'": false}')
            return

        _type = value.__class__
        if _type in (dict, Data):
            if value:
                _dict2json(value, _buffer)
            else:
                append(_buffer, u'{'+EXISTS_TYPE+': 1}')
        elif _type is str:
            append(_buffer, u'{'+STRING_TYPE+': "')
            try:
                v = utf82unicode(value)
            except Exception as e:
                raise problem_serializing(value, e)

            for c in v:
                append(_buffer, ESCAPE_DCT.get(c, c))
            append(_buffer, u'"}')
        elif _type is text_type:
            append(_buffer, u'{'+STRING_TYPE+': "')
            for c in value:
                append(_buffer, ESCAPE_DCT.get(c, c))
            append(_buffer, u'"}')
        elif _type in (int, long, Decimal):
            append(_buffer, u'{'+NUMBER_TYPE+': ')
            append(_buffer, float2json(value))
            append(_buffer, u'}')
        elif _type is float:
            append(_buffer, u'{'+NUMBER_TYPE+': ')
            append(_buffer, float2json(value))
            append(_buffer, u'}')
        elif _type in (set, list, tuple, FlatList):
            if any(isinstance(v, (Mapping, set, list, tuple, FlatList)) for v in value):
                append(_buffer, u'{'+NESTED_TYPE+': ')
                _list2json(value, _buffer)
                append(_buffer, u'}')
            else:
                # ALLOW PRIMITIVE MULTIVALUES
                _list2json(value, _buffer)
        elif _type is date:
            append(_buffer, u'{'+NUMBER_TYPE+': ')
            append(_buffer, float2json(time.mktime(value.timetuple())))
            append(_buffer, u'}')
        elif _type is datetime:
            append(_buffer, u'{'+NUMBER_TYPE+': ')
            append(_buffer, float2json(time.mktime(value.timetuple())))
            append(_buffer, u'}')
        elif _type is Date:
            append(_buffer, u'{'+NUMBER_TYPE+': ')
            append(_buffer, float2json(time.mktime(value.value.timetuple())))
            append(_buffer, u'}')
        elif _type is timedelta:
            append(_buffer, u'{'+NUMBER_TYPE+': ')
            append(_buffer, float2json(value.total_seconds()))
            append(_buffer, u'}')
        elif _type is Duration:
            append(_buffer, u'{'+NUMBER_TYPE+': ')
            append(_buffer, float2json(value.seconds))
            append(_buffer, u'}')
        elif _type is NullType:
            append(_buffer, u"null")
        elif hasattr(value, '__json__'):
            j = value.__json__()
            t = json2typed(j)
            append(_buffer, t)
        elif hasattr(value, '__iter__'):
            append(_buffer, u'{'+NESTED_TYPE+': ')
            _iter2json(value, _buffer)
            append(_buffer, u'}')
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
            sep = COMMA
            _typed_encode(v, _buffer)
        append(_buffer, u"]")


def _iter2json(value, _buffer):
    append(_buffer, u"[")
    sep = u""
    for v in value:
        append(_buffer, sep)
        sep = COMMA
        _typed_encode(v, _buffer)
    append(_buffer, u"]")


def _dict2json(value, _buffer):
    prefix = u'{'+EXISTS_TYPE+': 1, '
    for k, v in value.iteritems():
        if v == None or v == "":
            continue
        append(_buffer, prefix)
        prefix = u", "
        if isinstance(k, str):
            k = utf82unicode(k)
        if not isinstance(k, text_type):
            Log.error("Expecting property name to be a string")
        append(_buffer, json.dumps(encode_property(k)))
        append(_buffer, u": ")
        _typed_encode(v, _buffer)
    if prefix == u", ":
        append(_buffer, u'}')
    else:
        append(_buffer, u'{'+EXISTS_TYPE+': 1}')


VALUE = 0
PRIMITIVE = 1
BEGIN_OBJECT = 2
OBJECT = 3
KEYWORD = 4
STRING = 6
ESCAPE = 5



def json2typed(json):
    """
    every ': {' gets converted to ': {'+EXISTS_TYPE+': ".", '
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
                append(output, '{'+EXISTS_TYPE+': 1')
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
                append(output, '{'+STRING_TYPE+': ')
            else:
                mode = PRIMITIVE
                append(output, '{'+NUMBER_TYPE+': ')
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
                append(output, ',')
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


def untype_path(encoded):
    if encoded.startswith(".."):
        remainder = encoded.lstrip(".")
        back = len(encoded) - len(remainder) - 1
        return ("." * back) + join_field(decode_property(c) for c in split_field(remainder) if not c.startswith(TYPE_PREFIX))
    else:
        return join_field(decode_property(c) for c in split_field(encoded) if not c.startswith(TYPE_PREFIX))


def nest_free_path(encoded):
    if encoded.startswith(".."):
        encoded = encoded.lstrip(".")
        if not encoded:
            encoded = "."

    #     remainder = encoded.lstrip(".")
    #     back = len(encoded) - len(remainder) - 1
    #     return ("." * back) + join_field(decode_property(c) for c in split_field(remainder) if c != NESTED_TYPE)
    # else:
    return join_field(decode_property(c) for c in split_field(encoded) if c != NESTED_TYPE)


def untyped(value):
    return _untype(value)


def _untype(value):
    if isinstance(value, Mapping):
        output = {}

        for k, v in value.items():
            if k == EXISTS_TYPE:
                continue
            elif k.startswith('$'):
                return v
            else:
                output[decode_property(k)] = _untype(v)
        return output
    elif isinstance(value, list):
        return [_untype(v) for v in value]
    else:
        return value
