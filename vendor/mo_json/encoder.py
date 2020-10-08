# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

import json
import math
import time
from datetime import date, datetime, timedelta
from decimal import Decimal
from json.encoder import encode_basestring
from math import floor

from mo_dots import Data, FlatList, Null, NullType, SLOT, is_data, is_list, unwrap
from mo_future import PYPY, binary_type, is_binary, is_text, long, sort_using_key, text, utf8_json_encoder, xrange
from mo_json import ESCAPE_DCT, float2json, scrub
from mo_logs import Except
from mo_logs.strings import quote
from mo_times import Timer
from mo_times.dates import Date
from mo_times.durations import Duration

json_decoder = json.JSONDecoder().decode
_get = object.__getattribute__

_ = Except

# THIS FILE EXISTS TO SERVE AS A FAST REPLACEMENT FOR JSON ENCODING
# THE DEFAULT JSON ENCODERS CAN NOT HANDLE A DIVERSITY OF TYPES *AND* BE FAST
#
# 1) WHEN USING cPython, WE HAVE NO COMPILER OPTIMIZATIONS: THE BEST STRATEGY IS TO
# CONVERT THE MEMORY STRUCTURE TO STANDARD TYPES AND SEND TO THE INSANELY FAST
#    DEFAULT JSON ENCODER
# 2) WHEN USING PYPY, WE USE CLEAR-AND-SIMPLE PROGRAMMING SO THE OPTIMIZER CAN DO
#    ITS JOB.  ALONG WITH THE UnicodeBuilder WE GET NEAR C SPEEDS

COMMA = u","
QUOTE = u'"'
COLON = u":"
QUOTE_COLON = QUOTE + COLON
COMMA_QUOTE = COMMA + QUOTE

PRETTY_COMMA = u", "
PRETTY_COLON = u": "

if PYPY:
    # UnicodeBuilder IS ABOUT 2x FASTER THAN list()
    from __pypy__.builders import UnicodeBuilder
else:
    class UnicodeBuilder(list):
        def __init__(self, length=None):
            list.__init__(self)

        def build(self):
            return u"".join(self)

append = UnicodeBuilder.append

_dealing_with_problem = False


def pypy_json_encode(value, pretty=False):
    """
    pypy DOES NOT OPTIMIZE GENERATOR CODE WELL
    """
    global _dealing_with_problem
    if pretty:
        return pretty_json(value)

    try:
        _buffer = UnicodeBuilder(2048)
        _value2json(value, _buffer)
        output = _buffer.build()
        return output
    except Exception as e:
        # THE PRETTY JSON WILL PROVIDE MORE DETAIL ABOUT THE SERIALIZATION CONCERNS
        from mo_logs import Log

        if _dealing_with_problem:
            Log.error("Serialization of JSON problems", e)
        else:
            Log.warning("Serialization of JSON problems", e)
        _dealing_with_problem = True
        try:
            return pretty_json(value)
        except Exception as f:
            Log.error("problem serializing object", f)
        finally:
            _dealing_with_problem = False


class cPythonJSONEncoder(object):
    def __init__(self, sort_keys=True):
        object.__init__(self)

        self.encoder = utf8_json_encoder

    def encode(self, value, pretty=False):
        if pretty:
            return pretty_json(value)

        try:
            with Timer("scrub", too_long=0.1):
                scrubbed = scrub(value)
            param = {"size": 0}
            with Timer("encode {{size}} characters", param=param, too_long=0.1):
                output = text(self.encoder(scrubbed))
                param["size"] = len(output)
                return output
        except Exception as e:
            from mo_logs.exceptions import Except
            from mo_logs import Log

            e = Except.wrap(e)
            Log.warning("problem serializing {{type}}", type=text(repr(value)), cause=e)
            raise e


def _value2json(value, _buffer):
    try:
        _class = value.__class__
        if value is None:
            append(_buffer, u"null")
            return
        elif value is True:
            append(_buffer, u"true")
            return
        elif value is False:
            append(_buffer, u"false")
            return

        type = value.__class__
        if type is binary_type:
            append(_buffer, QUOTE)
            try:
                v = value.decode('utf8')
            except Exception as e:
                problem_serializing(value, e)

            for c in v:
                append(_buffer, ESCAPE_DCT.get(c, c))
            append(_buffer, QUOTE)
        elif type is text:
            append(_buffer, QUOTE)
            for c in value:
                append(_buffer, ESCAPE_DCT.get(c, c))
            append(_buffer, QUOTE)
        elif type is dict:
            if not value:
                append(_buffer, u"{}")
            else:
                _dict2json(value, _buffer)
            return
        elif type is Data:
            d = _get(value, SLOT)  # MIGHT BE A VALUE NOT A DICT
            _value2json(d, _buffer)
            return
        elif type in (int, long, Decimal):
            append(_buffer, text(value))
        elif type is float:
            if math.isnan(value) or math.isinf(value):
                append(_buffer, u'null')
            else:
                append(_buffer, float2json(value))
        elif type in (set, list, tuple, FlatList):
            _list2json(value, _buffer)
        elif type is date:
            append(_buffer, float2json(time.mktime(value.timetuple())))
        elif type is datetime:
            append(_buffer, float2json(time.mktime(value.timetuple())))
        elif type is Date:
            append(_buffer, float2json(value.unix))
        elif type is timedelta:
            append(_buffer, float2json(value.total_seconds()))
        elif type is Duration:
            append(_buffer, float2json(value.seconds))
        elif type is NullType:
            append(_buffer, u"null")
        elif is_data(value):
            if not value:
                append(_buffer, u"{}")
            else:
                _dict2json(value, _buffer)
            return
        elif hasattr(value, '__data__'):
            d = value.__data__()
            _value2json(d, _buffer)
        elif hasattr(value, '__json__'):
            j = value.__json__()
            append(_buffer, j)
        elif hasattr(value, '__iter__'):
            _iter2json(value, _buffer)
        else:
            from mo_logs import Log

            Log.error(text(repr(value)) + " is not JSON serializable")
    except Exception as e:
        from mo_logs import Log

        Log.error(text(repr(value)) + " is not JSON serializable", cause=e)


def _list2json(value, _buffer):
    if not value:
        append(_buffer, u"[]")
    else:
        sep = u"["
        for v in value:
            append(_buffer, sep)
            sep = COMMA
            _value2json(v, _buffer)
        append(_buffer, u"]")


def _iter2json(value, _buffer):
    append(_buffer, u"[")
    sep = u""
    for v in value:
        append(_buffer, sep)
        sep = COMMA
        _value2json(v, _buffer)
    append(_buffer, u"]")


def _dict2json(value, _buffer):
    try:
        prefix = u"{\""
        for k, v in value.items():
            append(_buffer, prefix)
            prefix = COMMA_QUOTE
            if is_binary(k):
                k = k.decode('utf8')
            for c in k:
                append(_buffer, ESCAPE_DCT.get(c, c))
            append(_buffer, QUOTE_COLON)
            _value2json(v, _buffer)
        append(_buffer, u"}")
    except Exception as e:
        from mo_logs import Log

        Log.error(text(repr(value)) + " is not JSON serializable", cause=e)


ARRAY_ROW_LENGTH = 80
ARRAY_ITEM_MAX_LENGTH = 30
ARRAY_MAX_COLUMNS = 20
ARRAY_MIN_ITEMS = 20  # DO NOT ATTEMPT ARRAY FORMATTING IF TOO FEW ITEMS
INDENT = "    "


def pretty_json(value):
    try:
        if value is False:
            return "false"
        elif value is True:
            return "true"
        elif value == None:
            return "null"
        elif is_data(value):
            try:
                value = unwrap(value)
                items = sort_using_key(value.items(), lambda r: r[0])
                values = [encode_basestring(k) + PRETTY_COLON + pretty_json(v) for k, v in items if v != None]
                if not values:
                    return "{}"
                elif len(values) == 1:
                    return "{" + values[0] + "}"
                else:
                    return "{\n" + ",\n".join(indent(v) for v in values) + "\n}"
            except Exception as e:
                from mo_logs import Log
                from mo_math import OR

                if OR(not is_text(k) for k in value.keys()):
                    Log.error(
                        "JSON must have string keys: {{keys}}:",
                        keys=[k for k in value.keys()],
                        cause=e
                    )

                Log.error(
                    "problem making dict pretty: keys={{keys}}:",
                    keys=[k for k in value.keys()],
                    cause=e
                )
        elif value.__class__ in (binary_type, text):
            if is_binary(value):
                value = value.decode('utf8')
            try:
                if "\n" in value and value.strip():
                    return pretty_json({"$concat": value.split("\n"), "separator": "\n"})
                else:
                    return quote(value)
            except Exception as e:
                from mo_logs import Log

                try:
                    Log.note("try explicit convert of string with length {{length}}", length=len(value))
                    acc = [QUOTE]
                    for c in value:
                        try:
                            try:
                                c2 = ESCAPE_DCT[c]
                            except Exception:
                                c2 = c
                            c3 = text(c2)
                            acc.append(c3)
                        except BaseException:
                            pass
                            # Log.warning("odd character {{ord}} found in string.  Ignored.",  ord= ord(c)}, cause=g)
                    acc.append(QUOTE)
                    output = u"".join(acc)
                    Log.note("return value of length {{length}}", length=len(output))
                    return output
                except BaseException as f:
                    Log.warning("can not convert {{type}} to json", type=f.__class__.__name__, cause=f)
                    return "null"
        elif is_list(value):
            if not value:
                return "[]"

            if ARRAY_MAX_COLUMNS == 1:
                return "[\n" + ",\n".join([indent(pretty_json(v)) for v in value]) + "\n]"

            if len(value) == 1:
                j = pretty_json(value[0])
                return "[" + j + "]"

            js = [pretty_json(v) for v in value]
            max_len = max(*[len(j) for j in js])
            if len(js) < ARRAY_MIN_ITEMS and max_len <= ARRAY_ITEM_MAX_LENGTH and max(*[j.find("\n") for j in js]) == -1:
                # ALL TINY VALUES
                num_columns = max(1, min(ARRAY_MAX_COLUMNS, int(floor((ARRAY_ROW_LENGTH + 2.0) / float(max_len + 2)))))  # +2 TO COMPENSATE FOR COMMAS
                if len(js) <= num_columns:  # DO NOT ADD \n IF ONLY ONE ROW
                    return "[" + PRETTY_COMMA.join(js) + "]"
                if num_columns == 1:  # DO NOT rjust IF THERE IS ONLY ONE COLUMN
                    return "[\n" + ",\n".join([indent(pretty_json(v)) for v in value]) + "\n]"

                content = ",\n".join(
                    PRETTY_COMMA.join(j.rjust(max_len) for j in js[r:r + num_columns])
                    for r in xrange(0, len(js), num_columns)
                )
                return "[\n" + indent(content) + "\n]"

            pretty_list = js

            output = ["[\n"]
            for i, p in enumerate(pretty_list):
                try:
                    if i > 0:
                        output.append(",\n")
                    output.append(indent(p))
                except Exception:
                    from mo_logs import Log

                    Log.warning("problem concatenating string of length {{len1}} and {{len2}}",
                        len1=len("".join(output)),
                        len2=len(p)
                    )
            output.append("\n]")
            try:
                return "".join(output)
            except Exception as e:
                from mo_logs import Log

                Log.error("not expected", cause=e)
        elif hasattr(value, '__data__'):
            d = value.__data__()
            return pretty_json(d)
        elif hasattr(value, '__json__'):
            j = value.__json__()
            if j == None:
                return "   null   "  # TODO: FIND OUT WHAT CAUSES THIS
            return pretty_json(json_decoder(j))
        elif scrub(value) is None:
            return "null"
        elif hasattr(value, '__iter__'):
            return pretty_json(list(value))
        elif hasattr(value, '__call__'):
            return "null"
        else:
            try:
                if int(value) == value:
                    return text(int(value))
            except Exception:
                pass

            try:
                if float(value) == value:
                    return text(float(value))
            except Exception:
                pass

            return pypy_json_encode(value)

    except Exception as e:
        problem_serializing(value, e)


def problem_serializing(value, e=None):
    """
    THROW ERROR ABOUT SERIALIZING
    """
    from mo_logs import Log

    try:
        typename = type(value).__name__
    except Exception:
        typename = "<error getting name>"

    try:
        rep = text(repr(value))
    except Exception as _:
        rep = None

    if rep == None:
        Log.error(
            "Problem turning value of type {{type}} to json",
            type=typename,
            cause=e
        )
    else:
        Log.error(
            "Problem turning value ({{value}}) of type {{type}} to json",
            value=rep,
            type=typename,
            cause=e
        )


def indent(value, prefix=INDENT):
    try:
        content = value.rstrip()
        suffix = value[len(content):]
        lines = content.splitlines()
        return prefix + (u"\n" + prefix).join(lines) + suffix
    except Exception as e:
        raise Exception(u"Problem with indent of value (" + e.message + u")\n" + value)


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


def datetime2milli(d, type):
    try:
        if type == datetime:
            diff = d - datetime(1970, 1, 1)
        else:
            diff = d - date(1970, 1, 1)

        return long(diff.total_seconds()) * long(1000) + long(diff.microseconds / 1000)
    except Exception as e:
        problem_serializing(d, e)


def unicode_key(key):
    """
    CONVERT PROPERTY VALUE TO QUOTED NAME OF SAME
    """
    if not isinstance(key, (text, binary_type)):
        from mo_logs import Log
        Log.error("{{key|quote}} is not a valid key", key=key)
    return quote(text(key))


if PYPY:
    json_encoder = pypy_json_encode
else:
    json_encoder = cPythonJSONEncoder().encode


