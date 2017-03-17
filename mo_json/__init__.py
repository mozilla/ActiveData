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

import math
import re

from collections import Mapping

from datetime import date, timedelta, datetime
from decimal import Decimal
from types import NoneType

from mo_dots import FlatList, NullType, Data, wrap_leaves, wrap, Null
from mo_dots.objects import DataObject
from mo_logs import Except, strings, Log
from mo_logs.strings import expand_template
from mo_times import Date, Duration

_get = object.__getattribute__


ESCAPE_DCT = {
    u"\\": u"\\\\",
    u"\"": u"\\\"",
    u"\b": u"\\b",
    u"\f": u"\\f",
    u"\n": u"\\n",
    u"\r": u"\\r",
    u"\t": u"\\t",
}
for i in range(0x20):
    ESCAPE_DCT.setdefault(chr(i), u'\\u{0:04x}'.format(i))

ESCAPE = re.compile(ur'[\x00-\x1f\\"\b\f\n\r\t]')


def replace(match):
    return ESCAPE_DCT[match.group(0)]


def quote(value):
    if value == None:
        return ""
    return "\"" + ESCAPE.sub(replace, value) + "\""


def float2json(value):
    """
    CONVERT NUMBER TO JSON STRING, WITH BETTER CONTROL OVER ACCURACY
    :param value: float, int, long, Decimal
    :return: unicode
    """
    if value == 0:
        return u'0'
    try:
        sign = "-" if value < 0 else ""
        value = abs(value)
        sci = value.__format__(".15e")
        mantissa, exp = sci.split("e")
        exp = int(exp)
        if 0 <= exp:
            digits = u"".join(mantissa.split("."))
            return sign+(digits[:1+exp]+u"."+digits[1+exp:].rstrip('0')).rstrip(".")
        elif -4 < exp:
            digits = ("0"*(-exp))+u"".join(mantissa.split("."))
            return sign+(digits[:1]+u"."+digits[1:].rstrip('0')).rstrip(".")
        else:
            return sign+mantissa.rstrip("0")+u"e"+unicode(exp)
    except Exception as e:
        from mo_logs import Log
        Log.error("not expected", e)


def scrub(value):
    """
    REMOVE/REPLACE VALUES THAT CAN NOT BE JSON-IZED
    """
    return _scrub(value, set())


def _scrub(value, is_done):
    type_ = value.__class__

    if type_ in (NoneType, NullType):
        return None
    elif type_ is unicode:
        value_ = value.strip()
        if value_:
            return value_
        else:
            return None
    elif type_ is float:
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    elif type_ in (int, long, bool):
        return value
    elif type_ in (date, datetime):
        return float(datetime2unix(value))
    elif type_ is timedelta:
        return value.total_seconds()
    elif type_ is Date:
        return float(value.unix)
    elif type_ is Duration:
        return float(value.seconds)
    elif type_ is str:
        return utf82unicode(value)
    elif type_ is Decimal:
        return float(value)
    elif type_ is Data:
        return _scrub(_get(value, '_dict'), is_done)
    elif isinstance(value, Mapping):
        _id = id(value)
        if _id in is_done:
            Log.warning("possible loop in structure detected")
            return '"<LOOP IN STRUCTURE>"'
        is_done.add(_id)

        output = {}
        for k, v in value.iteritems():
            if isinstance(k, basestring):
                pass
            elif hasattr(k, "__unicode__"):
                k = unicode(k)
            else:
                Log.error("keys must be strings")
            v = _scrub(v, is_done)
            if v != None or isinstance(v, Mapping):
                output[k] = v

        is_done.discard(_id)
        return output
    elif type_ in (tuple, list, FlatList):
        output = []
        for v in value:
            v = _scrub(v, is_done)
            output.append(v)
        return output
    elif type_ is type:
        return value.__name__
    elif type_.__name__ == "bool_":  # DEAR ME!  Numpy has it's own booleans (value==False could be used, but 0==False in Python.  DOH!)
        if value == False:
            return False
        else:
            return True
    elif hasattr(value, '__data__'):
        try:
            return _scrub(value.__data__(), is_done)
        except Exception as e:
            Log.error("problem with calling __json__()", e)
    elif hasattr(value, 'co_code') or hasattr(value, "f_locals"):
        return None
    elif hasattr(value, '__iter__'):
        output = []
        for v in value:
            v = _scrub(v, is_done)
            output.append(v)
        return output
    elif hasattr(value, '__call__'):
        return repr(value)
    else:
        return _scrub(DataObject(value), is_done)


def value2json(obj, pretty=False, sort_keys=False):
    try:
        json = json_encoder(obj, pretty=pretty)
        if json == None:
            Log.note(str(type(obj)) + " is not valid{{type}}JSON",  type= " (pretty) " if pretty else " ")
            Log.error("Not valid JSON: " + str(obj) + " of type " + str(type(obj)))
        return json
    except Exception as e:
        e = Except.wrap(e)
        try:
            json = pypy_json_encode(obj)
            return json
        except Exception, _:
            pass
        Log.error("Can not encode into JSON: {{value}}", value=repr(obj), cause=e)


def remove_line_comment(line):
    mode = 0  # 0=code, 1=inside_string, 2=escaping
    for i, c in enumerate(line):
        if c == '"':
            if mode == 0:
                mode = 1
            elif mode == 1:
                mode = 0
            else:
                mode = 1
        elif c == '\\':
            if mode == 0:
                mode = 0
            elif mode == 1:
                mode = 2
            else:
                mode = 1
        elif mode == 2:
            mode = 1
        elif c == "#" and mode == 0:
            return line[0:i]
        elif c == "/" and mode == 0 and line[i + 1] == "/":
            return line[0:i]
    return line


def json2value(json_string, params=Null, flexible=False, leaves=False):
    """
    :param json_string: THE JSON
    :param params: STANDARD JSON PARAMS
    :param flexible: REMOVE COMMENTS
    :param leaves: ASSUME JSON KEYS ARE DOT-DELIMITED
    :return: Python value
    """
    if isinstance(json_string, str):
        Log.error("only unicode json accepted")

    try:
        if flexible:
            # REMOVE """COMMENTS""", # COMMENTS, //COMMENTS, AND \n \r
            # DERIVED FROM https://github.com/jeads/datasource/blob/master/datasource/bases/BaseHub.py# L58
            json_string = re.sub(r"\"\"\".*?\"\"\"", r"\n", json_string, flags=re.MULTILINE)
            json_string = "\n".join(remove_line_comment(l) for l in json_string.split("\n"))
            # ALLOW DICTIONARY'S NAME:VALUE LIST TO END WITH COMMA
            json_string = re.sub(r",\s*\}", r"}", json_string)
            # ALLOW LISTS TO END WITH COMMA
            json_string = re.sub(r",\s*\]", r"]", json_string)

        if params:
            # LOOKUP REFERENCES
            json_string = expand_template(json_string, params)

        try:
            value = wrap(json_decoder(unicode(json_string)))
        except Exception as e:
            Log.error("can not decode\n{{content}}", content=json_string, cause=e)

        if leaves:
            value = wrap_leaves(value)

        return value

    except Exception as e:
        e = Except.wrap(e)

        if not json_string.strip():
            Log.error("JSON string is only whitespace")

        c = e
        while "Expecting '" in c.cause and "' delimiter: line" in c.cause:
            c = c.cause

        if "Expecting '" in c and "' delimiter: line" in c:
            line_index = int(strings.between(c.message, " line ", " column ")) - 1
            column = int(strings.between(c.message, " column ", " ")) - 1
            line = json_string.split("\n")[line_index].replace("\t", " ")
            if column > 20:
                sample = "..." + line[column - 20:]
                pointer = "   " + (" " * 20) + "^"
            else:
                sample = line
                pointer = (" " * column) + "^"

            if len(sample) > 43:
                sample = sample[:43] + "..."

            Log.error("Can not decode JSON at:\n\t" + sample + "\n\t" + pointer + "\n")

        base_str = strings.limit(json_string, 1000).encode('utf8')
        hexx_str = bytes2hex(base_str, " ")
        try:
            char_str = " " + "  ".join((c.decode("latin1") if ord(c) >= 32 else ".") for c in base_str)
        except Exception as e:
            char_str = " "
        Log.error("Can not decode JSON:\n" + char_str + "\n" + hexx_str + "\n", e)


def bytes2hex(value, separator=" "):
    return separator.join("%02X" % ord(x) for x in value)


def utf82unicode(value):
    return value.decode('utf8')


def datetime2unix(d):
    try:
        if d == None:
            return None
        elif isinstance(d, datetime):
            epoch = datetime(1970, 1, 1)
        elif isinstance(d, date):
            epoch = date(1970, 1, 1)
        else:
            Log.error("Can not convert {{value}} of type {{type}}",  value= d,  type= d.__class__)

        diff = d - epoch
        return float(diff.total_seconds())
    except Exception as e:
        Log.error("Can not convert {{value}}",  value= d, cause=e)


from mo_json.decoder import json_decoder
from mo_json.encoder import json_encoder, pypy_json_encode
