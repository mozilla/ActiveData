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
from datetime import date, timedelta, datetime
from decimal import Decimal
import json
import re
from types import NoneType

import math

from pyDots import FlatList, NullType, Data, unwrap
from pyDots.objects import DataObject
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import Duration


_Log = None
datetime2unix = None
utf82unicode = None


def _late_import():
    global _Log
    global datetime2unix
    global utf82unicode

    from pyLibrary.debugs.logs import Log as _Log
    from pyLibrary.convert import datetime2unix, utf82unicode

    _ = _Log
    _ = datetime2unix
    _ = utf82unicode


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
    except Exception, e:
        from pyLibrary.debugs.logs import Log
        Log.error("not expected", e)


def scrub(value):
    """
    REMOVE/REPLACE VALUES THAT CAN NOT BE JSON-IZED
    """
    if not _Log:
        _late_import()
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
        return _scrub(unwrap(value), is_done)
    elif isinstance(value, Mapping):
        _id = id(value)
        if _id in is_done:
            _Log.warning("possible loop in structure detected")
            return '"<LOOP IN STRUCTURE>"'
        is_done.add(_id)

        output = {}
        for k, v in value.iteritems():
            if isinstance(k, basestring):
                pass
            elif hasattr(k, "__unicode__"):
                k = unicode(k)
            else:
                _Log.error("keys must be strings")
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
    elif hasattr(value, '__json__'):
        try:
            output = json._default_decoder.decode(value.__json__())
            return output
        except Exception, e:
            _Log.error("problem with calling __json__()", e)
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


from . import encoder as json_encoder
from . import ref
