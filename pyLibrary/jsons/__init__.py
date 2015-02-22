from datetime import date, timedelta, datetime
from decimal import Decimal
import json
import re

from pyLibrary.dot import DictList
from pyLibrary.times.dates import Date

from pyLibrary.times.durations import Duration


Log = None
datetime2milli = None
utf82unicode = None


def _late_import():
    global Log
    global datetime2milli
    global utf82unicode

    from pyLibrary.debugs.logs import Log
    from pyLibrary.convert import datetime2milli, utf82unicode

    _ = Log
    __ = datetime2milli
    ___ = utf82unicode


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


def scrub(value):
    """
    REMOVE/REPLACE VALUES THAT CAN NOT BE JSON-IZED
    """
    if not Log:
        _late_import()
    return _scrub(value)


def _scrub(value):
    if value == None:
        return None

    type = value.__class__

    if type in (date, datetime):
        return float(datetime2milli(value)) / float(1000)
    elif type is timedelta:
        return value.total_seconds()
    elif type is Date:
        return value.unix
    elif type is Duration:
        return value.seconds
    elif type is str:
        return utf82unicode(value)
    elif type is Decimal:
        return float(value)
    elif isinstance(value, dict):
        output = {}
        for k, v in value.iteritems():
            if not isinstance(k, basestring):
                Log.error("keys must be strings")
            v = _scrub(v)
            output[k] = v
        return output
    elif type in (list, DictList):
        output = []
        for v in value:
            v = _scrub(v)
            output.append(v)
        return output
    elif type.__name__ == "bool_":  # DEAR ME!  Numpy has it's own booleans (value==False could be used, but 0==False in Python.  DOH!)
        if value == False:
            return False
        else:
            return True
    elif hasattr(value, '__json__'):
        try:
            output = json._default_decoder.decode(value.__json__())
            return output
        except Exception, e:
            Log.error("problem with calling __json__()", e)
    elif hasattr(value, '__iter__'):
        output = []
        for v in value:
            v = _scrub(v)
            output.append(v)
        return output
    else:
        return value


from . import encoder as json_encoder
from . import ref
