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

from datetime import datetime, date, timedelta
import math
import re
from pyLibrary.dot import Null

from pyLibrary.times.durations import Duration, MILLI_VALUES
from pyLibrary.vendor.dateutil.parser import parse as parse_date

try:
    import pytz
except Exception, e:
    pass
from pyLibrary.strings import deformat


class Date(object):

    MIN = None
    MAX = None

    def __new__(cls, *args, **kwargs):
        if len(args) == 1 and args[0] == None:
            return Null
        return object.__new__(cls, *args)

    def __init__(self, *args):
        try:
            if len(args) == 1:
                a0 = args[0]
                if isinstance(a0, (datetime, date)):
                    self.value = a0
                elif isinstance(a0, Date):
                    self.value = a0.value
                elif isinstance(a0, (int, long, float)):
                    if a0 == 9999999999000:  # PYPY BUG https://bugs.pypy.org/issue1697
                        self.value = Date.MAX
                    elif a0 > 9999999999:    # WAY TOO BIG IF IT WAS A UNIX TIMESTAMP
                        self.value = datetime.utcfromtimestamp(a0 / 1000)
                    else:
                        self.value = datetime.utcfromtimestamp(a0)
                elif isinstance(a0, basestring):
                    self.value = unicode2datetime(a0)
                else:
                    self.value = datetime(*args)
            else:
                if isinstance(args[0], basestring):
                    self.value = unicode2datetime(*args)
                else:
                    self.value = datetime(*args)

        except Exception, e:
            from pyLibrary.debugs.logs import Log
            Log.error("Can not convert {{args}} to Date", {"args": args}, e)

    def floor(self, duration=None):
        if duration is None:  # ASSUME DAY
            return Date(math.floor(self.milli / 86400000) * 86400000)
        elif duration.milli % (7*86400000) ==0:
            offset = 4*86400000
            return Date(math.floor((self.milli+offset) / duration.milli) * duration.milli - offset)
        elif not duration.month:
            return Date(math.floor(self.milli / duration.milli) * duration.milli)
        else:
            month = int(math.floor(self.value.month / duration.month) * duration.month)
            return Date(datetime(self.value.year, month, 1))

    def format(self, format="%Y-%m-%d %H:%M:%S"):
        try:
            return self.value.strftime(format)
        except Exception, e:
            from pyLibrary.debugs.logs import Log
            Log.error("Can not format {{value}} with {{format}}", {"value": self.value, "format": format}, e)

    @property
    def milli(self):
        return self.unix*1000

    @property
    def unix(self):
        try:
            if self.value == None:
                return None
            elif isinstance(self.value, datetime):
                epoch = datetime(1970, 1, 1)
            elif isinstance(self.value, date):
                epoch = date(1970, 1, 1)
            else:
                from pyLibrary.debugs.logs import Log
                Log.error("Can not convert {{value}} of type {{type}}", {"value": self.value, "type": self.value.__class__})

            diff = self.value - epoch
            return diff.total_seconds()
        except Exception, e:
            from pyLibrary.debugs.logs import Log
            Log.error("Can not convert {{value}}", {"value": self.value}, e)

    def addDay(self):
        return Date(self.value + timedelta(days=1))

    def add(self, other):
        if isinstance(other, datetime):
            return Date(self.value - other)
        elif isinstance(other, date):
            return Date(self.value - other)
        elif isinstance(other, Date):
            return Date(self.value - other.value)
        elif isinstance(other, timedelta):
            return Date(self.value + other)
        elif isinstance(other, Duration):
            if other.month:
                if (self.value+timedelta(days=1)).month != self.value.month:
                    # LAST DAY OF MONTH
                    output = add_month(self.value+timedelta(days=1), other.month) - timedelta(days=1)
                    return Date(output)
                else:
                    day = self.value.day
                    num_days = (add_month(datetime(self.value.year, self.value.month, 1), other.month+1) - timedelta(days=1)).day
                    day = min(day, num_days)
                    curr = set_day(self.value, day)
                    output = add_month(curr, other.month)
                    return Date(output)
            else:
                return Date(self.milli + other.milli)
        else:
            from pyLibrary.debugs.logs import Log
            Log.error("can not subtract {{type}} from Date", {"type":other.__class__.__name__})

    @staticmethod
    def now():
        return Date(datetime.utcnow())

    @staticmethod
    def eod():
        """
        RETURN END-OF-TODAY (WHICH IS SAME AS BEGINNING OF TOMORROW)
        """
        return Date.today().addDay()

    @staticmethod
    def today():
        return Date(datetime.utcnow()).floor()

    @staticmethod
    def range(min, max, interval):
        v = min
        while v < max:
            yield v
            v = v + interval

    def __str__(self):
        return str(self.value)

    def __sub__(self, other):
        if other == None:
            return None
        if isinstance(other, datetime):
            return Duration(self.milli-Date(other).milli)
        if isinstance(other, Date):
            return Duration(self.milli-other.milli)

        return self.add(-other)

    def __lt__(self, other):
        other = Date(other)
        return self.value < other.value

    def __le__(self, other):
        other = Date(other)
        return self.value <= other.value

    def __gt__(self, other):
        other = Date(other)
        return self.value > other.value

    def __ge__(self, other):
        other = Date(other)
        return self.value >= other.value

    def __add__(self, other):
        return self.add(other)


Date.MIN = Date(datetime(1, 1, 1))
Date.MAX = Date(datetime(2286, 11, 20, 17, 46, 39))





def add_month(offset, months):
    month = offset.month+months-1
    year = offset.year
    if not 0 <= month < 12:
        year += int((month - (month % 12)) / 12)
        month = (month % 12)
    month += 1

    output = datetime(
        year=year,
        month=month,
        day=offset.day,
        hour=offset.hour,
        minute=offset.minute,
        second=offset.second,
        microsecond=offset.microsecond
    )
    return output


def set_day(offset, day):
    output = datetime(
        year=offset.year,
        month=offset.month,
        day=day,
        hour=offset.hour,
        minute=offset.minute,
        second=offset.second,
        microsecond=offset.microsecond
    )
    return output


def parse(value):
    def simple_date(sign, dig, type, floor):
        if dig or sign:
            from pyLibrary.debugs.logs import Log
            Log.error("can not accept a multiplier on a datetime")

        if floor:
            return Date(type).floor(Duration(floor))
        else:
            return Date(type)

    terms = re.match(r'(\d*[|\w]+)\s*([+-]\s*\d*[|\w]+)*', value).groups()

    sign, dig, type = re.match(r'([+-]?)\s*(\d*)([|\w]+)', terms[0]).groups()
    if "|" in type:
        type, floor = type.split("|")
    else:
        floor = None

    if type in MILLI_VALUES.keys():
        value = Duration(dig+type)
    else:
        value = simple_date(sign, dig, type, floor)

    for term in terms[1:]:
        if not term:
            continue
        sign, dig, type = re.match(r'([+-])\s*(\d*)([|\w]+)', term).groups()
        if "|" in type:
            type, floor = type.split("|")
        else:
            floor = None

        op = {"+": "__add__", "-": "__sub__"}[sign]
        if type in MILLI_VALUES.keys():
            if floor:
                from pyLibrary.debugs.logs import Log
                Log.error("floor (|) of duration not accepted")
            value = value.__getattribute__(op)(Duration(dig+type))
        else:
            value = value.__getattribute__(op)(simple_date(sign, dig, type, floor))

    return value




def unicode2datetime(value, format=None):
    """
    CONVERT UNICODE STRING TO datetime VALUE
    """
    ## http://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior
    if value == None:
        return None

    value = value.strip()
    if value.lower() == "now":
        return Date.now().value
    elif value.lower() == "today":
        return Date.today().value

    if any(value.lower().find(n) >= 0 for n in ["now", "today"] + list(MILLI_VALUES.keys())):
        return parse(value).value

    if format != None:
        try:
            return datetime.strptime(value, format)
        except Exception, e:
            from pyLibrary.debugs.logs import Log
            Log.error("Can not format {{value}} with {{format}}", {"value": value, "format": format}, e)

    try:
        local_value = parse_date(value)  #eg 2014-07-16 10:57 +0200
        return (local_value - local_value.utcoffset()).replace(tzinfo=None)
    except Exception, e:
        pass



    formats = [
        #"%Y-%m-%d %H:%M %z",  # "%z" NOT SUPPORTED IN 2.7
    ]
    for f in formats:
        try:
            return unicode2datetime(value, format=f)
        except Exception:
            pass

    deformats = [
        "%Y-%m",# eg 2014-07-16 10:57 +0200
        "%Y%m%d",
        "%d%m%Y",
        "%d%m%y",
        "%d%b%Y",
        "%d%b%y",
        "%d%B%Y",
        "%d%B%y",
        "%Y%m%d%H%M%S",
        "%d%m%Y%H%M%S",
        "%d%m%y%H%M%S",
        "%d%b%Y%H%M%S",
        "%d%b%y%H%M%S",
        "%d%B%Y%H%M%S",
        "%d%B%y%H%M%S"
    ]
    value = deformat(value)
    for f in deformats:
        try:
            return unicode2datetime(value, format=f)
        except Exception:
            pass
    else:
        from pyLibrary.debugs.logs import Log
        Log.error("Can not interpret {{value}} as a datetime", {"value": value})

