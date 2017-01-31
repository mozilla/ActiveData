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

import StringIO
from collections import Mapping

from mo_logs import Log
from mo_dots import wrap


def int2hex(value, size):
    return (("0" * size) + hex(value)[2:])[-size:]


def unicode2latin1(value):
    output = value.encode("latin1")
    return output


def latin12unicode(value):
    if isinstance(value, unicode):
        Log.error("can not convert unicode from latin1")
    try:
        return unicode(value.decode('iso-8859-1'))
    except Exception, e:
        Log.error("Can not convert {{value|quote}} to unicode", value=value)

_map2url = {chr(i): latin12unicode(chr(i)) for i in range(32, 256)}
for c in " {}<>;/?:@&=+$,":
    _map2url[c] = "%" + int2hex(ord(c), 2)



def ini2value(ini_content):
    """
    INI FILE CONTENT TO Data
    """
    from ConfigParser import ConfigParser

    buff = StringIO.StringIO(ini_content)
    config = ConfigParser()
    config._read(buff, "dummy")

    output = {}
    for section in config.sections():
        output[section]=s = {}
        for k, v in config.items(section):
            s[k]=v
    return wrap(output)


def value2url(value):
    if value == None:
        Log.error("")

    if isinstance(value, Mapping):
        output = "&".join([value2url(k) + "=" + (value2url(v) if isinstance(v, basestring) else value2url(value2json(v))) for k,v in value.items()])
    elif isinstance(value, unicode):
        output = "".join([_map2url[c] for c in unicode2latin1(value)])
    elif isinstance(value, str):
        output = "".join([_map2url[c] for c in value])
    elif hasattr(value, "__iter__"):
        output = ",".join(value2url(v) for v in value)
    else:
        output = unicode(value)
    return output



