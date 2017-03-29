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

from mo_dots import wrap
from mo_logs import Log
from mo_logs.convert import value2json


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
    except Exception as e:
        Log.error("Can not convert {{value|quote}} to unicode", value=value, cause=e)

_map2url = {chr(i): chr(i) for i in range(32, 128)}
for c in b" {}<>;/?:@&=+$,":
    _map2url[c] = b"%" + str(int2hex(ord(c), 2))
for i in range(128, 256):
    _map2url[chr(i)] = b"%" + str(int2hex(i, 2))



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
    """
    :param value:
    :return: ascii URL
    """
    if value == None:
        Log.error("Can not encode None into a URL")

    if isinstance(value, Mapping):
        output = b"&".join([value2url(k) + b"=" + (value2url(v) if isinstance(v, basestring) else value2url(value2json(v))) for k, v in value.items()])
    elif isinstance(value, unicode):
        output = b"".join(_map2url[c] for c in value.encode('utf8'))
    elif isinstance(value, str):
        output = b"".join(_map2url[c] for c in value)
    elif hasattr(value, "__iter__"):
        output = b",".join(value2url(v) for v in value)
    else:
        output = str(value)
    return output



