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

from urlparse import urlparse

from pyDots import wrap

_convert = None
_Log = None


def _late_import():
    global _convert
    global _Log
    from pyLibrary import convert as _convert
    from MoLogs import Log as _Log
    _ = _convert
    _ = _Log

names = ["path", "query", "fragment"]
indicator = ["/", "?", "#"]


def parse(output, suffix, curr, next):
    if next == len(indicator):
        output.__setattr__(names[curr], suffix)
        return

    e = suffix.find(indicator[next])
    if e == -1:
        parse(output, suffix, curr, next + 1)
    else:
        output.__setattr__(names[curr], suffix[:e:])
        parse(output, suffix[e + 1::], next, next + 1)


class URL(object):
    """
    JUST LIKE urllib.parse() [1], BUT CAN HANDLE JSON query PARAMETERS

    [1] https://docs.python.org/3/library/urllib.parse.html
    """

    def __init__(self, value):
        if not _convert:
            _late_import()

        try:
            self.scheme = None
            self.host = None
            self.port = None
            self.path = ""
            self.query = ""
            self.fragment = ""

            if value == None:
                return

            if value.startswith("file://") or value.startswith("//"):
                # urlparse DOES NOT WORK IN THESE CASES
                scheme, suffix = value.split("//")
                self.scheme = scheme.rstrip(":")
                parse(self, suffix, 0, 1)
                self.query = wrap(_convert.url_param2value(self.query))
            else:
                output = urlparse(value)
                self.scheme = output.scheme
                self.port = output.port
                self.host = output.netloc.split(":")[0]
                self.path = output.path
                self.query = wrap(_convert.url_param2value(output.query))
                self.fragment = output.fragment
        except Exception, e:
            _Log.error("problem parsing {{value}} to URL", value=value, cause=e)
    def __nonzero__(self):
        if self.scheme or self.host or self.port or self.path or self.query or self.fragment:
            return True
        return False

    def __bool__(self):
        if self.scheme or self.host or self.port or self.path or self.query or self.fragment:
            return True
        return False

    def __str__(self):
        url = b""
        if self.host:
            url = self.host
        if self.scheme:
            url = self.scheme + "://"+url
        if self.port:
            url = url + ":" + str(self.port)
        if self.path:
            if self.path[0]=="/":
                url += str(self.path)
            else:
                url += b"/"+str(self.path)
        if self.query:
            url = url + '?' + _convert.value2url(self.query)
        if self.fragment:
            url = url + '#' + _convert.value2url(self.fragment)
        return url


