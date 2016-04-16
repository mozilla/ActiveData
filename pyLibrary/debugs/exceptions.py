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

import sys
from collections import Mapping

from pyLibrary.dot import Dict, listwrap, unwraplist, set_default
from pyLibrary.jsons.encoder import json_encoder
from pyLibrary.strings import indent, expand_template

FATAL = "FATAL"
ERROR = "ERROR"
WARNING = "WARNING"
ALARM = "ALARM"
UNEXPECTED = "UNEXPECTED"
NOTE = "NOTE"


class Except(Exception):

    @staticmethod
    def new_instance(desc):
        return Except(
            desc.type,
            desc.template,
            desc.params,
            [Except.new_instance(c) for c in listwrap(desc.cause)],
            desc.trace
        )


    def __init__(self, type=ERROR, template=None, params=None, cause=None, trace=None, **kwargs):
        Exception.__init__(self)
        self.type = type
        self.template = template
        self.params = set_default(kwargs, params)
        self.cause = cause

        if not trace:
            self.trace=extract_stack(2)
        else:
            self.trace = trace

    @classmethod
    def wrap(cls, e, stack_depth=0):
        if e == None:
            return None
        elif isinstance(e, (list, Except)):
            return e
        elif isinstance(e, Mapping):
            e.cause = unwraplist([Except.wrap(c) for c in listwrap(e.cause)])
            return Except(**e)
        else:
            if hasattr(e, "message") and e.message:
                cause = Except(ERROR, unicode(e.message), trace=_extract_traceback(0))
            else:
                cause = Except(ERROR, unicode(e), trace=_extract_traceback(0))

            trace = extract_stack(stack_depth + 2)  # +2 = to remove the caller, and it's call to this' Except.wrap()
            cause.trace.extend(trace)
            return cause

    @property
    def message(self):
        return expand_template(self.template, self.params)

    def __contains__(self, value):
        if isinstance(value, basestring):
            if self.template.find(value) >= 0 or self.message.find(value) >= 0:
                return True

        if self.type == value:
            return True
        for c in listwrap(self.cause):
            if value in c:
                return True
        return False

    def __unicode__(self):
        output = self.type + ": " + self.template + "\n"
        if self.params:
            output = expand_template(output, self.params)

        if self.trace:
            output += indent(format_trace(self.trace))

        if self.cause:
            cause_strings = []
            for c in listwrap(self.cause):
                try:
                    cause_strings.append(unicode(c))
                except Exception:
                    pass

            output += "caused by\n\t" + "and caused by\n\t".join(cause_strings)

        return output

    def __str__(self):
        return self.__unicode__().encode('latin1', 'replace')

    def as_dict(self):
        return Dict(
            type=self.type,
            template=self.template,
            params=self.params,
            cause=self.cause,
            trace=self.trace
        )

    def __json__(self):
        return json_encoder(self.as_dict())



def extract_stack(start=0):
    """
    SNAGGED FROM traceback.py
    Extract the raw traceback from the current stack frame.

    Each item in the returned list is a quadruple (filename,
    line number, function name, text), and the entries are in order
    from newest to oldest
    """
    try:
        raise ZeroDivisionError
    except ZeroDivisionError:
        trace = sys.exc_info()[2]
        f = trace.tb_frame.f_back

    for i in range(start):
        f = f.f_back

    stack = []
    n = 0
    while f is not None:
        stack.append({
            "depth": n,
            "line": f.f_lineno,
            "file": f.f_code.co_filename,
            "method": f.f_code.co_name
        })
        f = f.f_back
        n += 1
    return stack


def _extract_traceback(start):
    """
    SNAGGED FROM traceback.py

    RETURN list OF dicts DESCRIBING THE STACK TRACE
    """
    tb = sys.exc_info()[2]
    for i in range(start):
        tb = tb.tb_next

    trace = []
    n = 0
    while tb is not None:
        f = tb.tb_frame
        trace.append({
            "depth": n,
            "file": f.f_code.co_filename,
            "line": tb.tb_lineno,
            "method": f.f_code.co_name
        })
        tb = tb.tb_next
        n += 1
    trace.reverse()
    return trace


def format_trace(tbs, start=0):
    trace = []
    for d in tbs[start::]:
        item = expand_template('File "{{file}}", line {{line}}, in {{method}}\n', d)
        trace.append(item)
    return "".join(trace)

