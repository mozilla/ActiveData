# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

import re

from mo_dots import Data, coalesce, is_data, listwrap, wrap_leaves
from mo_logs import Log, strings
from mo_times.dates import Date

true = True
false = False
null = None
EMPTY_DICT = {}

_keep_imports = [coalesce, listwrap, Date, Log, Data, re, wrap_leaves, is_data]


def compile_expression(source):
    """
    THIS FUNCTION IS ON ITS OWN FOR MINIMAL GLOBAL NAMESPACE

    :param source:  PYTHON SOURCE CODE
    :return:  PYTHON FUNCTION
    """
    fake_locals = {}
    try:
        exec(
"""
def output(row, rownum=None, rows=None):
    _source = """ + strings.quote(source) + """
    try:
        return """ + source + """
    except Exception as e:
        Log.error("Problem with dynamic function {{func|quote}}",  func=_source, cause=e)
""",
            globals(),
            fake_locals
        )
    except Exception as e:
        Log.error("Bad source: {{source}}", source=source, cause=e)
    return fake_locals['output']
