# encoding: utf-8
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

import json
import sys

PY3 = sys.version_info[0] == 3
PY2 = sys.version_info[0] == 2

none_type = type(None)
boolean_type = type(True)

if PY3:
    import collections

    text_type = str
    string_types = str
    binary_type = bytes
    integer_types = int
    number_types = (int, float)
    long = int

    xrange = range
    filter_type = type(filter(lambda x: True, []))
    generator_types = (collections.Iterable, filter_type)

    round = round
    from html.parser import HTMLParser
    from urllib.parse import urlparse
    from io import StringIO
    from _thread import allocate_lock, get_ident, start_new_thread, interrupt_main

    def get_function_name(func):
        return func.__name__

    def get_function_arguments(func):
        return func.__code__.co_varnames[:func.__code__.co_argcount]

    def get_function_defaults(func):
        return func.__defaults__

    utf8_json_encoder = json.JSONEncoder(
        skipkeys=False,
        ensure_ascii=False,  # DIFF FROM DEFAULTS
        check_circular=True,
        allow_nan=True,
        indent=None,
        separators=(',', ':'),
        default=None,
        sort_keys=True   # <-- IMPORTANT!  sort_keys==True
    ).encode

else:
    import __builtin__
    from types import GeneratorType

    text_type = __builtin__.unicode
    string_types = (str, unicode)
    binary_type = str
    integer_types = (int, long)
    number_types = (int, long, float)
    long = __builtin__.long

    xrange = __builtin__.xrange
    generator_types = (GeneratorType,)

    round = __builtin__.round
    import HTMLParser
    from urlparse import urlparse
    from StringIO import StringIO
    from thread import allocate_lock, get_ident, start_new_thread, interrupt_main

    def get_function_name(func):
        return func.func_name

    def get_function_arguments(func):
        return func.func_code.co_varnames[:func.func_code.co_argcount]

    def get_function_defaults(func):
        return func.func_defaults

    utf8_json_encoder = json.JSONEncoder(
        skipkeys=False,
        ensure_ascii=False,  # DIFF FROM DEFAULTS
        check_circular=True,
        allow_nan=True,
        indent=None,
        separators=(',', ':'),
        encoding='utf-8',  # DIFF FROM DEFAULTS
        default=None,
        sort_keys=True   # <-- IMPORTANT!  sort_keys==True
    ).encode

