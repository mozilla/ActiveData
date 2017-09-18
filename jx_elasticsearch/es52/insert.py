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

import json
import time
from collections import deque, Mapping
from datetime import datetime, date, timedelta
from decimal import Decimal

from future.utils import text_type

from mo_dots import Data, FlatList, NullType, split_field, join_field, unwrap
from mo_json import ESCAPE_DCT, float2json
from mo_json.encoder import pretty_json, problem_serializing, _repr, UnicodeBuilder, COMMA
from mo_json.typed_encoder import untype_path
from mo_logs import Log
from mo_logs.strings import utf82unicode
from mo_times.dates import Date
from mo_times.durations import Duration

append = UnicodeBuilder.append


class Insert(object):

    def __init__(self, schema):
        schema = Data()
        for c in schema.columns:
            untyped_path = untype_path(c.names["."])
            type = c.type
            schema[untyped_path][type] = c
        self.schema = unwrap(schema)

    def typed_encode(self, value):
        try:
            _buffer = UnicodeBuilder(1024)
            self._typed_encode(value, _buffer)
            output = _buffer.build()
            return output
        except Exception as e:
            # THE PRETTY JSON WILL PROVIDE MORE DETAIL ABOUT THE SERIALIZATION CONCERNS
            from mo_logs import Log

            Log.warning("Serialization of JSON problems", e)
            try:
                return pretty_json(value)
            except Exception as f:
                Log.error("problem serializing object", f)

    def _typed_encode(self, value, sub_schema, _buffer):
        try:
            if value is None:
                append(_buffer, u'{}')
                return
            elif value is True:
                append(_buffer, u'{"$boolean": true}')
                return
            elif value is False:
                append(_buffer, u'{"$boolean": false}')
                return

            _type = value.__class__
            if _type in (dict, Data):
                if value:
                    _dict2json(self, value, _buffer)
                else:
                    append(_buffer, u'{"$exists": "."}')
            elif _type is str:
                append(_buffer, u'{"$string": "')
                try:
                    v = utf82unicode(value)
                except Exception as e:
                    raise problem_serializing(value, e)

                for c in v:
                    append(_buffer, ESCAPE_DCT.get(c, c))
                append(_buffer, u'"}')
            elif _type is text_type:
                append(_buffer, u'{"$string": "')
                for c in value:
                    append(_buffer, ESCAPE_DCT.get(c, c))
                append(_buffer, u'"}')
            elif _type in (int, long, Decimal):
                append(_buffer, u'{"$number": ')
                append(_buffer, float2json(value))
                append(_buffer, u'}')
            elif _type is float:
                append(_buffer, u'{"$number": ')
                append(_buffer, float2json(value))
                append(_buffer, u'}')
            elif _type in (set, list, tuple, FlatList):
                if any(isinstance(v, (Mapping, set, list, tuple, FlatList)) for v in value):
                    append(_buffer, u'{"$nested": ')
                    _list2json(self, value, _buffer)
                    append(_buffer, u'}')
                else:
                    # ALLOW PRIMITIVE MULTIVALUES
                    _list2json(self, value, _buffer)
            elif _type is date:
                append(_buffer, u'{"$number": ')
                append(_buffer, float2json(time.mktime(value.timetuple())))
                append(_buffer, u'}')
            elif _type is datetime:
                append(_buffer, u'{"$number": ')
                append(_buffer, float2json(time.mktime(value.timetuple())))
                append(_buffer, u'}')
            elif _type is Date:
                append(_buffer, u'{"$number": ')
                append(_buffer, float2json(time.mktime(value.value.timetuple())))
                append(_buffer, u'}')
            elif _type is timedelta:
                append(_buffer, u'{"$number": ')
                append(_buffer, float2json(value.total_seconds()))
                append(_buffer, u'}')
            elif _type is Duration:
                append(_buffer, u'{"$number": ')
                append(_buffer, float2json(value.seconds))
                append(_buffer, u'}')
            elif _type is NullType:
                append(_buffer, u"null")
            elif hasattr(value, '__json__'):
                j = value.__json__()
                t = json2typed(j)
                append(_buffer, t)
            elif hasattr(value, '__iter__'):
                append(_buffer, u'{"$nested": ')
                _iter2json(value, _buffer)
                append(_buffer, u'}')
            else:
                from mo_logs import Log

                Log.error(_repr(value) + " is not JSON serializable")
        except Exception as e:
            from mo_logs import Log

            Log.error(_repr(value) + " is not JSON serializable", e)


    def _list2json(self, value, _buffer):
        if not value:
            append(_buffer, u"[]")
        else:
            sep = u"["
            for v in value:
                append(_buffer, sep)
                sep = COMMA
                _typed_encode(self, v, _buffer)
            append(_buffer, u"]")


    def _iter2json(self, value, _buffer):
        append(_buffer, u"[")
        sep = u""
        for v in value:
            append(_buffer, sep)
            sep = COMMA
            _typed_encode(self, v, _buffer)
        append(_buffer, u"]")


    def _dict2json(self, value, _buffer):
        prefix = u'{"$exists": ".", '
        for k, v in value.iteritems():
            if v == None or v == "":
                continue
            append(_buffer, prefix)
            prefix = u", "
            if isinstance(k, str):
                k = utf82unicode(k)
            if not isinstance(k, text_type):
                Log.error("Expecting property name to be a string")
            append(_buffer, json.dumps(encode_property(self, k)))
            append(_buffer, u": ")
            _typed_encode(self, v, _buffer)
        if prefix == u", ":
            append(_buffer, u'}')
        else:
            append(_buffer, u'{"$exists": "."}')


