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
from collections import Mapping
from datetime import datetime, date, timedelta
from decimal import Decimal

from future.utils import text_type

from jx_base import python_type_to_json_type
from mo_dots import Data, FlatList, NullType, unwrap
from mo_json import ESCAPE_DCT, float2json, json2value
from mo_json.encoder import pretty_json, problem_serializing, UnicodeBuilder, COMMA, COLON
from mo_json.typed_encoder import untype_path, encode_property
from mo_logs import Log
from mo_logs.strings import utf82unicode
from mo_times.dates import Date
from mo_times.durations import Duration
from pyLibrary.env.elasticsearch import parse_properties, random_id

append = UnicodeBuilder.append

_BOOLEAN = "$boolean"
_NUMBER = "$number"
_STRING = "$string"
_NESTED = "$nested"
_EXISTS = "$exists"


class TypedInserter(object):
    def __init__(self, es, id_column="_id"):
        columns = parse_properties(es.settings.alias, ".", es.get_properties()).columns
        _schema = Data()
        for c in columns:
            untyped_path = untype_path(c.names["."])
            type = c.type
            _schema[untyped_path]["$" + type] = c
        self.schema = unwrap(_schema)
        self.es = es
        self.id_column = id_column
        self.remove_id = True if id_column == "_id" else False

    def typed_encode(self, r):
        """
        :param record:  expecting id and value properties
        :return:  dict with id and json properties
        """
        try:
            value = r['value']
            if "json" in r:
                value = json2value(r["json"])
            elif isinstance(value, Mapping) or value != None:
                pass
            else:
                from mo_logs import Log
                raise Log.error("Expecting every record given to have \"value\" or \"json\" property")

            _buffer = UnicodeBuilder(1024)
            net_new_properties = []
            path = []
            if isinstance(value, Mapping):
                given_id = value.get(self.id_column)
                if self.remove_id:
                    value[self.id_column] = None
            else:
                given_id = None

            if given_id:
                record_id = r.get('id')
                if record_id and record_id != given_id:
                    from mo_logs import Log

                    raise Log.error(
                        "expecting {{property}} of record ({{record_id|quote}}) to match one given ({{given|quote}})",
                        property=self.id_column,
                        record_id=record_id,
                        given=given_id
                    )
            else:
                record_id = r.get('id')
                if record_id:
                    given_id = record_id
                else:
                    given_id = random_id()

            self._typed_encode(value, self.schema, path, net_new_properties, _buffer)
            json = _buffer.build()

            for props in net_new_properties:
                path, type = props[:-1], props[-1][1:]
                # self.es.add_column(join_field(path), type)

            return {"id": given_id, "json": json}
        except Exception as e:
            # THE PRETTY JSON WILL PROVIDE MORE DETAIL ABOUT THE SERIALIZATION CONCERNS
            from mo_logs import Log

            Log.warning("Serialization of JSON problems", e)
            try:
                return pretty_json(r)
            except Exception as f:
                Log.error("problem serializing object", f)

    def _typed_encode(self, value, sub_schema, path, net_new_properties, _buffer):
        try:
            if value is None:
                append(_buffer, u'{}')
                return
            elif value is True:
                if _BOOLEAN not in sub_schema:
                    sub_schema[_BOOLEAN] = {}
                    net_new_properties.append(path+[_BOOLEAN])
                append(_buffer, u'{"$boolean": true}')
                return
            elif value is False:
                if _BOOLEAN not in sub_schema:
                    sub_schema[_BOOLEAN] = {}
                    net_new_properties.append(path+[_BOOLEAN])
                append(_buffer, u'{"$boolean": false}')
                return

            _type = value.__class__
            if _type in (dict, Data):
                if _NESTED in sub_schema:
                    # PREFER NESTED, WHEN SEEN BEFORE
                    if value:
                        append(_buffer, u'{"$nested": [')
                        self._dict2json(value, sub_schema[_NESTED], path + [_NESTED], net_new_properties, _buffer)
                        append(_buffer, ']}')
                    else:
                        # SINGLETON LISTS OF null SHOULD NOT EXIST
                        pass
                else:
                    if _EXISTS not in sub_schema:
                        sub_schema[_EXISTS] = {}
                        net_new_properties.append(path+[_EXISTS])

                    if value:
                        self._dict2json(value, sub_schema, path, net_new_properties, _buffer)
                    else:
                        append(_buffer, u'{"$exists": "."}')
            elif _type is str:
                if _STRING not in sub_schema:
                    sub_schema[_STRING] = True
                    net_new_properties.append(path + [_STRING])
                append(_buffer, u'{"$string": "')
                try:
                    v = utf82unicode(value)
                except Exception as e:
                    raise problem_serializing(value, e)

                for c in v:
                    append(_buffer, ESCAPE_DCT.get(c, c))
                append(_buffer, u'"}')
            elif _type is text_type:
                if _STRING not in sub_schema:
                    sub_schema[_STRING] = True
                    net_new_properties.append(path + [_STRING])

                append(_buffer, u'{"$string": "')
                for c in value:
                    append(_buffer, ESCAPE_DCT.get(c, c))
                append(_buffer, u'"}')
            elif _type in (int, long, Decimal):
                if _NUMBER not in sub_schema:
                    sub_schema[_NUMBER] = True
                    net_new_properties.append(path + [_NUMBER])

                append(_buffer, u'{"$number": ')
                append(_buffer, float2json(value))
                append(_buffer, u'}')
            elif _type is float:
                if _NUMBER not in sub_schema:
                    sub_schema[_NUMBER] = True
                    net_new_properties.append(path + [_NUMBER])
                append(_buffer, u'{"$number": ')
                append(_buffer, float2json(value))
                append(_buffer, u'}')
            elif _type in (set, list, tuple, FlatList):
                if any(isinstance(v, (Mapping, set, list, tuple, FlatList)) for v in value):
                    if _NESTED not in sub_schema:
                        sub_schema[_NESTED] = {}
                        net_new_properties.append(path + [_NESTED])
                    append(_buffer, u'{"$nested": ')
                    self._list2json(value, sub_schema[_NESTED], path+[_NESTED], net_new_properties, _buffer)
                    append(_buffer, u'}')
                else:
                    # ALLOW PRIMITIVE MULTIVALUES
                    types = list(set(python_type_to_json_type[v.__class__] for v in value))
                    if len(types) != 1:
                        from mo_logs import Log
                        Log.error("Can not handle multi-typed multivalues")
                    element_type = "$"+types[0]
                    if element_type not in sub_schema:
                        sub_schema[element_type] = True
                        net_new_properties.append(path + [element_type])
                    append(_buffer, u'{"'+element_type+u'": ')
                    self._multivalue2json(value, sub_schema[element_type], path+[element_type], net_new_properties, _buffer)
                    append(_buffer, u'}')
            elif _type is date:
                if _NUMBER not in sub_schema:
                    sub_schema[_NUMBER] = True
                    net_new_properties.append(path + [_NUMBER])
                append(_buffer, u'{"$number": ')
                append(_buffer, float2json(time.mktime(value.timetuple())))
                append(_buffer, u'}')
            elif _type is datetime:
                if _NUMBER not in sub_schema:
                    sub_schema[_NUMBER] = True
                    net_new_properties.append(path + [_NUMBER])
                append(_buffer, u'{"$number": ')
                append(_buffer, float2json(time.mktime(value.timetuple())))
                append(_buffer, u'}')
            elif _type is Date:
                if _NUMBER not in sub_schema:
                    sub_schema[_NUMBER] = True
                    net_new_properties.append(path + [_NUMBER])
                append(_buffer, u'{"$number": ')
                append(_buffer, float2json(value.unix))
                append(_buffer, u'}')
            elif _type is timedelta:
                if _NUMBER not in sub_schema:
                    sub_schema[_NUMBER] = True
                    net_new_properties.append(path + [_NUMBER])
                append(_buffer, u'{"$number": ')
                append(_buffer, float2json(value.total_seconds()))
                append(_buffer, u'}')
            elif _type is Duration:
                if _NUMBER not in sub_schema:
                    sub_schema[_NUMBER] = True
                    net_new_properties.append(path + [_NUMBER])
                append(_buffer, u'{"$number": ')
                append(_buffer, float2json(value.seconds))
                append(_buffer, u'}')
            elif _type is NullType:
                append(_buffer, u"null")
            elif hasattr(value, '__json__'):
                from mo_logs import Log
                Log.error("do not know how to handle")
            elif hasattr(value, '__iter__'):
                if _NESTED not in sub_schema:
                    sub_schema[_NESTED] = {}
                    net_new_properties.append(path + [_NESTED])

                append(_buffer, u'{"$nested": ')
                self._iter2json(value, sub_schema[_NESTED], path+[_NESTED], net_new_properties, _buffer)
                append(_buffer, u'}')
            else:
                from mo_logs import Log

                Log.error(repr(value) + " is not JSON serializable")
        except Exception as e:
            from mo_logs import Log

            Log.error(repr(value) + " is not JSON serializable", e)

    def _list2json(self, value, sub_schema, path, net_new_properties, _buffer):
        if not value:
            append(_buffer, u"[]")
        else:
            sep = u"["
            for v in value:
                append(_buffer, sep)
                sep = COMMA
                self._typed_encode(v, sub_schema, path, net_new_properties, _buffer)
            append(_buffer, u"]")

    def _multivalue2json(self, value, sub_schema, path, net_new_properties, _buffer):
        if not value:
            append(_buffer, u"[]")
        else:
            sep = u"["
            for v in value:
                append(_buffer, sep)
                sep = COMMA
                append(_buffer, json_encoder(v))
            append(_buffer, u"]")

    def _iter2json(self, value, sub_schema, path, net_new_properties, _buffer):
        append(_buffer, u"[")
        sep = u""
        for v in value:
            append(_buffer, sep)
            sep = COMMA
            self._typed_encode(v, sub_schema, path, net_new_properties, _buffer)
        append(_buffer, u"]")

    def _dict2json(self, value, sub_schema, path, net_new_properties, _buffer):
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
            if k not in sub_schema:
                sub_schema[k] = {}
                net_new_properties.append(path+[k])
            append(_buffer, json.dumps(encode_property(k)))
            append(_buffer, u": ")
            self._typed_encode(v, sub_schema[k], path+[k], net_new_properties, _buffer)
        if prefix == u", ":
            append(_buffer, u'}')
        else:
            append(_buffer, u'{"$exists": "."}')


json_encoder = json.JSONEncoder(
    skipkeys=False,
    ensure_ascii=False,  # DIFF FROM DEFAULTS
    check_circular=True,
    allow_nan=True,
    indent=None,
    separators=(COMMA, COLON),
    encoding='utf8',
    default=None,
    sort_keys=True
).encode
