# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

from datetime import date, datetime
from decimal import Decimal

from mo_dots.datas import register_data, Data, SLOT
from mo_dots.lists import FlatList
from mo_dots.nones import NullType
from mo_dots.utils import CLASS, OBJ
from mo_future import binary_type, generator_types, get_function_arguments, get_function_defaults, none_type, text, Mapping
from mo_imports import export, expect

get_attr, set_attr, list_to_data, to_data, from_data = expect("get_attr", "set_attr", "list_to_data", "to_data", "from_data")

_new = object.__new__
_get = object.__getattribute__
_set = object.__setattr__
WRAPPED_CLASSES = set()


class DataObject(Mapping):
    """
    TREAT AN OBJECT LIKE DATA
    """

    def __init__(self, obj):
        _set(self, OBJ, obj)

    def __getattr__(self, item):
        obj = _get(self, OBJ)
        output = get_attr(obj, item)
        return datawrap(output)

    def __setattr__(self, key, value):
        obj = _get(self, OBJ)
        set_attr(obj, key, value)

    def __getitem__(self, item):
        obj = _get(self, OBJ)
        output = get_attr(obj, item)
        return datawrap(output)

    def keys(self):
        obj = _get(self, OBJ)
        try:
            return obj.__dict__.keys()
        except Exception as e:
            raise e

    def items(self):
        obj = _get(self, OBJ)
        try:
            return obj.__dict__.items()
        except Exception as e:
            return [
                (k, getattr(obj, k, None))
                for k in dir(obj)
                if not k.startswith("__")
            ]

    def iteritems(self):
        obj = _get(self, OBJ)
        try:
            return obj.__dict__.iteritems()
        except Exception as e:
            def output():
                for k in dir(obj):
                    if k.startswith("__"):
                        continue
                    yield k, getattr(obj, k, None)
            return output()

    def __data__(self):
        return self

    def __iter__(self):
        return (k for k in self.keys())

    def __unicode__(self):
        obj = _get(self, OBJ)
        return text(obj)

    def __str__(self):
        obj = _get(self, OBJ)
        return str(obj)

    def __len__(self):
        obj = _get(self, OBJ)
        return len(obj)

    def __call__(self, *args, **kwargs):
        obj = _get(self, OBJ)
        return obj(*args, **kwargs)


register_data(DataObject)


def datawrap(v):
    type_ = _get(v, CLASS)

    if type_ is dict:
        m = _new(Data)
        _set(m, SLOT, v)  # INJECT m.__dict__=v SO THERE IS NO COPY
        return m
    elif type_ is tuple:
        return FlatList(v)
    elif type_ is list:
        return list_to_data(v)
    elif type_ in (Data, DataObject, FlatList, NullType):
        return v
    elif type_ in (none_type, text, binary_type, int, float, Decimal, datetime, date):
        return v
    elif type_ in generator_types:
        return (to_data(vv) for vv in v)
    elif isinstance(v, (text, binary_type, int, float, Decimal, datetime, date, FlatList, NullType, Mapping, none_type)):
        return v
    elif hasattr(v, "__data__"):
        return v.__data__()
    else:
        return DataObject(v)


class DictClass(object):
    """
    ALLOW INSTANCES OF class_ TO ACT LIKE dicts
    ALLOW CONSTRUCTOR TO ACCEPT @override
    """

    def __init__(self, class_):
        WRAPPED_CLASSES.add(class_)
        self.class_ = class_
        self.constructor = class_.__init__

    def __call__(self, *args, **kwargs):
        settings = to_data(kwargs).settings

        params = get_function_arguments(self.constructor)[1:]
        func_defaults = get_function_defaults(self.constructor)
        if not func_defaults:
            defaults = {}
        else:
            defaults = {k: v for k, v in zip(reversed(params), reversed(func_defaults))}

        ordered_params = dict(zip(params, args))

        output = self.class_(**params_pack(params, ordered_params, kwargs, settings, defaults))
        return DataObject(output)


def params_pack(params, *args):
    settings = {}
    for a in args:
        for k, v in a.items():
            k = text(k)
            if k in settings:
                continue
            settings[k] = v

    output = {str(k): from_data(settings[k]) for k in params if k in settings}
    return output


export("mo_dots.lists", datawrap)
