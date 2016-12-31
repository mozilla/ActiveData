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
from __future__ import absolute_import
from collections import Mapping
from types import FunctionType

import pyDots
from pyDots import set_default, wrap, _get_attr, Null, coalesce

from pyLibrary import convert
from pyLibrary.debugs.exceptions import Except, suppress_exception
from pyLibrary.debugs.logs import Log
from pyLibrary.maths.randoms import Random
from pyLibrary.strings import expand_template
from pyLibrary.thread.threads import Lock
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import DAY


def get_class(path):
    try:
        #ASSUME DIRECT FROM MODULE
        output = __import__(".".join(path[0:-1]), globals(), locals(), [path[-1]], 0)
        return _get_attr(output, path[-1:])
        # return output
    except Exception, e:
        from pyLibrary.debugs.logs import Log

        Log.error("Could not find module {{module|quote}}",  module= ".".join(path))


def new_instance(settings):
    """
    MAKE A PYTHON INSTANCE

    `settings` HAS ALL THE `kwargs`, PLUS `class` ATTRIBUTE TO INDICATE THE CLASS TO CREATE
    """
    settings = set_default({}, settings)
    if not settings["class"]:
        Log.error("Expecting 'class' attribute with fully qualified class name")

    # IMPORT MODULE FOR HANDLER
    path = settings["class"].split(".")
    class_name = path[-1]
    path = ".".join(path[:-1])
    constructor = None
    try:
        temp = __import__(path, globals(), locals(), [class_name], -1)
        constructor = object.__getattribute__(temp, class_name)
    except Exception, e:
        Log.error("Can not find class {{class}}", {"class": path}, cause=e)

    settings['class'] = None
    with suppress_exception:
        return constructor(settings=settings)  # MAYBE IT TAKES A SETTINGS OBJECT

    try:
        return constructor(**settings)
    except Exception, e:
        Log.error("Can not create instance of {{name}}", name=".".join(path), cause=e)


def get_function_by_name(full_name):
    """
    RETURN FUNCTION
    """

    # IMPORT MODULE FOR HANDLER
    path = full_name.split(".")
    function_name = path[-1]
    path = ".".join(path[:-1])
    constructor = None
    try:
        temp = __import__(path, globals(), locals(), [function_name], -1)
        output = object.__getattribute__(temp, function_name)
        return output
    except Exception, e:
        Log.error("Can not find function {{name}}",  name= full_name, cause=e)


def use_settings(func):
    """
    THIS DECORATOR WILL PUT ALL PARAMETERS INTO THE `settings` PARAMETER AND
    PUT ALL `settings` PARAMETERS INTO THE FUNCTION PARAMETERS.  THIS HAS BOTH
    THE BENEFIT OF HAVING ALL PARAMETERS IN ONE PLACE (settings) AND ALL
    PARAMETERS ARE EXPLICIT FOR CLARITY.

    OF COURSE, THIS MEANS PARAMETER ASSIGNMENT MAY NOT BE UNIQUE: VALUES CAN
    COME FROM EXPLICIT CALL PARAMETERS, OR FROM THE settings PARAMETER.  IN
    THESE CASES, PARAMETER VALUES ARE CHOSEN IN THE FOLLOWING ORDER:
    1) EXPLICT CALL PARAMETERS
    2) PARAMETERS FOUND IN settings
    3) DEFAULT VALUES ASSIGNED IN FUNCTION DEFINITION
    """

    params = func.func_code.co_varnames[:func.func_code.co_argcount]
    if not func.func_defaults:
        defaults = {}
    else:
        defaults = {k: v for k, v in zip(reversed(params), reversed(func.func_defaults))}

    if "settings" not in params:
        # WE ASSUME WE ARE ONLY ADDING A settings PARAMETER TO SOME REGULAR METHOD
        def w_settings(*args, **kwargs):
            settings = wrap(kwargs).settings

            params = func.func_code.co_varnames[:func.func_code.co_argcount]
            if not func.func_defaults:
                defaults = {}
            else:
                defaults = {k: v for k, v in zip(reversed(params), reversed(func.func_defaults))}

            ordered_params = dict(zip(params, args))

            return func(**params_pack(params, ordered_params, kwargs, settings, defaults))
        return w_settings

    def wrapper(*args, **kwargs):
        try:
            if func.func_name in ("__init__", "__new__") and "settings" in kwargs:
                packed = params_pack(params, kwargs, pyDots.zip(params[1:], args[1:]), kwargs["settings"], defaults)
                return func(args[0], **packed)
            elif func.func_name in ("__init__", "__new__") and len(args) == 2 and len(kwargs) == 0 and isinstance(args[1], Mapping):
                # ASSUME SECOND UNNAMED PARAM IS settings
                packed = params_pack(params, args[1], defaults)
                return func(args[0], **packed)
            elif func.func_name in ("__init__", "__new__"):
                # DO NOT INCLUDE self IN SETTINGS
                packed = params_pack(params, kwargs, pyDots.zip(params[1:], args[1:]), defaults)
                return func(args[0], **packed)
            elif params[0] == "self" and "settings" in kwargs:
                packed = params_pack(params, kwargs, pyDots.zip(params[1:], args[1:]), kwargs["settings"], defaults)
                return func(args[0], **packed)
            elif params[0] == "self" and len(args) == 2 and len(kwargs) == 0 and isinstance(args[1], Mapping):
                # ASSUME SECOND UNNAMED PARAM IS settings
                packed = params_pack(params, args[1], defaults)
                return func(args[0], **packed)
            elif params[0] == "self":
                packed = params_pack(params, kwargs, pyDots.zip(params[1:], args[1:]), defaults)
                return func(args[0], **packed)
            elif len(args) == 1 and len(kwargs) == 0 and isinstance(args[0], Mapping):
                # ASSUME SINGLE PARAMETER IS A SETTING
                packed = params_pack(params, args[0], defaults)
                return func(**packed)
            elif "settings" in kwargs and isinstance(kwargs["settings"], Mapping):
                # PUT args INTO SETTINGS
                packed = params_pack(params, kwargs, pyDots.zip(params, args), kwargs["settings"], defaults)
                return func(**packed)
            else:
                # PULL SETTINGS OUT INTO PARAMS
                packed = params_pack(params, kwargs, pyDots.zip(params, args), defaults)
                return func(**packed)
        except TypeError, e:
            if e.message.find("takes at least") >= 0:
                missing = [p for p in params if str(p) not in packed]

                Log.error(
                    "Problem calling {{func_name}}:  Expecting parameter {{missing}}",
                    func_name=func.func_name,
                    missing=missing,
                    stack_depth=1
                )
            Log.error("Unexpected", e)
    return wrapper


def params_pack(params, *args):
    settings = {}
    for a in args:
        for k, v in a.items():
            k = unicode(k)
            if k in settings:
                continue
            settings[k] = v
    settings["settings"] = wrap(settings)

    output = {str(k): settings[k] for k in params if k in settings}
    return output


class cache(object):

    """
    :param func: ASSUME FIRST PARAMETER OF `func` IS `self`
    :param duration: USE CACHE IF LAST CALL WAS LESS THAN duration AGO
    :param lock: True if you want multithreaded monitor (default False)
    :return:
    """

    def __new__(cls, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], FunctionType):
            func = args[0]
            return wrap_function(_SimpleCache(), func)
        else:
            return object.__new__(cls)

    def __init__(self, duration=DAY, lock=False):
        self.timeout = duration
        if lock:
            self.locker = Lock()
        else:
            self.locker = _FakeLock()

    def __call__(self, func):
        return wrap_function(self, func)


class _SimpleCache(object):

    def __init__(self):
        self.timeout = Null
        self.locker = _FakeLock()


def wrap_function(cache_store, func_):
    attr_name = "_cache_for_" + func_.__name__

    if func_.func_code.co_argcount > 0 and func_.func_code.co_varnames[0] == "self":
        using_self = True
        func = lambda self, *args: func_(self, *args)
    else:
        using_self = False
        func = lambda self, *args: func_(*args)

    def output(*args):
        with cache_store.locker:
            if using_self:
                self = args[0]
                args = args[1:]
            else:
                self = cache_store

            now = Date.now()
            try:
                _cache = getattr(self, attr_name)
            except Exception, _:
                _cache = {}
                setattr(self, attr_name, _cache)

            if Random.int(100) == 0:
                # REMOVE OLD CACHE
                _cache = {k: v for k, v in _cache.items() if v[0]==None or v[0] > now}
                setattr(self, attr_name, _cache)

            timeout, key, value, exception = _cache.get(args, (Null, Null, Null, Null))

        if now >= timeout:
            value = func(self, *args)
            with cache_store.locker:
                _cache[args] = (now + cache_store.timeout, args, value, None)
            return value

        if value == None:
            if exception == None:
                try:
                    value = func(self, *args)
                    with cache_store.locker:
                        _cache[args] = (now + cache_store.timeout, args, value, None)
                    return value
                except Exception, e:
                    e = Except.wrap(e)
                    with cache_store.locker:
                        _cache[args] = (now + cache_store.timeout, args, None, e)
                    raise e
            else:
                raise exception
        else:
            return value

    return output


# _repr = Repr()
# _repr.maxlevel = 3

def repr(obj):
    """
    JUST LIKE __builtin__.repr(), BUT WITH SOME REASONABLE LIMITS
    """
    return repr(obj)
    return _repr.repr(obj)


class _FakeLock():


    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def DataClass(name, columns):
    """
    Each column has {"name", "required", "nulls", "default", "type"} properties
    """
    columns = wrap([{"name": c, "required": True, "nulls": False, "type": object} if isinstance(c, basestring) else c for c in columns])
    slots = columns.name
    required = wrap(filter(lambda c: c.required and not c.nulls and not c.default, columns)).name
    nulls = wrap(filter(lambda c: c.nulls, columns)).name
    types = {c.name: coalesce(c.type, object) for c in columns}

    code = expand_template("""
from __future__ import unicode_literals
from collections import Mapping

meta = None
types_ = {{types}}

class {{name}}(Mapping):
    __slots__ = {{slots}}

    def __init__(self, **kwargs):
        if not kwargs:
            return

        for s in {{slots}}:
            setattr(self, s, kwargs.get(s, kwargs.get('default', Null)))

        missed = {{required}}-set(kwargs.keys())
        if missed:
            Log.error("Expecting properties {"+"{missed}}", missed=missed)

        illegal = set(kwargs.keys())-set({{slots}})
        if illegal:
            Log.error("{"+"{names}} are not a valid properties", names=illegal)

    def __getitem__(self, item):
        return getattr(self, item)

    def __setitem__(self, item, value):
        setattr(self, item, value)
        return self

    def __setattr__(self, item, value):
        if item not in {{slots}}:
            Log.error("{"+"{item|quote}} not valid attribute", item=item)
        #if not isinstance(value, types_[item]):
        #   Log.error("{"+"{item|quote}} not of type "+"{"+"{type}}", item=item, type=types_[item])
        if item=="nested_path" and (not isinstance(value, list) or len(value)==0):
            Log.error("expecting list for nested path")
        object.__setattr__(self, item, value)

    def __getattr__(self, item):
        Log.error("{"+"{item|quote}} not valid attribute", item=item)

    def __hash__(self):
        return object.__hash__(self)

    def __eq__(self, other):
        if isinstance(other, {{name}}) and dict(self)==dict(other) and self is not other:
            Log.error("expecting to be same object")
        return self is other

    def __dict__(self):
        return {k: getattr(self, k) for k in {{slots}}}

    def items(self):
        return ((k, getattr(self, k)) for k in {{slots}})

    def __copy__(self):
        _set = object.__setattr__
        output = object.__new__({{name}})
        {{assign}}
        return output

    def __iter__(self):
        return {{slots}}.__iter__()

    def __len__(self):
        return {{len_slots}}

    def __str__(self):
        return str({{dict}})

temp = {{name}}
""",
        {
            "name": name,
            "slots": "(" + (", ".join(convert.value2quote(s) for s in slots)) + ")",
            "required": "{" + (", ".join(convert.value2quote(s) for s in required)) + "}",
            "nulls": "{" + (", ".join(convert.value2quote(s) for s in nulls)) + "}",
            "len_slots": len(slots),
            "dict": "{" + (", ".join(convert.value2quote(s) + ": self." + s for s in slots)) + "}",
            "assign": "; ".join("_set(output, "+convert.value2quote(s)+", self."+s+")" for s in slots),
            "types": "{" + (",".join(convert.string2quote(k) + ": " + v.__name__ for k, v in types.items())) + "}"
        }
    )

    return _exec(code, name)


def _exec(code, name):
    temp = None
    exec (code)
    globals()[name] = temp
    return temp
