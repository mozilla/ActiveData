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
from pyLibrary import dot
from pyLibrary.debugs.logs import Log, Except
from pyLibrary.dot import unwrap, set_default, wrap


def new_instance(settings):
    """
    MAKE A PYTHON INSTANCE

    settings HAS ALL THE kwargs, PLUS class ATTRIBUTE TO INDICATE THE CLASS TO CREATE
    """
    settings = set_default({}, settings)
    if not settings["class"]:
        Log.error("Expectiong 'class' attribute with fully qualified class name")

    # IMPORT MODULE FOR HANDLER
    path = settings["class"].split(".")
    class_name = path[-1]
    path = ".".join(path[:-1])
    constructor = None
    try:
        temp = __import__(path, globals(), locals(), [class_name], -1)
        constructor = object.__getattribute__(temp, class_name)
    except Exception, e:
        Log.error("Can not find class {{class}}", {"class": path}, e)

    settings['class'] = None
    try:
        return constructor(settings=settings)  # MAYBE IT TAKES A SETTINGS OBJECT
    except Exception:
        pass

    try:
        return constructor(**unwrap(settings))
    except Exception, e:
        Log.error("Can not create instance of {{name}}", {"name": ".".join(path)}, e)


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
        Log.error("Can not find function {{name}}", {"name": full_name}, e)


def use_settings(func):
    """
    THIS DECORATOR WILL PUT ALL PARAMETERS INTO THE settings PARAMETER AND
    PUT ALL settings PARAMETERS INTO THE FUNCTION PARAMETERS.  THIS HAS BOTH
    THE BENEFIT OF HAVING ALL PARAMETERS IN ONE PLACE (settings) AND ALL
    PARAMETERS ARE EXPLICIT FOR CLARITY.

    PARAMETER ASSIGNMENT MAY NOT BE UNIQUE, THEY CAN COME FROM EXPLICIT
    CALL PARAMETERS, OR FROM THE settings PARAMETER.  IN THESE CASES,
    PARAMETER VALUES ARE CHOSEN IN THE FOLLOWING ORDER:
    1) EXPLICT CALL PARAMETERS
    2) PARAMETERS FOUND IN settings
    3) DEFAULT VALUES ASSIGNED IN FUNCTION DEFINITION
    """

    params = func.func_code.co_varnames[:func.func_code.co_argcount]
    if not func.func_defaults:
        defaults={}
    else:
        defaults = {k: v for k, v in zip(reversed(params), reversed(func.func_defaults))}
    if "settings" not in params:
        Log.error("Must have an optional 'settings' parameter, which will be "
                  "filled with dict of all parameter {name:value} pairs")

    def wrapper(*args, **kwargs):
        try:
            if func.func_name == "__init__" and "settings" in kwargs:
                packed = params_pack(params, kwargs, dot.zip(params[1:], args[1:]), kwargs["settings"], defaults)
                return func(args[0], **packed)
            elif func.func_name == "__init__" and len(args) == 2 and len(kwargs) == 0 and isinstance(args[1], dict):
                # ASSUME SECOND UNNAMED PARAM IS settings
                packed = params_pack(params, args[1], defaults)
                return func(args[0], **packed)
            elif func.func_name == "__init__":
                # DO NOT INCLUDE self IN SETTINGS
                packed = params_pack(params, kwargs, dot.zip(params[1:], args[1:]), defaults)
                return func(args[0], **packed)
            elif params[0] == "self" and "settings" in kwargs:
                packed = params_pack(params, kwargs, dot.zip(params[1:], args[1:]), kwargs["settings"], defaults)
                return func(args[0], **packed)
            elif params[0] == "self" and len(args) == 2 and len(kwargs) == 0 and isinstance(args[1], dict):
                # ASSUME SECOND UNNAMED PARAM IS settings
                packed = params_pack(params, args[1], defaults)
                return func(args[0], **packed)
            elif params[0] == "self":
                packed = params_pack(params, kwargs, dot.zip(params[1:], args[1:]), defaults)
                return func(args[0], **packed)
            elif len(args) == 1 and len(kwargs) == 0 and isinstance(args[0], dict):
                # ASSUME SINGLE PARAMETER IS A SETTING
                packed = params_pack(params, args[0], defaults)
                return func(**packed)
            elif "settings" in kwargs and isinstance(kwargs["settings"], dict):
                # PUT args INTO SETTINGS
                packed = params_pack(params, kwargs, dot.zip(params, args), kwargs["settings"], defaults)
                return func(**packed)
            else:
                # PULL SETTINGS OUT INTO PARAMS
                packed = params_pack(params, kwargs, dot.zip(params, args), defaults)
                return func(**packed)
        except TypeError, e:
            if e.message.find("takes at least")>=0:
                missing = [p for p in params if str(p) not in packed]

                Log.error("Problem calling {{func_name}}:  Expecting parameter {{missing}}", {
                    "func_name": func.func_name,
                    "missing": missing
                }, e, stack_depth=1)
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


