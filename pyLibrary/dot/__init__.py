# encoding: utf-8
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
from types import GeneratorType, NoneType, ModuleType

_get = object.__getattribute__


def inverse(d):
    """
    reverse the k:v pairs
    """
    output = {}
    for k, v in unwrap(d).iteritems():
        output[v] = output.get(v, [])
        output[v].append(k)
    return output


def coalesce(*args):
    # pick the first not null value
    # http://en.wikipedia.org/wiki/Null_coalescing_operator
    for a in args:
        if a != None:
            return wrap(a)
    return Null


def zip(keys, values):
    """
    CONVERT LIST OF KEY/VALUE PAIRS TO A DICT
    """
    output = Dict()
    for i, k in enumerate(keys):
        if i >= len(values):
            break
        output[k] = values[i]
    return output



def literal_field(field):
    """
    RETURN SAME WITH . ESCAPED
    """
    try:
        return field.replace(".", "\.")
    except Exception, e:
        from pyLibrary.debugs.logs import Log

        Log.error("bad literal", e)

def split_field(field):
    """
    RETURN field AS ARRAY OF DOT-SEPARATED FIELDS
    """
    if field.find(".") >= 0:
        field = field.replace("\.", "\a")
        return [k.replace("\a", ".") for k in field.split(".")]
    else:
        return [field]


def join_field(field):
    """
    RETURN field SEQUENCE AS STRING
    """
    return ".".join([f.replace(".", "\.") for f in field])


def hash_value(v):
    if isinstance(v, (set, tuple, list)):
        return hash(tuple(hash_value(vv) for vv in v))
    elif not isinstance(v, Mapping):
        return hash(v)
    else:
        return hash(tuple(sorted(hash_value(vv) for vv in v.values())))



def _setdefault(obj, key, value):
    """
    DO NOT USE __dict__.setdefault(obj, key, value), IT DOES NOT CHECK FOR obj[key] == None
    """
    v = obj.get(key)
    if v == None:
        obj[key] = value
        return value
    return v


def set_default(*params):
    """
    INPUT dicts IN PRIORITY ORDER
    UPDATES FIRST dict WITH THE MERGE RESULT, WHERE MERGE RESULT IS DEFINED AS:
    FOR EACH LEAF, RETURN THE HIGHEST PRIORITY LEAF VALUE
    """
    p0 = params[0]
    agg = p0 if p0 or isinstance(p0, Mapping) else {}
    for p in params[1:]:
        p = unwrap(p)
        if p is None:
            continue
        _all_default(agg, p, seen={})
    return wrap(agg)


def _all_default(d, default, seen=None):
    """
    ANY VALUE NOT SET WILL BE SET BY THE default
    THIS IS RECURSIVE
    """
    if default is None:
        return
    for k, default_value in wrap(default).items():
        # existing_value = d.get(k)
        existing_value = _get_attr(d, [k])

        if existing_value == None:
            if default_value != None:
                _set_attr(d, [k], default_value)
        elif (hasattr(existing_value, "__setattr__") or isinstance(existing_value, Mapping)) and isinstance(default_value, Mapping):
            df = seen.get(id(existing_value))
            if df:
                _set_attr(d, [k], df)
            else:
                seen[id(existing_value)] = default_value
                _all_default(existing_value, default_value, seen)


def _getdefault(obj, key):
    """
    TRY BOTH ATTRIBUTE AND ITEM ACCESS, OR RETURN Null
    """
    try:
        return getattr(obj, key)
    except Exception, e:
        pass

    try:
        return obj[key]
    except Exception, f:
        pass

    try:
        if float(key) == round(float(key), 0):
            return obj[int(key)]
    except Exception, f:
        pass

    # TODO: FIGURE OUT WHY THIS WAS EVER HERE (AND MAKE A TEST)
    # try:
    #     return eval("obj."+unicode(key))
    # except Exception, f:
    #     pass
    return NullType(obj, key)


PATH_NOT_FOUND = "Path not found"
AMBIGUOUS_PATH_FOUND = "Path is ambiguous"


def set_attr(obj, path, value):
    """
    SAME AS object.__setattr__(), BUT USES DOT-DELIMITED path
    RETURN OLD VALUE
    """
    try:
        return _set_attr(obj, split_field(path), value)
    except Exception, e:
        from pyLibrary.debugs.logs import Log
        if PATH_NOT_FOUND in e:
            Log.warning(PATH_NOT_FOUND + ": {{path}}",  path= path)
        else:
            Log.error("Problem setting value", e)


def get_attr(obj, path):
    """
    SAME AS object.__getattr__(), BUT USES DOT-DELIMITED path
    """
    try:
        return _get_attr(obj, split_field(path))
    except Exception, e:
        from pyLibrary.debugs.logs import Log
        if PATH_NOT_FOUND in e:
            Log.error(PATH_NOT_FOUND+": {{path}}",  path=path, cause=e)
        else:
            Log.error("Problem setting value", e)


def _get_attr(obj, path):
    if not path:
        return obj

    attr_name = path[0]

    if isinstance(obj, ModuleType):
        if attr_name in obj.__dict__:
            return _get_attr(obj.__dict__[attr_name], path[1:])
        elif attr_name in dir(obj):
            return _get_attr(obj[attr_name], path[1:])

        # TRY FILESYSTEM
        from pyLibrary.env.files import File
        if File.new_instance(File(obj.__file__).parent, attr_name).set_extension("py").exists:
            try:
                # THIS CASE IS WHEN THE __init__.py DOES NOT IMPORT THE SUBDIR FILE
                # WE CAN STILL PUT THE PATH TO THE FILE IN THE from CLAUSE
                if len(path)==1:
                    #GET MODULE OBJECT
                    output = __import__(obj.__name__ + "." + attr_name, globals(), locals(), [path[0]], 0)
                    return output
                else:
                    #GET VARIABLE IN MODULE
                    output = __import__(obj.__name__ + "." + attr_name, globals(), locals(), [path[1]], 0)
                    return _get_attr(output, path[1:])
            except Exception, e:
                pass

        # TRY A CASE-INSENSITIVE MATCH
        attr_name = lower_match(attr_name, dir(obj))
        if not attr_name:
            from pyLibrary.debugs.logs import Log
            Log.error(PATH_NOT_FOUND)
        elif len(attr_name)>1:
            from pyLibrary.debugs.logs import Log
            Log.error(AMBIGUOUS_PATH_FOUND+" {{paths}}",  paths=attr_name)
        else:
            return _get_attr(obj[attr_name[0]], path[1:])
    try:
        obj = getattr(obj, attr_name)
        return _get_attr(obj, path[1:])
    except Exception, e:
        try:
            obj = obj[attr_name]
            return _get_attr(obj, path[1:])
        except Exception, f:
            return None


def _set_attr(obj, path, value):
    obj = _get_attr(obj, path[:-1])
    if obj is None:  # DELIBERATE, WE DO NOT WHAT TO CATCH Null HERE (THEY CAN BE SET)
        from pyLibrary.debugs.logs import Log
        Log.error(PATH_NOT_FOUND)

    attr_name = path[-1]

    # ACTUAL SETTING OF VALUE
    try:
        old_value = _get_attr(obj, [attr_name])
        if old_value == None:
            old_value = None
            new_value = value
        else:
            new_value = old_value.__class__(value)  # TRY TO MAKE INSTANCE OF SAME CLASS
    except Exception, e:
        old_value = None
        new_value = value

    try:
        _get(obj, "__setattr__")(attr_name, new_value)
        return old_value
    except Exception, e:
        try:
            obj[attr_name] = new_value
            return old_value
        except Exception, f:
            from pyLibrary.debugs.logs import Log
            Log.error(PATH_NOT_FOUND)


def lower_match(value, candidates):
    return [v for v in candidates if v.lower()==value.lower()]


def wrap(v):
    type_ = _get(v, "__class__")

    if type_ is dict:
        m = Dict(v)
        return m
    elif type_ is NoneType:
        return Null
    elif type_ is list:
        return DictList(v)
    elif type_ is GeneratorType:
        return (wrap(vv) for vv in v)
    else:
        return v


def wrap_dot(value):
    """
    dict WITH DOTS IN KEYS IS INTERPRETED AS A PATH
    """
    return wrap(_wrap_dot(value))


def _wrap_dot(value):
    if value == None:
        return None
    if isinstance(value, (basestring, int, float)):
        return value
    if isinstance(value, Mapping):
        if isinstance(value, Dict):
            value = unwrap(value)

        output = {}
        for key, value in value.iteritems():
            value = _wrap_dot(value)

            if key == "":
                from pyLibrary.debugs.logs import Log

                Log.error("key is empty string.  Probably a bad idea")
            if isinstance(key, str):
                key = key.decode("utf8")

            d = output
            if key.find(".") == -1:
                if value is None:
                    d.pop(key, None)
                else:
                    d[key] = value
            else:
                seq = split_field(key)
                for k in seq[:-1]:
                    e = d.get(k, None)
                    if e is None:
                        d[k] = {}
                        e = d[k]
                    d = e
                if value == None:
                    d.pop(seq[-1], None)
                else:
                    d[seq[-1]] = value
        return output
    if hasattr(value, '__iter__'):
        output = []
        for v in value:
            v = wrap_dot(v)
            output.append(v)
        return output
    return value


def unwrap(v):
    _type = _get(v, "__class__")
    if _type is Dict:
        d = _get(v, "_dict")
        return d
    elif _type is DictList:
        return v.list
    elif _type is NullType:
        return None
    elif _type is GeneratorType:
        return (unwrap(vv) for vv in v)
    else:
        return v


def listwrap(value):
    """
    PERFORMS THE FOLLOWING TRANSLATION
    None -> []
    value -> [value]
    [...] -> [...]  (unchanged list)

    ##MOTIVATION##
    OFTEN IT IS NICE TO ALLOW FUNCTION PARAMETERS TO BE ASSIGNED A VALUE,
    OR A list-OF-VALUES, OR NULL.  CHECKING FOR WHICH THE CALLER USED IS
    TEDIOUS.  INSTEAD WE CAST FROM THOSE THREE CASES TO THE SINGLE CASE
    OF A LIST

    # BEFORE
    def do_it(a):
        if a is None:
            return
        if not isinstance(a, list):
            a=[a]
        for x in a:
            # do something

    # AFTER
    def do_it(a):
        for x in listwrap(a):
            # do something

    """
    if value == None:
        return []
    elif isinstance(value, list):
        return wrap(value)
    else:
        return wrap([unwrap(value)])

def unwraplist(v):
    """
    LISTS WITH ZERO AND ONE element MAP TO None AND element RESPECTIVELY
    """
    if isinstance(v, list):
        if len(v) == 0:
            return None
        elif len(v) == 1:
            return unwrap(v[0])
        else:
            return unwrap(v)
    else:
        return unwrap(v)


def tuplewrap(value):
    """
    INTENDED TO TURN lists INTO tuples FOR USE AS KEYS
    """
    if isinstance(value, (list, set, tuple, GeneratorType)):
        return tuple(tuplewrap(v) if isinstance(v, (list, tuple, GeneratorType)) else v for v in value)
    return unwrap(value),


from pyLibrary.dot.nones import Null, NullType
from pyLibrary.dot.dicts import Dict
from pyLibrary.dot.lists import DictList
