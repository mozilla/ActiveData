# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

from collections import deque
from copy import copy
from datetime import datetime
from decimal import Decimal
from math import isnan

from mo_dots import Data, data_types, listwrap, NullType, startswith_field
from mo_dots.lists import list_types, is_many
from mo_future import boolean_type, long, none_type, text, transpose, function_type, get_function_arguments
from mo_logs import Log
from mo_times import Date

builtin_tuple = tuple

Expression = None
expression_module = "jx_base.expressions"
JX = None
ID = "_op_id"

_next_language_id = 0
_next_operator_id = 0


def next_operator_id():
    global _next_operator_id
    try:
        return _next_operator_id
    finally:
        _next_operator_id += 1


def next_language_id():
    global _next_language_id
    try:
        return _next_language_id
    finally:
        _next_language_id += 1


def all_bases(bases):
    for b in bases:
        yield b
        for y in all_bases(b.__bases__):
            yield y


# PROBLEM: Every operator of every language may have its own partial_eval(lang)
# to ensure the assumptions of the language are accounted for
# this switching can not be done using __class__ switching because the
# operator instance must know what language it is resolving to, which
# requires multiple dispatch.
#

# SOLUTION: multiple dispatch on the (self, lang) parameter pair
#
# DETAILS: partial_eval(lang) demands a language argument so it can switch
# decisions based on language. You declare a class for every implemented
# operator in a language, and use inheritance if you wish.

# The register_ops() will review all operators in a module and build a
# dispatch array pointing to the classes' partial_eval(lang)
# THE partial_eval(lang) IN YOUR CLASS IS REMOVED; REPLACED WITH partial_eval
# DEFINED IN THIS MODULE
class LanguageElement(type):
    def __new__(cls, name, bases, dct):
        x = type.__new__(cls, name, bases, dct)
        x.lang = None
        x.lookups = {}
        if startswith_field(x.__module__, expression_module):
            # ALL OPS IN expression_module ARE GIVEN AN ID, NO OTHERS
            setattr(x, ID, next_operator_id())
        return x

    def __init__(self, *args):
        global Expression, expression_module
        type.__init__(self, *args)

        if not expression_module and self.__name__ == "Expression":
            # THE expression_module IS DETERMINED BY THE LOCATION OF Expression CLASS
            Expression = self
            expression_module = self.__module__


def partial_eval(self, lang):
    """
    DISPATCH TO CLASS-SPECIFIC partial_eval(lang)
    """
    try:
        if self.simplified:
            return self
        func = self.lookups["partial_eval"][lang.id]
        output = func(self, lang)
        output.simplified = True
        return output
    except Exception as cause:
        Log.error("Not expected", cause=cause)


def get_dispatcher_for(name):
    def dispatcher(self, lang):
        func = self.lookups[name][lang.id]
        output = func(self, lang)
        return output

    return dispatcher


BaseExpression = LanguageElement(
    str("BaseExpression"), (object,), {"partial_eval": partial_eval}
)


class Language(object):
    def __init__(self, name):
        global JX
        if not name:
            name = "JX"
            JX = self
        self.name = name
        self.ops = None
        self.id = next_language_id()

    def register_ops(self, module_vars):
        global JX

        if self.name != "JX":
            self.ops = copy(JX.ops)  # A COPY, IF ONLY TO KNOW IT WAS REPLACED
            expecting = tuple(sorted(set(self.ops[1].lookups.keys())))
        else:
            num_ops = 1 + max(
                obj.get_id()
                for obj in module_vars.values()
                if isinstance(obj, type) and hasattr(obj, ID)
            )
            self.ops = [None] * num_ops

            # FIND ALL MULTIDISPATCH
            expecting = set()
            for _, new_op in list(module_vars.items()):
                if is_Expression(new_op):
                    for name, member in vars(new_op).items():
                        try:
                            args = get_function_arguments(member)
                            if args[:2] == ("self", "lang"):
                                expecting.add(name)
                        except Exception as cause:
                            pass
            expecting = tuple(sorted(expecting))

        for _, new_op in list(module_vars.items()):
            if is_Expression(new_op):
                op_id = new_op.get_id()
                jx_op = JX.ops[op_id]
                # LET EACH LANGUAGE POINT TO OP CLASS
                self.ops[op_id] = new_op
                new_op.lang = self

                # ENSURE THE partial_eval IS REGISTERED
                if jx_op is None:
                    for expect in expecting:
                        member = extract_method(new_op, expect)
                        args = get_function_arguments(member)
                        if args[:2] != ("self", "lang"):
                            Log.error(
                                "{{module}}.{{clazz}}.{{name}} is expecting (self, lang) parameters, minimum",
                                module=new_op.__module__,
                                clazz=new_op.__name__,
                                name=expect
                            )
                        new_op.lookups[expect] = [member]
                elif jx_op.__name__ != new_op.__name__:
                    Log.error("Logic error")
                else:
                    new_op.lookups = jx_op.lookups
                    for expect in expecting:
                        member = extract_method(new_op, expect)
                        jx_op.lookups[expect] += [member]

                    # COPY OTHER DEFINED METHODS
                    others = list(vars(new_op).items())
                    for n, v in others:
                        if v is not None:
                            o = getattr(jx_op, n, None)
                            if o is None:
                                setattr(jx_op, n, v)
        if self.name == 'JX':
            # FINALLY, SWAP OUT THE BASE METHODS
            for expect in expecting:
                existing = getattr(BaseExpression, expect, None)
                if existing:
                    # USE BaseExpression WHEN AVAILABLE
                    setattr(Expression, expect, existing)
                else:
                    # MAKE A DISPATCHER, IF NOT ONE ALREADY
                    setattr(Expression, expect, get_dispatcher_for(expect))

        else:
            # ENSURE THE ALL OPS ARE DEFINED ON THE NEW LANGUAGE
            for base_op, new_op in transpose(JX.ops, self.ops):
                if base_op and new_op is base_op:
                    # MISSED DEFINITION, ADD ONE
                    new_op = type(base_op.__name__, (base_op,), {})
                    self.ops[new_op.get_id()] = new_op
                    setattr(new_op, "lookups", base_op.lookups)
                    for n, v in base_op.lookups.items():
                        v += v[-1:]

        # ENSURE THIS LANGUAGE INSTANCE POINTS TO ALL THE OPS BY NAME
        for o in self.ops[1:]:
            setattr(self, o.__name__, o)

    def __getitem__(self, item):
        if item == None:
            Log.error("expecting operator")
        class_ = self.ops[item.get_id()]
        if class_.__name__ != item.__class__.__name__:
            Log.error("programming error")
        item.__class__ = class_
        return item

    def __str__(self):
        return self.name


def is_op(call, op):
    """
    :param call: The specific operator instance (a method call)
    :param op: The the operator we are testing against
    :return: isinstance(call, op), but faster
    """
    try:
        return call.get_id() == op.get_id()
    except Exception as e:
        return False


def is_expression(call):
    if is_many(call):
        return False
    try:
        output = getattr(call, ID, None) != None
    except Exception:
        output = False
    # if output != isinstance(call, Expression):
    #     Log.error("programmer error")
    return output


def value_compare(left, right, ordering=1):
    """
    SORT VALUES, NULL IS THE LEAST VALUE
    :param left: LHS
    :param right: RHS
    :param ordering: (-1, 0, 1) TO AFFECT SORT ORDER
    :return: The return value is negative if x < y, zero if x == y and strictly positive if x > y.
    """

    if left is right:
        return 0

    try:
        ltype = left.__class__
        rtype = right.__class__

        if ltype in list_types or rtype in list_types:
            if left == None:
                return ordering
            elif right == None:
                return -ordering

            left = listwrap(left)
            right = listwrap(right)
            for a, b in zip(left, right):
                c = value_compare(a, b) * ordering
                if c != 0:
                    return c

            if len(left) < len(right):
                return -ordering
            elif len(left) > len(right):
                return ordering
            else:
                return 0

        if ltype is float and isnan(left):
            left = None
            ltype = none_type
        if rtype is float and isnan(right):
            right = None
            rtype = none_type

        ltype_num = type_order(ltype, ordering)
        rtype_num = type_order(rtype, ordering)

        type_diff = ltype_num - rtype_num
        if type_diff != 0:
            return ordering if type_diff > 0 else -ordering

        if ltype_num in (-10, 10):
            return 0
        elif ltype is builtin_tuple:
            for a, b in zip(left, right):
                c = value_compare(a, b)
                if c != 0:
                    return c * ordering
            return 0
        elif ltype in data_types:
            for k in sorted(set(left.keys()) | set(right.keys())):
                c = value_compare(left.get(k), right.get(k)) * ordering
                if c != 0:
                    return c
            return 0
        elif ltype is function_type:
            return 0
        elif left > right:
            return ordering
        elif left < right:
            return -ordering
        else:
            return 0
    except Exception as e:
        Log.error(
            "Can not compare values {{left}} to {{right}}",
            left=left,
            right=right,
            cause=e,
        )


def type_order(dtype, ordering):
    o = TYPE_ORDER.get(dtype)
    if o is None:
        if dtype in NULL_TYPES:
            return ordering * 10
        else:
            Log.warning("type will be treated as its own type while sorting")
            TYPE_ORDER[dtype] = 6
            return 6
    return o


NULL_TYPES = (none_type, NullType)


TYPE_ORDER = {
    boolean_type: 0,
    int: 1,
    float: 1,
    Decimal: 1,
    Date: 1,
    datetime: 1,
    long: 1,
    text: 3,
    list: 4,
    builtin_tuple: 4,
    dict: 5,
    Data: 5,
}


all_removed_methods = {}


def extract_method(cls, name):
    # ENSURE WE TERMINATE AT ORIGINAL Expression METHODS
    to_do = deque([cls])
    while to_do:
        cls = to_do.popleft()
        if cls is BaseExpression:
            return None
        method = all_removed_methods.get((cls.__module__, cls.__name__, name))
        if method:
            return method
        if name in vars(cls):
            method = getattr(cls, name)
            all_removed_methods[(cls.__module__, cls.__name__, name)] = method
            delattr(cls, name)
            return method
        to_do.extend(cls.__bases__)
    return None


def is_Expression(cls):
    global Expression

    if Expression:
        to_do = deque([cls])
        while to_do:
            cls = to_do.popleft()
            try:
                bases = cls.__bases__
                if Expression in bases:
                    return True
                to_do.extend(bases)
            except Exception:
                return False
    else:
        to_do = deque([cls])
        while to_do:
            cls = to_do.popleft()
            try:
                bases = cls.__bases__
                if BaseExpression in bases:
                    Expression = cls
                    return True
                to_do.extend(bases)
            except Exception:
                return False
    return False
