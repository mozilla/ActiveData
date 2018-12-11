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

from copy import copy

Expression = None
expression_module = None
JX = None


def first(values):
    return iter(values).next()


def _gen_ids():
    id = 0
    while (True):
        yield id
        id += 1


_ids = _gen_ids()


def all_bases(bases):
    for b in bases:
        yield b
        for y in all_bases(b.__bases__):
            yield y


# EVERY OPERATOR WILL HAVE lang WHICH POINTS TO LANGUAGE
class LanguageElement(type):
    def __new__(cls, name, bases, dct):
        x = type.__new__(cls, name, bases, dct)
        x.lang = None
        if x.__module__ == expression_module:
            # ALL OPS IN expression_module ARE GIVEN AN ID
            x.id = _ids.next()
        return x

    def __init__(cls, *args):
        global Expression, expression_module
        type.__init__(cls, *args)
        if not expression_module and cls.__name__ == "Expression":
            # THE expression_module IS DETERMINED BY THE LOCATION OF Expression CLASS
            Expression = cls
            expression_module = cls.__module__


BaseExpression = LanguageElement(str("BaseExpression"), (object,), {})


class Language(object):

    def __init__(self, name):
        self.name = name
        self.ops = None

    def __getitem__(self, item):
        class_ = self.ops[item.id]
        item.__class__ = class_
        return item

    def __str__(self):
        return self.name


def define_language(lang_name, module_vars):
    # LET ALL EXPRESSIONS POINT TO lang OBJECT WITH ALL EXPRESSIONS
    # ENSURE THIS IS BELOW ALL SUB_CLASS DEFINITIONS SO var() CAPTURES ALL EXPRESSIONS
    global JX

    if lang_name:
        language = Language(lang_name)
        language.ops = copy(JX.ops)
    else:
        num_ops = 1 + max(
            obj.id
            for obj in module_vars.values() if isinstance(obj, type) and hasattr(obj, 'id')
        )
        language = JX = Language("JX")
        language.ops = [None] * num_ops

    for _, new_op in module_vars.items():
        if isinstance(new_op, type) and hasattr(new_op, 'id'):
            # EXPECT OPERATORS TO HAVE id
            # EXPECT NEW DEFINED OPS IN THIS MODULE TO HAVE lang NOT SET
            curr = getattr(new_op, "lang")
            if not curr:
                language.ops[new_op.id] = new_op
                setattr(new_op, "lang", language)

    if lang_name:
        # ENSURE THE ALL OPS ARE DEFINED ON THE NEW LANGUAGE
        for base_op, new_op in list(zip(JX.ops, language.ops)):
            if new_op is base_op:
                # MISSED DEFINITION, ADD ONE
                new_op = type(base_op.__name__, (base_op,), {})
                language.ops[new_op.id] = new_op
                setattr(new_op, "lang", language)



    return language

