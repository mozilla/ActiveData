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

import math

from pyLibrary.collections.multiset import Multiset
from pyDots import Null


def reverse(values):
    """
    REVERSE - WITH NO SIDE EFFECTS!
    """
    output = list(values)
    output.reverse()
    return output


def MIN(values, *others):
    if len(others) > 0:
        from pyLibrary.debugs.logs import Log
        Log.error("no longer accepting args, use a single list")

    output = Null
    for v in values:
        if v == None:
            continue
        if isinstance(v, float) and math.isnan(v):
            continue
        if output == None:
            output = v
            continue
        output = min(output, v)
    return output


def MAX(values, *others):
    if len(others) > 0:
        from pyLibrary.debugs.logs import Log
        Log.error("Expecting an list, or iterable, only")

    output = Null
    for v in values:
        if v == None:
            continue
        if isinstance(v, float) and math.isnan(v):
            continue
        if output == None:
            output = v
            continue
        output = max(output, v)
    return output


def PRODUCT(values, *others):
    if len(others) > 0:
        from pyLibrary.debugs.logs import Log
        Log.error("no longer accepting args, use a single list")

    output = Null
    for v in values:
        if v == None:
            continue
        if isinstance(v, float) and math.isnan(v):
            continue
        if output == None:
            output = v
            continue
        output *= v
    return output


def COUNT(values, *others):
    if len(others) > 0:
        from pyLibrary.debugs.logs import Log
        Log.error("no longer accepting args, use a single list")

    output = 0
    for v in values:
        if v != None:
            output += 1
    return output


def SUM(values, *others):
    if len(others) > 0:
        from pyLibrary.debugs.logs import Log
        Log.error("no longer accepting args, use a single list")

    output = Null
    for v in values:
        if v == None:
            continue
        if isinstance(v, float) and math.isnan(v):
            continue
        if output == None:
            output = v
            continue
        output += v
    return output


def AND(values, *others):
    if len(others) > 0:
        from pyLibrary.debugs.logs import Log
        Log.error("no longer accepting args, use a single list")

    for v in values:
        if v == None:
            continue
        if not v:
            return False
    return True


def OR(values, *others):
    if len(others) > 0:
        from pyLibrary.debugs.logs import Log
        Log.error("no longer accepting args, use a single list")

    for v in values:
        if v == None:
            continue
        if v:
            return True
    return False


def UNION(values, *others):
    if len(others) > 0:
        from pyLibrary.debugs.logs import Log
        Log.error("no longer accepting args, use a single list")

    output = set()
    for v in values:
        if values == None:
            continue
        if isinstance(v, (list, set)):
            output.update(v)
            continue
        else:
            output.add(v)
    return output


def INTERSECT(values, *others):
    if len(others) > 0:
        from pyLibrary.debugs.logs import Log
        Log.error("no longer accepting args, use a single list")

    output = set(values[0])
    for v in values[1:]:
        output -= set(v)
        if not output:
            return output  # EXIT EARLY
    return output
