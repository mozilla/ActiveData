# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

import unittest
from pyLibrary.dot import nvl
from pyLibrary.maths import Math
from pyLibrary.dot import wrap
from pyLibrary.strings import expand_template


class FuzzyTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.default_places=15


    def set_default_places(self, places):
        """
        WHEN COMPARING float, HOW MANY DIGITS ARE SIGNIFICANT BY DEFAULT
        """
        self.default_places=places

    def assertAlmostEqual(self, first, second, msg=None, digits=None, places=None, delta=None):
        if delta or digits:
            assertAlmostEqual(first, second, msg=msg, digits=digits, places=places, delta=delta)
        else:
            assertAlmostEqual(first, second, msg=msg, digits=digits, places=nvl(places, self.default_places), delta=delta)

    def assertEqual(self, first, second, msg=None, digits=None, places=None, delta=None):
        self.assertAlmostEqual(first, second, msg=msg, digits=digits, places=places, delta=delta)


def zipall(*args):
    """
    LOOP THROUGH LONGEST OF THE LISTS
    """
    iters = [a.__iter__() for a in args]

    def _next(_iter):
        try:
            return False, _iter.next()
        except:
            return True, None

    while True:
        output = zip(*(_next(a) for a in iters))
        if all(output[0]):
            return
        else:
            yield output[1]


def assertAlmostEqual(test, expected, digits=None, places=None, msg=None, delta=None):
    if isinstance(expected, dict):
        test = wrap({"value": test})
        expected = wrap(expected)
        for k, v2 in expected.items():
            v1 = test["value." + unicode(k)]
            assertAlmostEqual(v1, v2, msg=msg, digits=digits, places=places, delta=delta)
    elif hasattr(test, "__iter__") and hasattr(expected, "__iter__"):
        for a, b in zipall(test, expected):
            assertAlmostEqual(a, b, msg=msg, digits=digits, places=places, delta=delta)

    else:
        assertAlmostEqualValue(test, expected, msg=msg, digits=digits, places=places, delta=delta)


def assertAlmostEqualValue(test, expected, digits=None, places=None, msg=None, delta=None):
    """
    Snagged from unittest/case.py, then modified (Aug2014)
    """
    if test == expected:
        # shortcut
        return

    num_param = 0
    if digits != None:
        num_param += 1
    if places != None:
        num_param += 1
    if delta != None:
        num_param += 1
    if num_param>1:
        raise TypeError("specify only one of digits, places or delta")

    if digits is not None:
        try:
            diff = Math.log10(abs(test-expected))
            if diff < digits:
                return
        except Exception, e:
            pass

        standardMsg = expand_template("{{test}} != {{expected}} within {{digits}} decimal places", locals())
    elif delta is not None:
        if abs(test - expected) <= delta:
            return

        standardMsg = expand_template("{{test}} != {{expected}} within {{delta}} delta", locals())
    else:
        if places is None:
            places = 15

        try:
            diff = Math.log10(abs(test-expected))
            if diff < Math.ceiling(Math.log10(abs(test)))-places:
                return
        except Exception, e:
            pass

        standardMsg = expand_template("{{test}} != {{expected}} within {{places}} places", locals())

    raise AssertionError(nvl(msg, "") + ": (" + standardMsg + ")")




