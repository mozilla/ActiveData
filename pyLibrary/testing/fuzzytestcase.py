# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from collections import Mapping

import unittest
from pyLibrary import dot
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce, Dict, literal_field
from pyLibrary.maths import Math
from pyLibrary.dot import wrap
from pyLibrary.strings import expand_template


class FuzzyTestCase(unittest.TestCase):
    """
    COMPARE STRUCTURE AND NUMBERS!

    ONLY THE ATTRIBUTES IN THE expected STRUCTURE ARE TESTED TO EXIST
    EXTRA ATTRIBUTES ARE IGNORED.

    NUMBERS ARE MATCHED BY ...
    * places (UP TO GIVEN SIGNIFICANT DIGITS)
    * digits (UP TO GIVEN DECIMAL PLACES, WITH NEGATIVE MEANING LEFT-OF-UNITS)
    * delta (MAXIMUM ABSOLUTE DIFFERENCE FROM expected)
    """

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.default_places=15


    def set_default_places(self, places):
        """
        WHEN COMPARING float, HOW MANY DIGITS ARE SIGNIFICANT BY DEFAULT
        """
        self.default_places=places

    def assertAlmostEqual(self, test_value, expected, msg=None, digits=None, places=None, delta=None):
        if delta or digits:
            assertAlmostEqual(test_value, expected, msg=msg, digits=digits, places=places, delta=delta)
        else:
            assertAlmostEqual(test_value, expected, msg=msg, digits=digits, places=coalesce(places, self.default_places), delta=delta)

    def assertEqual(self, test_value, expected, msg=None, digits=None, places=None, delta=None):
        self.assertAlmostEqual(test_value, expected, msg=msg, digits=digits, places=places, delta=delta)


def zipall(*args):
    """
    LOOP THROUGH LONGEST OF THE LISTS, None-FILL THE REMAINDER
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
    show_detail=True
    try:
        if test==None and expected==None:
            return
        elif isinstance(expected, Mapping):
            for k, v2 in expected.items():
                if isinstance(k, basestring):
                    v1 = dot.get_attr(test, literal_field(k))
                else:
                    show_deta=False
                    v1 = test[k]
                assertAlmostEqual(v1, v2, msg=msg, digits=digits, places=places, delta=delta)
        elif isinstance(test, set) and isinstance(expected, set):
            if test != expected:
                Log.error("Sets do not match")
        elif hasattr(test, "__iter__") and hasattr(expected, "__iter__"):
            for a, b in zipall(test, expected):
                assertAlmostEqual(a, b, msg=msg, digits=digits, places=places, delta=delta)
        else:
            assertAlmostEqualValue(test, expected, msg=msg, digits=digits, places=places, delta=delta)
    except Exception, e:
        Log.error(
            "{{test|json}} does not match expected {{expected|json}}",
            test=test if show_detail else "[can not show]",
            expected=expected if show_detail else "[can not show]",
            cause=e
        )


def assertAlmostEqualValue(test, expected, digits=None, places=None, msg=None, delta=None):
    """
    Snagged from unittest/case.py, then modified (Aug2014)
    """
    if test == expected:
        # shortcut
        return

    if not Math.is_number(expected):
        # SOME SPECIAL CASES, EXPECTING EMPTY CONTAINERS IS THE SAME AS EXPECTING NULL
        if isinstance(expected, list) and len(expected)==0 and test == None:
            return
        if isinstance(expected, Mapping) and not expected.keys() and test == None:
            return
        if test != expected:
            raise AssertionError(expand_template("{{test}} != {{expected}}", locals()))
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

        standardMsg = expand_template("{{test|json}} != {{expected|json}} within {{places}} places", locals())

    raise AssertionError(coalesce(msg, "") + ": (" + standardMsg + ")")
