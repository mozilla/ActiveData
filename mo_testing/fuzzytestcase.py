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

import types
import unittest
from collections import Mapping

import mo_dots
from mo_collections.unique_index import UniqueIndex
from mo_dots import coalesce, literal_field, unwrap
from mo_logs import Log
from mo_logs.exceptions import suppress_exception, Except
from mo_logs.strings import expand_template
from mo_math import Math


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

    def assertRaises(self, problem, function, *args, **kwargs):
        try:
            function(*args, **kwargs)
        except Exception as e:
            f = Except.wrap(e)
            if isinstance(problem, basestring):
                if problem in f:
                    return
                Log.error(
                    "expecting an exception returning {{problem|quote}} got something else instead",
                    problem=problem,
                    cause=f
                )
            elif not isinstance(f, problem) and not isinstance(e, problem):
                Log.error("expecting an exception of type {{type}} to be raised", type=problem)
            else:
                return

        Log.error("Expecting an exception to be raised")

def zipall(*args):
    """
    LOOP THROUGH LONGEST OF THE LISTS, None-FILL THE REMAINDER
    """
    iters = [iter(a) for a in args]

    def _next(_iter):
        try:
            return False, _iter.next()
        except:
            return True, None

    while True:
        is_done, value = zip(*(_next(a) for a in iters))
        if all(is_done):
            return
        else:
            yield value


def assertAlmostEqual(test, expected, digits=None, places=None, msg=None, delta=None):
    show_detail = True
    test = unwrap(test)
    expected = unwrap(expected)
    try:
        if test is None and expected is None:
            return
        elif isinstance(test, UniqueIndex):
            if test ^ expected:
                Log.error("Sets do not match")
        elif isinstance(expected, Mapping) and isinstance(test, Mapping):
            for k, v2 in unwrap(expected).items():
                v1 = test.get(k)
                assertAlmostEqual(v1, v2, msg=msg, digits=digits, places=places, delta=delta)
        elif isinstance(expected, Mapping):
            for k, v2 in expected.items():
                if isinstance(k, basestring):
                    v1 = mo_dots.get_attr(test, literal_field(k))
                else:
                    v1 = test[k]
                assertAlmostEqual(v1, v2, msg=msg, digits=digits, places=places, delta=delta)
        elif isinstance(test, (set, list)) and isinstance(expected, set):
            test = set(test)
            if len(test) != len(expected):
                Log.error(
                    "Sets do not match, element count different:\n{{test|json|indent}}\nexpecting{{expectedtest|json|indent}}",
                    test=test,
                    expected=expected
                )

            for e in expected:
                for t in test:
                    try:
                        assertAlmostEqual(t, e, msg=msg, digits=digits, places=places, delta=delta)
                        break
                    except Exception, _:
                        pass
                else:
                    Log.error("Sets do not match. {{value|json}} not found in {{test|json}}", value=e, test=test)

        elif isinstance(expected, types.FunctionType):
            return expected(test)
        elif hasattr(test, "__iter__") and hasattr(expected, "__iter__"):
            if test == None and not expected:
                return
            if expected == None:
                expected = []  # REPRESENT NOTHING
            for a, b in zipall(test, expected):
                assertAlmostEqual(a, b, msg=msg, digits=digits, places=places, delta=delta)
        else:
            assertAlmostEqualValue(test, expected, msg=msg, digits=digits, places=places, delta=delta)
    except Exception as e:
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
    if expected.__class__.__name__ == "NullOp":
        if test == None:
            return
        else:
            raise AssertionError(expand_template("{{test}} != {{expected}}", locals()))

    if expected == None:  # None has no expectations
        return
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
        with suppress_exception:
            diff = Math.log10(abs(test-expected))
            if diff < digits:
                return

        standardMsg = expand_template("{{test}} != {{expected}} within {{digits}} decimal places", locals())
    elif delta is not None:
        if abs(test - expected) <= delta:
            return

        standardMsg = expand_template("{{test}} != {{expected}} within {{delta}} delta", locals())
    else:
        if places is None:
            places = 15

        with suppress_exception:
            diff = Math.log10(abs(test-expected))
            if diff < Math.ceiling(Math.log10(abs(test)))-places:
                return


        standardMsg = expand_template("{{test|json}} != {{expected|json}} within {{places}} places", locals())

    raise AssertionError(coalesce(msg, "") + ": (" + standardMsg + ")")
