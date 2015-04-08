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

from pyLibrary.queries.expressions import get_all_vars, simplify_esfilter, where2esfilter
from pyLibrary.testing.fuzzytestcase import FuzzyTestCase


class TestExpressions(FuzzyTestCase):
    def test_range_packing(self):
        where = {"and": [
            {"gt": {"a": 20}},
            {"lt": {"a": 40}}
        ]}

        result = simplify_esfilter(where2esfilter(where))
        self.assertEqual(result, {"range": {"a": {"gt": 20, "lt": 40}}})

    def test_value_not_a_variable(self):
        result = get_all_vars({"eq": {"result.test": "/XMLHttpRequest/send-entity-body-document.htm"}})
        expected = set(["result.test"])
        self.assertEqual(result, expected, "expecting the one and only variable")




