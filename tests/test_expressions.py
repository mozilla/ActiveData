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
from pyLibrary.queries.domains import is_keyword

from pyLibrary.queries.expressions import get_all_vars, simplify_esfilter, qb_expression_to_esfilter
from pyLibrary.testing.fuzzytestcase import FuzzyTestCase


class TestExpressions(FuzzyTestCase):

    def test_error_on_bad_var(self):
        self.assertFalse(
            is_keyword(u'coalesce(rows[rownum+1].timestamp, Date.eod())'),
            "That's not a valid variable name!!"
        )

    def test_good_var(self):
        self.assertTrue(
            is_keyword(u'_a._b'),
            "That's a good variable name!"
        )

    def test_range_packing(self):
        where = {"and": [
            {"gt": {"a": 20}},
            {"lt": {"a": 40}}
        ]}

        result = simplify_esfilter(qb_expression_to_esfilter(where))
        self.assertEqual(result, {"range": {"a": {"gt": 20, "lt": 40}}})

    def test_value_not_a_variable(self):
        result = get_all_vars({"eq": {"result.test": "/XMLHttpRequest/send-entity-body-document.htm"}})
        expected = set(["result.test"])
        self.assertEqual(result, expected, "expecting the one and only variable")

    def test_eq1(self):
        where = {"eq": {"a": 20}}
        result = simplify_esfilter(qb_expression_to_esfilter(where))
        self.assertEqual(result, {"term": {"a": 20}})

    def test_eq2(self):
        where = {"eq": {
            "a": 1,
            "b": 2
        }}
        result = simplify_esfilter(qb_expression_to_esfilter(where))
        self.assertEqual(result, {"and": [{"term": {"a": 1}}, {"term": {"b": 2}}]})

    def test_eq3(self):
        where = {"eq": {
            "a": 1,
            "b": [2, 3]
        }}
        result = simplify_esfilter(qb_expression_to_esfilter(where))
        self.assertEqual(result, {"and": [{"term": {"a": 1}}, {"terms": {"b": [2, 3]}}]})


