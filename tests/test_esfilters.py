# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import division
from __future__ import unicode_literals

from jx_base.queries import is_variable_name
from mo_testing.fuzzytestcase import FuzzyTestCase
from mo_times.dates import Date
from jx_elasticsearch.es14.expressions import simplify_esfilter, USE_BOOL_MUST
from jx_base.expressions import jx_expression


class TestESFilters(FuzzyTestCase):


    def test_range_packing1(self):
        where = {"and": [
            {"gt": {"a": 20}},
            {"lt": {"a": 40}}
        ]}

        result = simplify_esfilter(jx_expression(where).to_esfilter())
        self.assertEqual(result, {"range": {"a": {"gt": 20, "lt": 40}}})

    def test_range_packing2(self):
        where = {"and": [
            {"gte": {"build.date": 1429747200}},
            {"lt": {"build.date": 1429920000}}
        ]}

        result = simplify_esfilter(jx_expression(where).to_esfilter())
        self.assertEqual(result, {"range": {"build.date": {"gte": Date("23 APR 2015").unix, "lt": Date("25 APR 2015").unix}}})

    def test_eq1(self):
        where = {"eq": {"a": 20}}
        result = simplify_esfilter(jx_expression(where).to_esfilter())
        self.assertEqual(result, {"term": {"a": 20}})

    def test_eq2(self):
        where = {"eq": {
            "a": 1,
            "b": 2
        }}
        result = simplify_esfilter(jx_expression(where).to_esfilter())
        if USE_BOOL_MUST:
            self.assertEqual(result, {"bool": {"must": [{"term": {"a": 1}}, {"term": {"b": 2}}]}})
        else:
            self.assertEqual(result, {"and": [{"term": {"a": 1}}, {"term": {"b": 2}}]})

    def test_eq3(self):
        where = {"eq": {
            "a": 1,
            "b": [2, 3]
        }}
        result = simplify_esfilter(jx_expression(where).to_esfilter())
        if USE_BOOL_MUST:
            self.assertEqual(result, {"bool": {"must": [{"term": {"a": 1}}, {"terms": {"b": [2, 3]}}]}})
        else:
            self.assertEqual(result, {"and": [{"term": {"a": 1}}, {"terms": {"b": [2, 3]}}]})

    def test_ne1(self):
        where = {"ne": {"a": 1}}
        result = simplify_esfilter(jx_expression(where).to_esfilter())
        self.assertEqual(result, {"not": {"term": {"a": 1}}})

    def test_ne2(self):
        where = {"neq": {"a": 1}}
        result = simplify_esfilter(jx_expression(where).to_esfilter())
        self.assertEqual(result, {"not": {"term": {"a": 1}}})

    def test_in(self):
        where = {"in": {"a": [1, 2]}}
        result = simplify_esfilter(jx_expression(where).to_esfilter())
        self.assertEqual(result, {"terms": {"a": [1, 2]}})


