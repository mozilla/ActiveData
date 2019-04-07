# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

from jx_base.expressions import NULL, jx_expression
from jx_elasticsearch.es52.expressions import ES52, simplify_esfilter
from jx_elasticsearch.es52.painless import EsScript
from jx_elasticsearch.es52.util import es_and
from mo_dots import Null, wrap
from mo_json import NUMBER, OBJECT
from mo_json.typed_encoder import STRING_TYPE
from mo_testing.fuzzytestcase import FuzzyTestCase
from mo_times.dates import Date


class TestESFilters(FuzzyTestCase):

    def test_range_packing1(self):
        where = {"and": [
            {"gt": {"a": 20}},
            {"lt": {"a": 40}}
        ]}

        result = simplify_esfilter(ES52[jx_expression(where)].partial_eval().to_esfilter(identity_schema))
        self.assertEqual(result, {"range": {"a": {"gt": 20, "lt": 40}}})

    def test_range_packing2(self):
        where = {"and": [
            {"gte": {"build.date": 1429747200}},
            {"lt": {"build.date": 1429920000}}
        ]}

        result = simplify_esfilter(ES52[jx_expression(where)].partial_eval().to_esfilter(identity_schema))
        self.assertEqual(result, {"range": {"build.date": {"gte": Date("23 APR 2015").unix, "lt": Date("25 APR 2015").unix}}})

    def test_eq1(self):
        where = {"eq": {"a": 20}}
        result = simplify_esfilter(ES52[jx_expression(where)].partial_eval().to_esfilter(identity_schema))
        self.assertEqual(result, {"term": {"a": 20}})

    def test_eq2(self):
        where = {"eq": {
            "a": 1,
            "b": 2
        }}
        result = simplify_esfilter(ES52[jx_expression(where)].partial_eval().to_esfilter(identity_schema))
        self.assertEqual(result, es_and([{"term": {"a": 1}}, {"term": {"b": 2}}]))

    def test_eq3(self):
        where = {"eq": {
            "a": 1,
            "b": [2, 3]
        }}
        result = simplify_esfilter(ES52[jx_expression(where)].partial_eval().to_esfilter(identity_schema))
        self.assertEqual(result, es_and([{"term": {"a": 1}}, {"terms": {"b": [2, 3]}}]))

    def test_ne1(self):
        where = {"ne": {"a": 1}}

        result = simplify_esfilter(ES52[jx_expression(where)].partial_eval().to_esfilter(identity_schema))
        self.assertEqual(result, {"bool": {"must_not": {"term": {"a": 1}}}})

    def test_ne2(self):
        where = {"neq": {"a": 1}}
        result = simplify_esfilter(ES52[jx_expression(where)].partial_eval().to_esfilter(identity_schema))
        self.assertEqual(result, {"bool": {"must_not": {"term": {"a": 1}}}})

    def test_in(self):
        where = {"in": {"a": [1, 2]}}
        result = ES52[jx_expression(where)].partial_eval().to_esfilter(identity_schema)
        self.assertEqual(result, {"terms": {"a": [1, 2]}})

    def test_prefix(self):
        k = "k."+STRING_TYPE
        where = {"prefix": {k: "v"}}
        result = ES52[jx_expression(where)].partial_eval().to_esfilter(identity_schema)
        self.assertEqual(result, {"prefix": {k: "v"}})

    def test_painless(self):
        # THIS TEST IS USED TO FORCE-IMPORT OF elasticsearch EXTENSION METHODS
        a = EsScript(type=OBJECT, expr=NULL, frum=NULL, schema=identity_schema)

    def test_null_startswith(self):
        filter = ES52[jx_expression({"prefix": [{"null": {}}, {"literal": "something"}]})].to_esfilter(Null)
        expected = {"bool": {"must_not": {"match_all": {}}}}
        self.assertEqual(filter, expected)
        self.assertEqual(expected, filter)

    def test_null_startswith_null(self):
        filter = ES52[jx_expression({"prefix": [{"null": {}}, {"literal": ""}]})].to_esfilter(Null)
        expected = {"match_all": {}}
        self.assertEqual(filter, expected)
        self.assertEqual(expected, filter)





class S(object):
    snowflake = Null

    def values(self, name, exclude=None):
        return wrap([{"es_column": name}])

    def leaves(self, name):
        return wrap([
            {"es_column": name, "jx_type": NUMBER}
        ])


identity_schema = S()

