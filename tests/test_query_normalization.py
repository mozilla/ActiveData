from pyLibrary.queries import _normalize_select
from pyLibrary.queries.query import _normalize_edge
from pyLibrary.testing.fuzzytestcase import FuzzyTestCase


class TestQueryNormalization(FuzzyTestCase):
    def test_complex_edge_with_no_name(self):
        edge = {"value": ["a", "c"]}
        self.assertRaises(Exception, _normalize_edge, edge)

    def test_complex_edge_value(self):
        edge = {"name": "n", "value": ["a", "c"]}

        result = _normalize_edge(edge)
        expected = {"name": "n", "domain": {"dimension": {"fields": ["a", "c"]}}}
        self.assertEqual(result, expected)
        self.assertEqual(result.value, None)

    def test_naming_select(self):
        select = {"value": "result.duration", "aggregate": "avg"}
        result = _normalize_select(select)
        #DEEP NAMES ARE ALLOWED, AND NEEDED TO BUILD STRUCTURE FROM A QUERY
        expected = {"name": "result.duration", "value": "result.duration", "aggregate": "average"}
        self.assertEqual(result, expected)
