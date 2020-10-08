# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

from unittest import skip, skipIf

from jx_base.expressions import NULL
from mo_dots import set_default, to_data
from mo_future import text
from tests.test_jx import BaseTestCase, TEST_TABLE, global_settings


class TestgroupBy1(BaseTestCase):

    def test_no_select(self):
        test = {
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "groupby": "a"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b", "count": 2},
                    {"a": "c", "count": 3},
                    {"a": NULL, "count": 1}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "count"],
                "data": [
                    ["b", 2],
                    ["c", 3],
                    [NULL, 1]
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_count_rows(self):
        test = {
            "name": "count rows, 1d",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"aggregate": "count"},
                "groupby": ["a"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b", "count": 2},
                    {"a": "c", "count": 3},
                    {"a": NULL, "count": 1}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "count"],
                "data": [
                    ["b", 2],
                    ["c", 3],
                    [NULL, 1]
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_count_self(self):
        test = {
            "name": "count column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"name": "count_a", "value": "a", "aggregate": "count"},
                "groupby": ["a"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b", "count_a": 2},
                    {"a": "c", "count_a": 3},
                    {"count_a": 0}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "count_a"],
                "data": [
                    ["b", 2],
                    ["c", 3],
                    [NULL, 0]
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_count_other(self):
        test = {
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"name": "count_v", "value": "v", "aggregate": "count"},
                "groupby": ["a"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b", "count_v": 1},
                    {"a": "c", "count_v": 3},
                    {"count_v": 1}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "count_v"],
                "data": [
                    ["b", 1],
                    ["c", 3],
                    [NULL, 1]
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_select_2(self):
        test = {
            "name": "count column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": [
                    {"name": "count", "value": "v", "aggregate": "count"},
                    {"name": "avg", "value": "v", "aggregate": "average"}
                ],
                "groupby": ["a"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b", "count": 1, "avg": 2},
                    {"a": "c", "count": 3, "avg": 31 / 3},
                    {"count": 1, "avg": 3}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "count", "avg"],
                "data": [
                    ["b", 1, 2],
                    ["c", 3, 31 / 3],
                    [NULL, 1, 3]
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_sum_column(self):
        test = {
            "name": "sum column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "v", "aggregate": "sum"},
                "groupby": ["a"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b", "v": 2},
                    {"a": "c", "v": 31},
                    {"a": NULL, "v": 3}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "v"],
                "data": [
                    ["b", 2],
                    ["c", 31],
                    [NULL, 3]
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_where(self):
        test = {
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "v", "aggregate": "max"},
                "groupby": ["a"],
                "where": {"term": {"a": "c"}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "c", "v": 13},
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "v"],
                "data": [
                    ["c", 13]
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_where_w_dimension(self):
        test = {
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "v", "aggregate": "max"},
                "groupby": "a",
                "where": {"term": {"a": "c"}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "c", "v": 13}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "v"],
                "data": [
                    ["c", 13]
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_bad_groupby(self):
        test = {
            "data": [],
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "v", "aggregate": "max"},
                "groupby": [
                    {"value": "a", "allowNulls": False, "domain": {"type": "set", "partitions": ["b", "c"]}}
                ]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": []
            }
        }
        self.assertRaises(Exception, self.utils.execute_tests, test)

    def test_empty_default_domain(self):
        test = {
            "name": "sum column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "v", "aggregate": "max"},
                "groupby": ["a"],
                "where": {"term": {"a": "d"}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "v"],
                "data": []
            }
        }
        self.utils.execute_tests(test)

    def test_default(self):
        test = {
            "name": "sum column",
            "metadata": {},
            "data": [
                {"a": "c"},
                {"a": "b", "v": 2},
                {"v": 3},
                {"a": "b"},
                {"a": "c"},
                {"a": "c"}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "v", "aggregate": "sum", "default": 0},
                "groupby": ["a"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b", "v": 2},
                    {"a": "c", "v": 0},
                    {"a": NULL, "v": 3}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "v"],
                "data": [
                    ["b", 2],
                    ["c", 0],
                    [None, 3]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [{"name": "a", "domain": {"partitions": [
                    {"value": "b"},
                    {"value": "c"}
                ]}}],
                "data": {
                    "v": [2, 0, 3]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_many_aggs_on_one_column(self):
        # ES WILL NOT ACCEPT TWO (NAIVE) AGGREGATES ON SAME FIELD, COMBINE THEM USING stats AGGREGATION
        test = {
            # d = {"a": a, "v", v}
            "data": [
                {
                    "a": {"b": {"c": d.v}},
                    "b": d.a
                }
                for d in to_data(simple_test_data)
            ],
            "query": {
                "from": TEST_TABLE,
                "select": [
                    {"name": "maxi", "value": "a.b.c", "aggregate": "max"},
                    {"name": "mini", "value": "a.b.c", "aggregate": "min"}
                ],
                "groupby": "b"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"b": "b", "mini": 2, "maxi": 2},
                    {"b": "c", "mini": 7, "maxi": 13},
                    {"b": NULL, "mini": 3, "maxi": 3}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["b", "mini", "maxi"],
                "data": [
                    ["b", 2, 2],
                    ["c", 7, 13],
                    [NULL, 3, 3]
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_error_on_same_column_name(self):
        test = {
            "data": [],
            "query": {
                "from": TEST_TABLE,
                "select": [
                    {"value": "_a._b._c", "aggregate": "max"},
                    {"value": "_a._b._c", "aggregate": "min"}
                ],
                "groupby": "_b"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": []
            }
        }
        self.assertRaises(Exception, self.utils.execute_tests, test)

    def test_groupby_is_table(self):
        test = {
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": [
                    {"value": "v", "aggregate": "sum"}
                ],
                "groupby": "a"
            },
            "expecting": {
                "meta": {"format": "table"},
                "header": ["a", "v"],
                "data": [
                    ["b", 2],
                    ["c", 31],
                    [NULL, 3]
                ]
            }
        }
        self.utils.execute_tests(test)

    @skipIf(global_settings.elasticsearch.version.startswith("5."), "Not supported by es5")
    def test_groupby_left_id(self):
        test = {
            "data": [set_default(d, {"_id": "aa" + text(i)}) for i, d in enumerate(simple_test_data)],
            "query": {
                "from": TEST_TABLE,
                "groupby": {"name": "prefix", "value": {"left": {"_id": 2}}}
            },
            "expecting": {
                "meta": {"format": "table"},
                "header": ["prefix", "count"],
                "data": [
                    ["aa", 6]
                ]
            }
        }
        self.utils.execute_tests(test)

    @skipIf(int(global_settings.elasticsearch.version.split(".")[0]) <= 4, "version 4 and below do not implement")
    def test_count_values(self):
        # THIS IS NOT PART OF THE JX SPEC, IT IS AN INTERMEDIATE FORM FOR DEBUGGING
        test = {
            "data": [
                {"a": 1, "b": [1, 2, 3]},
                {"a": 2, "b": [4, 5, 6]},
                {"a": 3, "b": [2, 3, 4]},
                {"a": 4},
                {"a": 5, "b": [3, 4, 5]},
                {"a": 6, "b": 6}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": [{"value": "b", "aggregate": "count_values"}]
            },
            "expecting_list": {
                "meta": {"format": "value"},
                "data":
                    {"b": {"1.0": 1, "2.0": 2, "3.0": 3, "4.0": 3, "5.0": 2, "6.0": 2}}

            }
        }
        self.utils.execute_tests(test)

    # @skipIf(int(global_settings.elasticsearch.version.split(".")[0]) <= 5, "version 5 and below do not implement")
    @skip("for coverage")
    def test_groupby_multivalue_nested(self):
        test = {
            "data": [
                {"a": 1, "b": [1, 2, 3]},
                {"a": 2, "b": [4, 5, 6]},
                {"a": 3, "b": [2, 3, 4]},
                {"a": 4},
                {"a": 5, "b": [3, 4, 5]},
                {"a": 6, "b": 6}
            ],
            "query": {
                "from": TEST_TABLE + ".b",
                "groupby": [{"name": "b", "value": "."}]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"b": 1, "count": 1},
                    {"b": 2, "count": 2},
                    {"b": 3, "count": 3},
                    {"b": 4, "count": 3},
                    {"b": 5, "count": 2},
                    {"b": 6, "count": 2},
                    {"b": NULL, "count": 1}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["b", "count"],
                "data": [
                    [1, 1],
                    [2, 2],
                    [3, 3],
                    [4, 3],
                    [5, 2],
                    [6, 2],
                    [NULL, 1]
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_groupby_object(self):
        test = {
            "data": [
                {"g": {"a": "c", "v": 1}},
                {"g": {"a": "b", "v": 1}},
                {"g": {"a": "b", "v": 1}},
                {"g": {"v": 2}},
                {"g": {"a": "b"}},
                {"g": {"a": "c", "v": 2}},
                {"g": {"a": "c", "v": 2}}
            ],
            "query": {
                "from": TEST_TABLE,
                "groupby": ["g"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data":[
                    {"g": {"a": "b", "v": 1}, "count": 2},
                    {"g": {"a": "b"}, "count": 1},
                    {"g": {"a": "c", "v": 2}, "count": 2},
                    {"g": {"a": "c", "v": 1}, "count": 1},
                    {"g": {"v": 2}, "count": 1}
                ]
            },
            "expecting_table": {
              #  "meta": {"format": "table"}, this meta property is not included as
              #  test is ensuring the default format is table when given a groupby clause
                "header": ["g", "count"],
                "data": [
                    [{"a": "b", "v": 1}, 2],
                    [{"a": "b"        }, 1],
                    [{"a": "c", "v": 2}, 2],
                    [{"a": "c", "v": 1}, 1],
                    [{          "v": 2}, 1]
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_groupby_object_star(self):
        test = {
            "data": [
                {"g": {"a": "c", "v": 1}},
                {"g": {"a": "b", "v": 1}},
                {"g": {"a": "b", "v": 1}},
                {"g": {          "v": 2}},
                {"g": {"a": "b"        }},
                {"g": {"a": "c", "v": 2}},
                {"g": {"a": "c", "v": 2}}
            ],
            "query": {
                "from": TEST_TABLE,
                "groupby": ["g.*"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"g.a": "b", "g.v": 1, "count": 2},
                    {"g.a": "b",           "count": 1},
                    {"g.a": "c", "g.v": 2, "count": 2},
                    {"g.a": "c", "g.v": 1, "count": 1},
                    {            "g.v": 2, "count": 1}
                ]
            },
            "expecting_table": {
                "header": ["g.a", "g.v", "count"],
                "data": [
                    ["b", 1, 2],
                    ["b", NULL, 1],
                    ["c", 2, 2],
                    ["c", 1, 1],
                    [NULL, 2, 1]
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_groupby_multivalue_naive(self):
        test = {
            "data": [
                {"r": {"t": ["a", "b"]}},
                {"r": {"t": ["b", "a"]}},
                {"r": {"t": ["a"]}},
                {"r": {"t": ["b"]}},
                {},
            ],
            "query": {
                "from": TEST_TABLE,
                "groupby": ["r.t"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"r": {"t": ["a", "b"]}, "count": 2},
                    {"r": {"t": "b"}, "count": 1},
                    {"r": {"t": "a"}, "count": 1},
                    {"r": {"t": NULL}, "count": 1},
                ]
            },
            "expecting_table": {
                "header": ["r.t", "count"],
                "data": [
                    [["a", "b"], 2],
                    ["a", 1],
                    ["b", 1],
                    [NULL, 1]
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_script_on_missing_column1(self):
        test = {
            "data": [
                {"a": "skip"},
                {"a": "ok"},
                {"a": "error"},
                {"a": "ok"},
                {},
            ],
            "query": {
                "from": TEST_TABLE,
                "select": {
                    "name": "skips",
                    "value": {"when": {"eq": {"b": "skip"}}, "then": 1, "else": 0},
                    "aggregate": "sum"
                }
            },
            "expecting_list": {
                "meta": {"format": "value"},
                "data": 0
            },
            "expecting_table": {
                "header": ["skips"],
                "data": [[0]]
            }
        }
        self.utils.execute_tests(test)

    def test_script_missing_column2(self):
        test = {
            "data": [
                {"a": "skip"},
                {"a": "ok"},
                {"a": "error"},
                {"a": "ok"},
                {},
            ],
            "query": {
                "from": TEST_TABLE,
                "select": {
                    "name": "skips",
                    "value": {"eq": {"b": "skip"}},
                    "aggregate": "sum"
                }
            },
            "expecting_list": {
                "meta": {"format": "value"},
                "data": 0
            },
            "expecting_table": {
                "header": ["skips"],
                "data": [[0]]
            }
        }
        self.utils.execute_tests(test)

    def test_boolean_count(self):
        test = {
            "data": [
                {"a": "skip"},
                {"a": "ok"},
                {"a": "error"},
                {"a": "ok"},
                {},
            ],
            "query": {
                "from": TEST_TABLE,
                "select": {
                    "name": "skips",
                    "value": {"eq": {"a": "skip"}},
                    "aggregate": "sum"
                }
            },
            "expecting_list": {
                "meta": {"format": "value"},
                "data": 1
            },
            "expecting_table": {
                "header": ["skips"],
                "data": [[1]]
            }
        }
        self.utils.execute_tests(test)

    def test_boolean_min_max(self):
        test = {
            "data": [
                {"a": True},
                {"a": False},
                {"a": False},
                {"a": None},
                {},
            ],
            "query": {
                "from": TEST_TABLE,
                "select": [
                    {
                        "name": "max",
                        "value": "a",
                        "aggregate": "max"
                    },
                    {
                        "name": "min",
                        "value": "a",
                        "aggregate": "min"
                    },
                    {
                        "name": "or",
                        "value": "a",
                        "aggregate": "or"
                    },
                    {
                        "name": "and",
                        "value": "a",
                        "aggregate": "and"
                    }
                ],
            },
            "expecting_list": {
                "meta": {"format": "value"},
                "data": {"and": False, "or": True, "min": 0, "max": 1}
            },
            "expecting_table": {
                "header": ["max", "min", "or", "and"],
                "data": [[1, 0, True, False]]
            }
        }
        self.utils.execute_tests(test)

    def test_boolean_and_or_on_expression(self):
        test = {
            "data": [
                {"a": 1},
                {"a": 2},
                {"a": 3},
                {"a": None},
                {},
            ],
            "query": {
                "from": TEST_TABLE,
                "select": [
                    {
                        "name": "max",
                        "value": {"eq": {"a": 1}},
                        "aggregate": "max"
                    },
                    {
                        "name": "min",
                        "value": {"eq": {"a": 1}},
                        "aggregate": "min"
                    },
                    {
                        "name": "or",
                        "value": {"eq": {"a": 1}},
                        "aggregate": "or"
                    },
                    {
                        "name": "and",
                        "value": {"eq": {"a": 1}},
                        "aggregate": "and"
                    }
                ],
            },
            "expecting_list": {
                "meta": {"format": "value"},
                "data": {"and": False, "or": True, "min": 0, "max": 1}
            },
            "expecting_table": {
                "header": ["max", "min", "or", "and"],
                "data": [[1, 0, True, False]]
            }
        }
        self.utils.execute_tests(test)

    def test_eq_1(self):
        test = {
            "data": [
                {"a": 1},
                {"a": 2},
                {"a": 3},
                {"a": None},
                {},
            ],
            "query": {
                "from": TEST_TABLE,
                "groupby": {"name": "eq1", "value": {"eq": {"a": 1}}},
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"eq1": True, "count": 1},
                    {"eq1": False, "count": 4}
                ]
            },
            "expecting_table": {
                "header": ["eq1", "count"],
                "data": [
                    [True, 1],
                    [False, 4]
                ]
            }
        }
        self.utils.execute_tests(test)









# TODO: GROUPBY NUMBER SHOULD NOT RESULT IN A STRING
#         "groupby":[{
#        		"name":"date",
#        		"value":{"floor":[{"div":{"run.timestamp":86400}},1]}
#        	}],



# TODO: AGG SHALLOW FIELD WITH DEEP GROUPBY
# {
#     "from": "coverage.source.file.covered",
#     "select": {"value":"test.url", "aggregate":"union"},   # NOT SUPPORTED ON DEEP QUERY, YET
#     "where": {"and": [
#         {"missing": "source.method.name"},
#         {"eq": {
#             "source.file.name": not_summarized.source.file.name,
#             "build.revision12": not_summarized.build.revision12
#         }},
#     ]},
#     "groupby": [
#         "line"
#     ],
#     "limit": 100000,
#     "format": "list"
# }


simple_test_data = [
    {"a": "c", "v": 13},
    {"a": "b", "v": 2},
    {"v": 3},
    {"a": "b"},
    {"a": "c", "v": 7},
    {"a": "c", "v": 11}
]


