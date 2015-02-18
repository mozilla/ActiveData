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

from tests.base_test_class import ActiveDataBaseTest


class TestEdge1(ActiveDataBaseTest):
    def test_count_rows(self):
        test = {
            "name": "count rows, 1d",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": "testdata",
                "select": {"aggregate": "count"},
                "edges": ["a"]
            },
            "expecting_list": {"data": [
                {"a": "b", "count": 2},
                {"a": "c", "count": 3},
                {"a": None, "count": 1}
            ]},
            "expecting_table": {
                "header": ["a", "count"],
                "data": [
                    ["b", 2],
                    ["c", 3],
                    [None, 1]
                ]
            },
            "expecting_cube": {
                "edges": [
                    {
                        "name": "a",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [{"value": "b"}, {"value": "c"}]
                        }
                    }
                ],
                "data": {
                    "count": [2, 3, 1]
                }
            }
        }
        self._execute_es_tests(test)

    def test_count_self(self):
        test = {
            "name": "count column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": "testdata",
                "select": {"name": "count_a", "value": "a", "aggregate": "count"},
                "edges": ["a"]
            },
            "expecting_list": {"data": [
                {"a": "b", "count_a": 2},
                {"a": "c", "count_a": 3},
                {"count_a": 0}
            ]},
            "expecting_table": {
                "header": ["a", "count_a"],
                "data": [
                    ["b", 2],
                    ["c", 3],
                    [None, 0]
                ]
            },
            "expecting_cube": {
                "edges": [
                    {
                        "name": "a",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [{"value": "b"}, {"value": "c"}]
                        }
                    }
                ],
                "data": {
                    "count_a": [2, 3, 0]
                }
            }
        }
        self._execute_es_tests(test)

    def test_count_other(self):
        test = {
            "name": "count column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": "testdata",
                "select": {"name": "count_v", "value": "v", "aggregate": "count"},
                "edges": ["a"]
            },
            "expecting_list": {"data": [
                {"a": "b", "count_v": 1},
                {"a": "c", "count_v": 3},
                {"count_v": 1}
            ]},
            "expecting_table": {
                "header": ["a", "count_v"],
                "data": [
                    ["b", 1],
                    ["c", 3],
                    [None, 1]
                ]
            },
            "expecting_cube": {
                "edges": [
                    {
                        "name": "a",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [{"value": "b"}, {"value": "c"}]
                        }
                    }
                ],
                "data": {
                    "count_v": [1, 3, 1]
                }
            }
        }
        self._execute_es_tests(test)

    def test_select_2(self):
        test = {
            "name": "count column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": "testdata",
                "select": [
                    {"name": "count", "value": "v", "aggregate": "count"},
                    {"name": "avg", "value": "v", "aggregate": "average"}
                ],
                "edges": ["a"]
            },
            "expecting_list": {"data": [
                {"a": "b", "count": 1, "avg": 2},
                {"a": "c", "count": 3, "avg": 31 / 3},
                {"count": 1, "avg": 3}
            ]},
            "expecting_table": {
                "header": ["a", "count", "avg"],
                "data": [
                    ["b", 1, 2],
                    ["c", 3, 31 / 3],
                    [None, 1, 3]
                ]
            },
            "expecting_cube": {
                "edges": [
                    {
                        "name": "a",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [{"value": "b"}, {"value": "c"}]
                        }
                    }
                ],
                "data": {
                    "count": [1, 3, 1],
                    "avg": [2, 31 / 3, 3]
                }
            }
        }
        self._execute_es_tests(test)

    def test_sum_column(self):
        test = {
            "name": "sum column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": "testdata",
                "select": {"value": "v", "aggregate": "sum"},
                "edges": ["a"]
            },
            "expecting_list": {"data": [
                {"a": "b", "v": 2},
                {"a": "c", "v": 31},
                {"a": None, "v": 3}
            ]},
            "expecting_table": {
                "header": ["a", "v"],
                "data": [
                    ["b", 2],
                    ["c", 31],
                    [None, 3]
                ]
            },
            "expecting_cube": {
                "edges": [
                    {
                        "name": "a",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [{"value": "b"}, {"value": "c"}]
                        }
                    }
                ],
                "data": {
                    "v": [2, 31, 3]
                }
            }
        }
        self._execute_es_tests(test)

    def test_where(self):
        test = {
            "name": "sum column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": "testdata",
                "select": {"value": "v", "aggregate": "max"},
                "edges": ["a"],
                "where": {"term": {"a": "c"}}
            },
            "expecting_list": {"data": [
                {"a": "c", "v": 13},
            ]},
            "expecting_table": {
                "header": ["a", "v"],
                "data": [
                    ["c", 13]
                ]
            },
            "expecting_cube": {
                "edges": [
                    {
                        "name": "a",
                        "allowNulls": False,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [{"value": "c"}]
                        }
                    }
                ],
                "data": {
                    "v": [13]
                }
            }
        }
        self._execute_es_tests(test)

    def test_where_w_dimension(self):
        test = {
            "name": "sum column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": "testdata",
                "select": {"value": "v", "aggregate": "max"},
                "edges": [
                    {"value": "a", "allowNulls":False, "domain": {"type": "set", "partitions": ["b", "c"]}}
                ],
                "where": {"term": {"a": "c"}}
            },
            "expecting_list": {"data": [
                {"a": "b", "v": None},
                {"a": "c", "v": 13}
            ]},
            "expecting_table": {
                "header": ["a", "v"],
                "data": [
                    ["b", None],
                    ["c", 13]
                ]
            },
            "expecting_cube": {
                "edges": [
                    {
                        "name": "a",
                        "allowNulls": False,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [{"value": "b"}, {"value": "c"}]
                        }
                    }
                ],
                "data": {
                    "v": [None, 13],
                }
            }
        }
        self._execute_es_tests(test)

    def test_empty_default_domain(self):
        test = {
            "name": "sum column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": "testdata",
                "select": {"value": "v", "aggregate": "max"},
                "edges": ["a"],
                "where": {"term": {"a": "d"}}
            },
            "expecting_list": {"data": [
            ]},
            "expecting_table": {
                "header": ["a", "v"],
                "data": []
            },
            "expecting_cube": {
                "edges": [
                    {
                        "name": "a",
                        "allowNulls": False,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": []
                        }
                    }
                ],
                "data": {
                    "v": []
                }
            }
        }
        self._execute_es_tests(test)


simple_test_data = [
    {"a": "c", "v": 13},
    {"a": "b", "v": 2},
    {"v": 3},
    {"a": "b"},
    {"a": "c", "v": 7},
    {"a": "c", "v": 11}
]


