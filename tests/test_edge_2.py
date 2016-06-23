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
import base_test_class

from tests.base_test_class import ActiveDataBaseTest, TEST_TABLE


class TestEdge2(ActiveDataBaseTest):
    def test_count_rows(self):
        test = {
            "name": "count rows, 2d",
            "metadata": {},
            "data": two_dim_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"aggregate": "count"},
                "edges": [
                    {"value": "a", "domain": {"type": "set", "partitions": ["x", "y", "z"]}},
                    "b"
                ]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "x", "b": "m", "count": 2},
                    {"a": "x", "b": "n", "count": 1},
                    {"a": "x", "b": None, "count": 1},
                    {"a": "y", "b": "m", "count": 1},
                    {"a": "y", "b": "n", "count": 2},
                    {"a": "y", "b": None, "count": 1},
                    {"a": None, "b": "m", "count": 1},
                    {"a": None, "b": "n", "count": 1}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "b", "count"],
                "data": [
                    [None, "m", 1],
                    [None, "n", 1],
                    ["x", None, 1],
                    ["x", "m", 2],
                    ["x", "n", 1],
                    ["y", None, 1],
                    ["y", "m", 1],
                    ["y", "n", 2]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "a",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "partitions": [
                                {
                                    "dataIndex": 0,
                                    "name": "x",
                                    "value": "x"
                                },
                                {
                                    "dataIndex": 1,
                                    "name": "y",
                                    "value": "y"
                                },
                                {
                                    "dataIndex": 2,
                                    "name": "z",
                                    "value": "z"
                                }
                            ]
                        }
                    },
                    {
                        "name": "b",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "partitions": [
                                {
                                    "dataIndex": 0,
                                    "name": "m",
                                    "value": "m"
                                },
                                {
                                    "dataIndex": 1,
                                    "name": "n",
                                    "value": "n"
                                }
                            ]
                        }
                    }
                ],
                "data": {
                    "count": [
                        [2, 1, 1],
                        [1, 2, 1],
                        [0, 0, 0],
                        [1, 1, 0]
                    ]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_sum_rows(self):
        test = {
            "name": "sum rows",
            "metadata": {},
            "data": two_dim_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "v", "aggregate": "sum"},
                "edges": ["a", "b"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "x", "b": "m", "v": 29},
                    {"a": "x", "b": "n", "v": 3},
                    {"a": "x", "b": None, "v": 5},
                    {"a": "y", "b": "m", "v": 7},
                    {"a": "y", "b": "n", "v": 50},
                    {"a": "y", "b": None, "v": 13},
                    {"a": None, "b": "m", "v": 17},
                    {"a": None, "b": "n", "v": 19}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "b", "v"],
                "data": [
                    ["x", "m", 29],
                    ["x", "n", 3],
                    ["x", None, 5],
                    ["y", "m", 7],
                    ["y", "n", 50],
                    ["y", None, 13],
                    [None, "m", 17],
                    [None, "n", 19]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "a",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "partitions": [
                                {"name": "x", "value": "x", "dataIndex": 0},
                                {"name": "y", "value": "y", "dataIndex": 1}
                            ]
                        }
                    },
                    {
                        "name": "b",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "partitions": [
                                {"name": "m", "value": "m", "dataIndex": 0},
                                {"name": "n", "value": "n", "dataIndex": 1}
                            ]
                        }
                    }
                ],
                "data": {
                    "v": [
                        [29, 3, 5],
                        [7, 50, 13],
                        [17, 19, None]
                    ]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_avg_rows_w_default(self):
        test = {
            "metadata": {},
            "data": [
                {"a": "x", "b": "m", "v": 2},
                {"a": "x", "b": "m"},
                {"a": "x", "b": "n", "v": 3},
                {"a": "x"},
                {"a": "y", "b": "m", "v": 7},
                {"a": "y", "b": "n"},
                {"a": "y", "b": "n"},
                {"a": "y", "v": 13},
                {"b": "m", "v": 17},
                {"b": "n", "v": 19}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "v", "aggregate": "average", "default": 0},
                "edges": ["a", "b"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "x", "b": "m", "v": 2},
                    {"a": "x", "b": "n", "v": 3},
                    {"a": "x", "v": 0},
                    {"a": "y", "b": "m", "v": 7},
                    {"a": "y", "b": "n", "v": 0},
                    {"a": "y", "v": 13},
                    {"b": "m", "v": 17},
                    {"b": "n", "v": 19}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "b", "v"],
                "data": [
                    ["x", None, 0],
                    ["x", "m", 2],
                    ["x", "n", 3],
                    ["y", None, 13],
                    ["y", "m", 7],
                    ["y", "n", 0],
                    [None, "m", 17],
                    [None, "n", 19]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "a",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "partitions": [
                                {
                                    "dataIndex": 0,
                                    "name": "x",
                                    "value": "x"
                                },
                                {
                                    "dataIndex": 1,
                                    "name": "y",
                                    "value": "y"
                                }
                            ]
                        }
                    },
                    {
                        "name": "b",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "partitions": [
                                {
                                    "dataIndex": 0,
                                    "name": "m",
                                    "value": "m"
                                },
                                {
                                    "dataIndex": 1,
                                    "name": "n",
                                    "value": "n"
                                }
                            ]
                        }
                    }
                ],
                "data": {
                    "v": [
                        [2, 3, 0],
                        [7, 0, 13],
                        [17, 19, 0]
                    ]
                }
            }
        }
        self.utils.execute_es_tests(test)



    def test_sum_rows_w_domain(self):
        test = {
            "name": "sum rows",
            "metadata": {},
            "data": two_dim_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "v", "aggregate": "sum"},
                "edges": [
                    {
                        "value": "a",
                        "domain": {
                            "type": "set",
                            "partitions": ["x", "y", "z"]
                        }
                    },
                    {
                        "value": "b",
                        "domain": {
                            "type": "set",
                            "partitions": ["m", "n"]
                        }
                    }
                ]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "x", "b": "m", "v": 29},
                    {"a": "x", "b": "n", "v": 3},
                    {"a": "x", "b": None, "v": 5},
                    {"a": "y", "b": "m", "v": 7},
                    {"a": "y", "b": "n", "v": 50},
                    {"a": "y", "b": None, "v": 13},
                    {"a": None, "b": "m", "v": 17},
                    {"a": None, "b": "n", "v": 19}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "b", "v"],
                "data": [
                    ["x", "m", 29],
                    ["x", "n", 3],
                    ["x", None, 5],
                    ["y", "m", 7],
                    ["y", "n", 50],
                    ["y", None, 13],
                    [None, "m", 17],
                    [None, "n", 19]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "a",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [
                                {"name": "x", "value": "x", "dataIndex": 0},
                                {"name": "y", "value": "y", "dataIndex": 1},
                                {"name": "z", "value": "z", "dataIndex": 2}
                            ]
                        }
                    },
                    {
                        "name": "b",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [
                                {"name": "m", "value": "m", "dataIndex": 0},
                                {"name": "n", "value": "n", "dataIndex": 1}
                            ]
                        }
                    }
                ],
                "data": {
                    "v": [
                        [29, 3, 5],
                        [7, 50, 13],
                        [None, None, None],
                        [17, 19, None]
                    ]
                }
            }
        }
        self.utils.execute_es_tests(test)


two_dim_test_data = [
    {"a": "x", "b": "m", "v": 2},
    {"a": "x", "b": "n", "v": 3},
    {"a": "x", "b": None, "v": 5},
    {"a": "y", "b": "m", "v": 7},
    {"a": "y", "b": "n", "v": 11},
    {"a": "y", "b": None, "v": 13},
    {"a": None, "b": "m", "v": 17},
    {"a": None, "b": "n", "v": 19},
    {"a": "x", "b": "m", "v": 27},
    {"a": "y", "b": "n", "v": 39}
]

metadata = {
    "properties": {
        "a": {
            "type": "string",
            "domain": {
                "type": "set",
                "partitions": ["x", "y", "z"]
            }
        },
        "b": {
            "type": "string",
            "domain": {
                "type": "set",
                "partitions": ["m", "n"]
            }
        }
    }
}

