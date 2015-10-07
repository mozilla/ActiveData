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
from __future__ import absolute_import

import base_test_class
from pyLibrary.dot import wrap
from tests.base_test_class import ActiveDataBaseTest
from pyLibrary.maths import Math

lots_of_data = wrap([{"a": i} for i in range(30)])


class TestDeepOps(ActiveDataBaseTest):

    def test_deep_select_column(self):
        test = {
            "data": [
                {"_a": [
                    {"b": "x", "v": 2},
                    {"b": "y", "v": 3}
                ]},
                {"_a": {"b": "x", "v": 5}},
                {"_a": [
                    {"b": "x", "v": 7},
                ]},
                {"c": "x"}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index+"._a",
                "select": {"value": "_a.v", "aggregate": "sum"},
                "edges": ["_a.b"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"_a": {"b": "x", "v": 14}},
                    {"_a": {"b": "y", "v": 3}},
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["_a.b", "_a.v"],
                "data": [
                    ["x", 14],
                    ["y", 3]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "_a.b",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [{"value": "x"}, {"value": "y"}]
                        }
                    }
                ],
                "data": {
                    "_a.v": [14, 3]
                }
            }
        }
        self._execute_es_tests(test)

    def test_deep_select_column_w_groupby(self):
        test = {
            "data": [
                {"_a": [
                    {"b": "x", "v": 2},
                    {"b": "y", "v": 3}
                ]},
                {"_a": {"b": "x", "v": 5}},
                {"_a": [
                    {"b": "x", "v": 7},
                ]},
                {"c": "x"}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index+"._a",
                "select": {"value": "_a.v", "aggregate": "sum"},
                "groupby": ["_a.b"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"_a": {"b": "x", "v": 14}},
                    {"_a": {"b": "y", "v": 3}},
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["_a.b", "_a.v"],
                "data": [
                    ["x", 14],
                    ["y", 3]
                ]
            }
        }
        self._execute_es_tests(test)

    def test_bad_deep_select_column_w_groupby(self):
        test = {
            "data": [  # WE NEED SOME DATA TO MAKE A NESTED COLUMN
                {"_a": {"b": "x"}}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"value": "a.v", "aggregate": "sum"},
                "groupby": ["a.b"]
            },
            "expecting_list": {  # DUMMY: SO AN QUERY ATTEMPT IS MADE
                "meta": {"format": "list"},
                "data": []
            }
        }
        self.assertRaises(Exception, self._execute_es_tests, test)

    def test_abs_shallow_select(self):
        # TEST THAT ABSOLUTE COLUMN NAMES WORK (WHEN THEY DO NOT CONFLICT WITH RELATIVE PROPERTY NAME)
        test = {
            "data": [
                {"o": 3, "_a": [
                    {"b": "x", "v": 2},
                    {"b": "y", "v": 3}
                ]},
                {"o": 1, "_a": {"b": "x", "v": 5}},
                {"o": 2, "_a": [
                    {"b": "x", "v": 7},
                ]},
                {"o": 4, "c": "x"}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index+"._a",
                "select": ["b", "o"],
                "where": {"eq": {"b": "x"}},
                "sort": ["o"]
            },
            "es_query": {  # FOR REFERENCE
                "query": {"nested": {
                    "path": "_a",
                    "inner_hits": {},
                    "filter": {"term": {"_a.b": "x"}}
                }},
                "fields": ["o"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"o": 1, "b": "x"},
                    {"o": 2, "b": "x"},
                    {"o": 3, "b": "x"}
                ]},
            # "expecting_table": {
            #     "meta": {"format": "table"},
            #     "header": ["o", "a", "c"],
            #     "data": [
            #         [1, {"b": "x", "v": 5}, None],
            #         [2, {"b": "x", "v": 7}, None],
            #         [3,
            #             [
            #                 {"b": "x", "v": 2},
            #                 {"b": "y", "v": 3}
            #             ],
            #             None
            #         ],
            #         [4, None, "x"]
            #     ]
            # },
            # "expecting_cube": {
            #     "meta": {"format": "cube"},
            #     "edges": [
            #         {
            #             "name": "rownum",
            #             "domain": {"type": "rownum", "min": 0, "max": 4, "interval": 1}
            #         }
            #     ],
            #     "data": {
            #         "a": [
            #             {"b": "x", "v": 5},
            #             {"b": "x", "v": 7},
            #             [
            #                 {"b": "x", "v": 2},
            #                 {"b": "y", "v": 3}
            #             ],
            #             None
            #         ],
            #         "c": [None, None, None, "x"],
            #         "o": [1, 2, 3, 4]
            #     }
            # }
        }

        self._execute_es_tests(test)



    def test_select_whole_document(self):
        test = {
            "data": [
                {"o": 3, "_a": [
                    {"b": "x", "v": 2},
                    {"b": "y", "v": 3}
                ]},
                {"o": 1, "_a": {"b": "x", "v": 5}},
                {"o": 2, "_a": [
                    {"b": "x", "v": 7},
                ]},
                {"o": 4, "c": "x"}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": "*",
                "sort": ["o"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"o": 1, "a": {"b": "x", "v": 5}},
                    {"o": 2, "a": {"b": "x", "v": 7}},
                    {"o": 3, "a": [
                        {"b": "x", "v": 2},
                        {"b": "y", "v": 3}
                    ]},
                    {"o": 4, "c": "x"}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["o", "a", "c"],
                "data": [
                    [1, {"b": "x", "v": 5}, None],
                    [2, {"b": "x", "v": 7}, None],
                    [3,
                        [
                            {"b": "x", "v": 2},
                            {"b": "y", "v": 3}
                        ],
                        None
                    ],
                    [4, None, "x"]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 4, "interval": 1}
                    }
                ],
                "data": {
                    "a": [
                        {"b": "x", "v": 5},
                        {"b": "x", "v": 7},
                        [
                            {"b": "x", "v": 2},
                            {"b": "y", "v": 3}
                        ],
                        None
                    ],
                    "c": [None, None, None, "x"],
                    "o": [1, 2, 3, 4]
                }
            }
        }

    def test_select_whole_nested_document(self):
        test = {
            "data": [
                {"o": 3, "_a": [
                    {"b": "x", "v": 2},
                    {"b": "y", "v": 3}
                ]},
                {"o": 1, "_a": {"b": "x", "v": 5}},
                {"o": 2, "_a": [
                    {"b": "x", "v": 7},
                ]},
                {"o": 4, "c": "x"}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index+"._a",
                "select": "*"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"o": 1, "b": "x", "v": 5},
                    {"o": 2, "b": "x", "v": 7},
                    {"o": 3, "b": "x", "v": 2},
                    {"o": 3, "b": "y", "v": 3},
                    {"o": 4, "c": "x"}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["o", "b", "v", "c"],
                "data": [
                    [1, "x", 5, None],
                    [2, "x", 7, None],
                    [3, "x", 2, None],
                    [3, "y", 3, None],
                    [4, None, None, "x"]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 5, "interval": 1}
                    }
                ],
                "data": {
                    "b": ["x", "x", "y", "x", None],
                    "v": [5, 7, 3, 2, None],
                    "c": [None, None, None, None, "x"],
                    "o": [1, 2, 3, 3, 4]
                }
            }
        }

        self._execute_es_tests(test)

    def test_deep_names_w_star(self):
        test = {
            "data": [  # LETTERS FROM action, timing, builder, harness, step
                {"a": {"_t": [
                    {"b": {"s": 1}, "h": {"s": "a-a"}},
                    {"b": {"s": 2}, "h": {"s": "a-b"}},
                    {"b": {"s": 3}, "h": {"s": "a-c"}},
                    {"b": {"s": 4}, "h": {"s": "b-d"}},
                    {"b": {"s": 5}, "h": {"s": "b-e"}},
                    {"b": {"s": 6}, "h": {"s": "b-f"}},
                ]}}
            ],
            "query": {
                "select": "a._t.*",
                "from": base_test_class.settings.backend_es.index + ".a._t",
                "where": {
                    "prefix": {
                        "h.s": "a-"
                    }
                }
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": {"_t": {"b.s": 1, "h.s": "a-a"}}},
                    {"a": {"_t": {"b.s": 2, "h.s": "a-b"}}},
                    {"a": {"_t": {"b.s": 3, "h.s": "a-c"}}}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a._t.b.s", "a._t.h.s"],
                "data": [
                    [1, "a-a"],
                    [2, "a-b"],
                    [3, "a-c"]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 3, "interval": 1}
                    }
                ],
                "data": {
                    "a._t.b.s": [1, 2, 3],
                    "a._t.h.s": ["a-a", "a-b", "a-c"]
                }
            }
        }

        self._execute_es_tests(test)


    def test_deep_names(self):
        test = {
            "data": [  # LETTERS FROM action, timing, builder, harness, step
                {"a": {"_t": [
                    {"b": {"s": 1}, "h": {"s": "a-a"}},
                    {"b": {"s": 2}, "h": {"s": "a-b"}},
                    {"b": {"s": 3}, "h": {"s": "a-c"}},
                    {"b": {"s": 4}, "h": {"s": "b-d"}},
                    {"b": {"s": 5}, "h": {"s": "b-e"}},
                    {"b": {"s": 6}, "h": {"s": "b-f"}},
                ]}}
            ],
            "query": {
                "select": "a._t",
                "from": base_test_class.settings.backend_es.index + ".a._t",
                "where": {
                    "prefix": {
                        "h.s": "a-"
                    }
                }
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": {"_t": {"b": {"s": 1}, "h": {"s": "a-a"}}}},
                    {"a": {"_t": {"b": {"s": 2}, "h": {"s": "a-b"}}}},
                    {"a": {"_t": {"b": {"s": 3}, "h": {"s": "a-c"}}}}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a._t"],
                "data": [
                    [{"b": {"s": 1}, "h": {"s": "a-a"}}],
                    [{"b": {"s": 2}, "h": {"s": "a-b"}}],
                    [{"b": {"s": 3}, "h": {"s": "a-c"}}]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 3, "interval": 1}
                    }
                ],
                "data": {
                    "a._t": [
                        {"b": {"s": 1}, "h": {"s": "a-a"}},
                        {"b": {"s": 2}, "h": {"s": "a-b"}},
                        {"b": {"s": 3}, "h": {"s": "a-c"}}
                    ],
                }
            }
        }

        self._execute_es_tests(test)

    def test_deep_agg_on_expression(self):
        # TEST WE CAN PERFORM AGGREGATES ON EXPRESSIONS OF DEEP VARIABLES
        test = {
            "data": [
                {"o": 3, "a": {"_a": [
                    {"v": "a string"},
                    {"v": "another string"}
                ]}},
                {"o": 1, "a": {"_a": {"v": "still more"}}},
                {"o": 2, "a": {"_a": [
                    {"v": "string!"},
                ]}},
                {"o": 4, "a": {}}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index+".a._a",
                "select": {"name": "l", "value": {"length": "v"}, "aggregate": "max"}
            },
            "es_query": {  # FOR REFERENCE
               "fields": [],
               "aggs": {"_nested": {
                   "nested": {"path": "a._a"},
                   "aggs": {"max_length": {"max": {"script": "(doc[\"v\"].value).length()"}}}
               }},
               "size": 10,
               "sort": []
            },
            "expecting_list": {
                "meta": {"format": "value"},
                "data": 14
            }
        }

        self._execute_es_tests(test, delete_index=False)

    def test_deep_agg_on_expression_w_shallow_where(self):
        # TEST WE CAN PERFORM AGGREGATES ON EXPRESSIONS OF DEEP VARIABLES
        test = {
            "data": [
                {"o": 3, "a": {"_a": [
                    {"v": "a string"},
                    {"v": "another string"}
                ]}},
                {"o": 1, "a": {"_a": {"v": "still more"}}},
                {"o": 2, "a": {"_a": [
                    {"v": "string!"},
                ]}},
                {"o": 4, "a": {}}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index+".a._a",
                "select": {"name": "l", "value": {"length": "v"}, "aggregate": "max"},
                "where": {"lt": {"o": 3}}
            },
            "es_query": {  # FOR REFERENCE
               "fields": [],
               "aggs": {"_nested": {
                   "nested": {"path": "a._a"},
                   "aggs": {"max_length": {"max": {"script": "(doc[\"v\"].value).length()"}}}
               }},
               "size": 10,
               "sort": []
            },
            "expecting_list": {
                "meta": {"format": "value"},
                "data": 10
            }
        }

        self._execute_es_tests(test, delete_index=False)

    def test_agg_w_complicated_where(self):

        # TEST WE CAN PERFORM AGGREGATES ON EXPRESSIONS OF DEEP VARIABLES
        test = {
            "data": [
                {"o": 3, "a": {"_a": [
                    {"v": "a string"},
                    {"v": "another string"}
                ]}},
                {"o": 1, "a": {"_a": {"v": "still more"}}},
                {"o": 2, "a": {"_a": [
                    {"v": "string!"},
                ]}},
                {"o": 4, "a": {}}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index + ".a._a",
                "select": "*",
                "where": {
                    "eq": [
                        {
                            "length": "v"
                        },
                        10
                    ]
                }
            },
            "es_query": {  # FOR REFERENCE
               "fields": [],
               "aggs": {"_nested": {
                   "nested": {"path": "a._a"},
                   "aggs": {"max_length": {"max": {"script": "(doc[\"v\"].value).length()"}}}
               }},
               "size": 10,
               "sort": []
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [{"o": 1, "v": "still more"}]
            }
        }

        self._execute_es_tests(test, delete_index=False)

    def test_setop_w_complicated_where(self):
        # TEST WE CAN PERFORM AGGREGATES ON EXPRESSIONS OF DEEP VARIABLES
        test = {
            "data": [
                {"o": 3, "a": {"_a": [
                    {"v": "a string", "s": False},
                    {"v": "another string"}
                ]}},
                {"o": 1, "a": {"_a": {
                    "v": "still more",
                    "s": False
                }}},
                {"o": 2, "a": {"_a": [
                    {"v": "string!", "s": True},
                ]}},
                {"o": 4, "a": {"_a": {"s": False}}}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index+".a._a",
                "select": ["o", "v"],
                "where": {"and": [
                    {"gte": {"o": 2}},
                    {"eq": {"s": False}}
                ]}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"o": 3, "v": "a string"},
                    {"o": 4}
                ]
            }
        }

        self._execute_es_tests(test, delete_index=False)

    def test_id_select(self):
        """
        ALWAYS GOOD TO HAVE AN ID, CALL IT "_id"
        """
        test = {
            "data": [
                {"o": 3, "a": {"_a": [
                    {"v": "a string", "s": False},
                    {"v": "another string"}
                ]}},
                {"o": 1, "a": {"_a": {
                    "v": "still more",
                    "s": False
                }}},
                {"o": 2, "a": {"_a": [
                    {"v": "string!", "s": True},
                ]}},
                {"o": 4, "a": {"_a": {"s": False}}}
            ],
            "query": {
                "select": ["_id"],
                "from": base_test_class.settings.backend_es.index+".a._a",
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"_id": Math.is_hex},
                    {"_id": Math.is_hex},
                    {"_id": Math.is_hex},
                    {"_id": Math.is_hex},
                    {"_id": Math.is_hex}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["_id"],
                "data": [
                    [Math.is_hex],
                    [Math.is_hex],
                    [Math.is_hex],
                    [Math.is_hex],
                    [Math.is_hex]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 5, "interval": 1}
                    }
                ],
                "data": {
                    "_id": [Math.is_hex, Math.is_hex, Math.is_hex, Math.is_hex, Math.is_hex]
                }
            }
        }
        self._execute_es_tests(test)

    def test_id_value_select(self):
        """
        ALWAYS GOOD TO HAVE AN ID, CALL IT "_id"
        """

        test = {
            "data": [
                {"o": 3, "a": {"_a": [
                    {"v": "a string", "s": False},
                    {"v": "another string"}
                ]}},
                {"o": 1, "a": {"_a": {
                    "v": "still more",
                    "s": False
                }}},
                {"o": 2, "a": {"_a": [
                    {"v": "string!", "s": True},
                ]}},
                {"o": 4, "a": {"_a": {"s": False}}}
            ],
            "query": {
                "select": "_id",
                "from": base_test_class.settings.backend_es.index+".a._a",
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    Math.is_hex,
                    Math.is_hex,
                    Math.is_hex,
                    Math.is_hex,
                    Math.is_hex
                ]
            }
        }
        self._execute_es_tests(test)


