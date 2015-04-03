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
from pyLibrary.dot import wrap
from pyLibrary.queries import query
from tests.base_test_class import ActiveDataBaseTest


lots_of_data = wrap([{"a": i} for i in range(30)])


class TestSetOps(ActiveDataBaseTest):

    def test_simplest(self):
        test = {
            "data": [
                {"a": "b"}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": "a"
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {"a": "b"}
            ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a"],
                "data": [["b"]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 1, "interval": 1}
                    }
                ],
                "data": {
                    "a": ["b"]
                }
            }
        }
        self._execute_es_tests(test)

    def test_single_deep_select(self):

        test = {
            "data": [  # PROPERTIES STARTING WITH _ ARE NOT NESTED AUTOMATICALLY
                {"_a": {"_b": {"_c": 1}}},
                {"_a": {"_b": {"_c": 2}}},
                {"_a": {"_b": {"_c": 3}}},
                {"_a": {"_b": {"_c": 4}}},
                {"_a": {"_b": {"_c": 5}}}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": "_a._b._c",
                "sort": "_a._b._c"  # SO THE CUBE COMPARISON WILL PASS
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {"_a": {"_b": {"_c": 1}}},
                {"_a": {"_b": {"_c": 2}}},
                {"_a": {"_b": {"_c": 3}}},
                {"_a": {"_b": {"_c": 4}}},
                {"_a": {"_b": {"_c": 5}}}
            ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["_a._b._c"],
                "data": [[1], [2], [3], [4], [5]]
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
                    "_a._b._c": [1, 2, 3, 4, 5]
                }
            }
        }
        self._execute_es_tests(test)


    def test_single_select_alpha(self):
        test = {
            "data": [
                {"a": "b"}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": "a"
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {"a": "b"}
            ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a"],
                "data": [["b"]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 1, "interval": 1}
                    }
                ],
                "data": {
                    "a": ["b"]
                }
            }
        }
        self._execute_es_tests(test)


    def test_single_rename(self):
        test = {
            "name": "rename singleton alpha",
            "data": [
                {"a": "b"}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"name": "value", "value": "a"}
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {"value": "b"}
            ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["value"],
                "data": [["b"]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 1, "interval": 1}
                    }
                ],
                "data": {
                    "value": ["b"]
                }
            }
        }
        self._execute_es_tests(test)

    def test_single_alpha_no_select(self):
        test = {
            "name": "singleton_alpha no select (select *)",
            "data": [
                {"a": "b"}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {"a": "b"}
            ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a"],
                "data": [["b"]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 1, "interval": 1}
                    }
                ],
                "data": {
                    "a": ["b"]
                }
            }
        }
        self._execute_es_tests(test)

    def test_dot_select(self):
        test = {
            "name": "singleton_alpha dot select",
            "data": [
                {"a": "b"}
            ],
            "query": {
                "select": {"name": "value", "value": "."},
                "from": base_test_class.settings.backend_es.index
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [{"value": {"a": "b"}}]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["value"],
                "data": [[{"a": "b"}]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 1, "interval": 1}
                    }
                ],
                "data": {
                    "value": [{"a": "b"}]
                }
            }
        }
        self._execute_es_tests(test)

    def test_list_of_values(self):
        test = {
            "name": "list of values",
            "not": "elasticsearch",  # CAN NOT TEST VALUES AGAINST ES
            "data": ["a", "b"],
            "query": {
                "from": base_test_class.settings.backend_es.index
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    "a", "b"
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["value"],
                "data": [["a"], ["b"]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "index",
                        "domain": {"type": "rownum", "min": 0, "max": 1, "interval": 1}
                    }
                ],
                "data": {
                    "value": ["a", "b"]
                }
            }
        }
        self._execute_es_tests(test)

    def test_select_all_from_list_of_objects(self):
        test = {
            "disable": True,  # TODO: PLEASE ENABLE, ES CAN NOT BE EXPECTED TO KEEP ORDER OF data
            "name": "select * from list of objects",
            "data": [
                {"a": "b"},
                {"a": "d"}
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": "*"
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {"a": "b"},
                {"a": "d"}
            ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a"],
                "data": [
                    ["b"],
                    ["d"]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 2, "interval": 1}
                    }
                ],
                "data": {
                    "a": ["b", "d"]
                }
            }
        }
        self._execute_es_tests(test)

    def test_select_into_children(self):
        test = {
            "name": "select into children to table",
            "disable": True,  # TODO: PLEASE ENABLE, TOO COMPLICATED FOR v1
            "metadata": {
                "properties": {
                    "x": {"type": "integer"},
                    "a": {
                        "type": "nested",
                        "properties": {
                            "y": {
                                "type": "string"
                            },
                            "b": {
                                "type": "nested",
                                "properties": {
                                    "c": {"type": "integer"},
                                    "1": {"type": "integer"}

                                }
                            },
                            "z": {
                                "type": "string"
                            }
                        }
                    }
                }
            },
            "data": [
                {"x": 5},
                {
                    "a": [
                        {
                            "b": {"c": 13},
                            "y": "m"
                        },
                        {
                            "b": [
                                {"c": 17, "1": 27},
                                {"c": 19}

                            ],
                            "y": "q"
                        },
                        {
                            "y": "r"
                        }
                    ],
                    "x": 3
                },
                {
                    "a": {"b": {"c": 23}},
                    "x": 7
                },
                {
                    "a": {"b": [
                        {"c": 29, "1": 31},
                        {"c": 37, "1": 41},
                        {"1": 47},
                        {"c": 53, "1": 59}
                    ]},
                    "x": 11
                }
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index+".a.b",
                "select": ["...x", "c"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"x": 5, "c": None},
                    {"x": 3, "c": 13},
                    {"x": 3, "c": 17},
                    {"x": 3, "c": 19},
                    {"x": 7, "c": 23},
                    {"x": 11, "c": 29},
                    {"x": 11, "c": 37},
                    {"x": 11, "c": None},
                    {"x": 11, "c": 53}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["x", "c"],
                "data": [
                    [5, None],
                    [3, 13],
                    [3, 17],
                    [3, 19],
                    [7, 23],
                    [11, 29],
                    [11, 37],
                    [11, None],
                    [11, 53]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "index",
                        "domain": {"type": "rownum", "min": 0, "max": 9, "interval": 1}
                    }
                ],
                "data": {
                    "x": [5, 3, 3, 3, 7, 11, 11, 11, 11],
                    "c": [None, 13, 17, 19, 23, 29, 37, None, 53]
                }
            }
        }
        self._execute_es_tests(test)

    def test_default_limit(self):
        test = wrap({
            "data": lots_of_data,
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"name": "value", "value": "a"},
            },
        })

        settings = self._fill_es(test)
        try:
            test.query.format = "list"
            result = self._execute_query(test.query)
            self.assertEqual(len(result.data), query.DEFAULT_LIMIT)

            test.query.format = "table"
            result = self._execute_query(test.query)
            self.assertEqual(len(result.data), query.DEFAULT_LIMIT)

            test.query.format = "cube"
            result = self._execute_query(test.query)
            self.assertEqual(len(result.data.value), query.DEFAULT_LIMIT)
        finally:
            # REMOVE CONTAINER
            self.es.delete_index(settings.index)



    def test_specific_limit(self):
        test = wrap({
            "data": lots_of_data,
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"name": "value", "value": "a"},
                "limit":5
            },
        })

        settings = self._fill_es(test)
        try:
            test.query.format = "list"
            result = self._execute_query(test.query)
            self.assertEqual(len(result.data), 5)

            test.query.format = "table"
            result = self._execute_query(test.query)
            self.assertEqual(len(result.data), 5)

            test.query.format = "cube"
            result = self._execute_query(test.query)
            self.assertEqual(len(result.data.value), 5)
        finally:
            # REMOVE CONTAINER
            self.es.delete_index(settings.index)

    def test_negative_limit(self):
        test = wrap({
            "data": lots_of_data,
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"name": "value", "value": "a"},
                "limit": -1
            },
        })

        settings = self._fill_es(test)
        try:
            test.query.format = "list"
            self.assertRaises(Exception, self._execute_query, test.query)
        finally:
            # REMOVE CONTAINER
            self.es.delete_index(settings.index)
