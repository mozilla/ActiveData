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
from tests.base_test_class import ActiveDataBaseTest



class TestAggOps(ActiveDataBaseTest):

    def test_simplest(self):
        test = {
            "data": [{"a": i} for i in range(30)],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"aggregate": "count"}
            },
            "expecting_list": {
                "meta": {"format": "value"}, "data": 30
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["count"],
                "data": [[30]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [],
                "data": {
                    "count": 30
                }
            }
        }
        self._execute_es_tests(test)

    def test_max(self):
        test = {
            "data": [{"a": i*2} for i in range(30)],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"value": "a", "aggregate": "max"}
            },
            "expecting_list": {
                "meta": {"format": "value"}, "data": 58
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a"],
                "data": [[58]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [],
                "data": {
                    "a": 58
                }
            }
        }
        self._execute_es_tests(test)


    def test_median(self):
        test = {
            "data": [{"a": i**2} for i in range(30)],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"value": "a", "aggregate": "median"}
            },
            "expecting_list": {
                "meta": {"format": "value"}, "data": 210.5
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a"],
                "data": [[210.5]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [],
                "data": {
                    "a": 210.5
                }
            }
        }
        self._execute_es_tests(test)


    def test_percentile(self):
        test = {
            "data": [{"a": i**2} for i in range(30)],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"value": "a", "aggregate": "percentile", "percentile": 0.90}
            },
            "expecting_list": {
                "meta": {"format": "value"}, "data": 681.3
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a"],
                "data": [[681.3]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [],
                "data": {
                    "a": 681.3
                }
            }
        }
        self._execute_es_tests(test)


# TODO:  FIX WHATEVER IS HAPPENING HERE
#
#     {
#         "select": [
#             {
#                 "name": "average",
#                 "value": "result.duration",
#                 "aggregate": "average"
#             },
#             {
#                 "name": "95percentile",
#                 "value": "result.duration",
#                 "aggregate": "percentile",
#                 "percentile": "0.95"
#             }
#         ],
#         "from": "unittest",
#         "edges": [
#             "result.result",
#             "build.branch"
#         ],
#         "where": {
#             "and": [
#                 {
#                     "eq": {
#                         "result.test": "browser/base/content/test/general/browser_aboutHealthReport.js"
#                     }
#                 },
#                 {
#                     "gt": {
#                         "run.timestamp": "{{today-week}}"
#                     }
#                 },
#                 {
#                     "neq": {
#                         "result.result": "SKIP"
#                     }
#                 }
#             ]
#         },
#         "limit": 10000
#     }

# Call to ActiveData failed
# 	File ESQueryRunner.js, line 33, in ActiveDataQuery
# 	File thread.js, line 246, in Thread_prototype_resume
# 	File thread.js, line 226, in Thread_prototype_resume/retval
# 	File Rest.js, line 46, in Rest.send/ajaxParam.error
# 	File Rest.js, line 104, in Rest.send/request.onreadystatechange
# caused by Error while calling /query
# caused by Bad response (400)
# caused by problem
# 	File qb_usingES.py, line 150, in query
# 	File qb.py, line 52, in run
# 	File app.py, line 109, in query
# 	File app.py, line 1461, in dispatch_request
# 	File app.py, line 1475, in full_dispatch_request
# 	File app.py, line 1817, in wsgi_app
# 	File app.py, line 1836, in __call__
# 	File serving.py, line 168, in execute
# 	File serving.py, line 180, in run_wsgi
# 	File serving.py, line 238, in handle_one_request
# 	File BaseHTTPServer.py, line 340, in handle
# 	File serving.py, line 203, in handle
# 	File SocketServer.py, line 655, in __init__
# 	File SocketServer.py, line 334, in finish_request
# 	File SocketServer.py, line 599, in process_request_thread
# 	File threading.py, line 766, in run
# 	File threading.py, line 813, in __bootstrap_inner
# 	File threading.py, line 786, in __bootstrap
# caused by invalid literal for float(): 0.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.950.95
# 	File __init__.py, line 165, in round
# 	File aggs.py, line 123, in es_aggsop
# 	File qb_usingES.py, line 137, in query
# 	File qb.py, line 52, in run
# 	File app.py, line 109, in query
# 	File app.py, line 1461, in dispatch_request
# 	File app.py, line 1475, in full_dispatch_request
# 	File app.py, line 1817, in wsgi_app
# 	File app.py, line 1836, in __call__
# 	File serving.py, line 168, in execute
# 	File serving.py, line 180, in run_wsgi
# 	File serving.py, line 238, in handle_one_request
# 	File BaseHTTPServer.py, line 340, in handle
# 	File serving.py, line 203, in handle
# 	File SocketServer.py, line 655, in __init__
# 	File SocketServer.py, line 334, in finish_request
# 	File SocketServer.py, line 599, in process_request_thread
# 	File threading.py, line 766, in run
# 	File threading.py, line 813, in __bootstrap_inner
# 	File threading.py, line 786, in __bootstrap




    def test_many_aggs_on_one_column(self):
        # ES WILL NOT ACCEPT TWO (NAIVE) AGGREGATES ON SAME FIELD, COMBINE THEM USING stats AGGREGATION
        test = {
            "data": [{"a": i*2} for i in range(30)],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": [
                    {"name": "maxi", "value": "a", "aggregate": "max"},
                    {"name": "mini", "value": "a", "aggregate": "min"},
                    {"name": "count", "value": "a", "aggregate": "count"}
                ]
            },
            "expecting_list": {
                "meta": {"format": "value"},
                "data": {"mini": 0, "maxi": 58, "count": 30}
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["mini", "maxi", "count"],
                "data": [
                    [0, 58, 30]
                ]
            }
        }
        self._execute_es_tests(test)


    def test_simplest_on_value(self):
        test = {
            "data": range(30),
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"aggregate": "count"}
            },
            "expecting_list": {
                "meta": {"format": "value"}, "data": 30
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["count"],
                "data": [[30]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [],
                "data": {
                    "count": 30
                }
            }
        }
        self._execute_es_tests(test, tjson=True)

    def test_max_on_value(self):
        test = {
            "data": [{"a": i*2} for i in range(30)],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"value": ".", "aggregate": "max"}
            },
            "expecting_list": {
                "meta": {"format": "value"}, "data": 58
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["max"],
                "data": [[58]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [],
                "data": {
                    "max": 58
                }
            }
        }
        self._execute_es_tests(test, tjson=True)


    def test_max_object_on_value(self):
        test = {
            "data": [{"a": i*2} for i in range(30)],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": [{"value": ".", "aggregate": "max"}]
            },
            "expecting_list": {
                "meta": {"format": "value"}, "data": {"max": 58}
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["max"],
                "data": [[58]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [],
                "data": {
                    "max": 58
                }
            }
        }
        self._execute_es_tests(test, tjson=True)


    def test_median_on_value(self):
        test = {
            "data": [i**2 for i in range(30)],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": {"value": ".", "aggregate": "median"}
            },
            "expecting_list": {
                "meta": {"format": "value"}, "data": 210.5
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["median"],
                "data": [[210.5]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [],
                "data": {
                    "median": 210.5
                }
            }
        }
        self._execute_es_tests(test, tjson=True)


    def test_many_aggs_on_value(self):
        # ES WILL NOT ACCEPT TWO (NAIVE) AGGREGATES ON SAME FIELD, COMBINE THEM USING stats AGGREGATION
        test = {
            "data": [i*2 for i in range(30)],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": [
                    {"name": "maxi", "value": ".", "aggregate": "max"},
                    {"name": "mini", "value": ".", "aggregate": "min"},
                    {"name": "count", "value": ".", "aggregate": "count"}
                ]
            },
            "expecting_list": {
                "meta": {"format": "value"},
                "data": {"mini": 0, "maxi": 58, "count": 30}
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["mini", "maxi", "count"],
                "data": [
                    [0, 58, 30]
                ]
            }
        }
        self._execute_es_tests(test, tjson=True)

    def test_cardinality(self):
        test = {
            "data": [
                {"a": 1, "b": "x"},
                {"a": 1, "b": "x"},
                {"a": 2, "b": "x"},
                {"a": 2, "d": "x"},
                {"a": 3, "d": "x"},
                {"a": 3, "d": "x"},
                {"a": 3, "d": "x"},
            ],
            "query": {
                "from": base_test_class.settings.backend_es.index,
                "select": [
                    {"value": "a", "aggregate": "cardinality"},
                    {"value": "b", "aggregate": "cardinality"},
                    {"value": "c", "aggregate": "cardinality"},
                    {"value": "d", "aggregate": "cardinality"}
                ]
            },
            "expecting_list": {
                "meta": {"format": "value"},
                "data": {"a": 3, "b": 1, "c": 0, "d": 1}
            }
        }
        self._execute_es_tests(test, tjson=False)

#TODO: AGGREGATING ON CONSTANT DOES NOT SEEM TO WORK

test = {
    "from": base_test_class.settings.backend_es.index,
    "select": {"name": "count", "value": "1", "aggregate": "count"},
    "edges": ["a"],
    "esfilter": True,
    "limit": 10
}

#TODO: SIMPLE COUNT NOT WORKING
example = {
    "from": "jobs.action.timings",
    "where": {"eq": {"build.name": "Windows XP 32-bit try opt test mochitest-1"}},
    "select": [
        {
            "name": "duration",
            "aggregate": "average",
            "value": "action.timings.builder.duration"
        },
        {"aggregate": "count"}
    ],
    "format": "table"
}
