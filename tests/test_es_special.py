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

from mo_dots import set_default, wrap
from mo_future import text
from mo_threads import Till
from mo_times import MINUTE
from pyLibrary.env import http
from tests.test_jx import BaseTestCase, TEST_TABLE


class TestESSpecial(BaseTestCase):
    """
    TESTS THAT COVER ES SPECIAL FEATURES
    """

    def test_query_on_es_base_field(self):
        schema = {
            "settings": {
                "analysis": {
                    "analyzer": {
                        "whiteboard_tokens": {
                            "type": "custom",
                            "tokenizer": "whiteboard_tokens_pattern",
                            "filter": ["stop"],
                        }
                    },
                    "tokenizer": {
                        "whiteboard_tokens_pattern": {
                            "type": "pattern",
                            "pattern": "\\s*([,;]*\\[|\\][\\s\\[]*|[;,])\\s*",
                        }
                    },
                }
            },
            "mappings": {
                "test_result": {
                    "properties": {
                        "status_whiteboard": {
                            "type": "keyword",
                            "store": True,
                            "fields": {
                                "tokenized": {
                                    "type": "text",
                                    "analyzer": "whiteboard_tokens",
                                }
                            },
                        }
                    }
                }
            },
        }

        test = {
            "schema": schema,
            "data": [{"bug_id": 123, "status_whiteboard": "[test][fx21]"}],
            "query": {"select": ["status_whiteboard"], "from": TEST_TABLE},
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [{"status_whiteboard": "[test][fx21]"}],
            },
        }
        self.utils.execute_tests(test)

    def test_query_on_es_sub_field(self):
        schema = {
            "settings": {
                "analysis": {
                    "analyzer": {
                        "whiteboard_tokens": {
                            "type": "custom",
                            "tokenizer": "whiteboard_tokens_pattern",
                            "filter": ["stop"],
                        }
                    },
                    "tokenizer": {
                        "whiteboard_tokens_pattern": {
                            "type": "pattern",
                            "pattern": "\\s*([,;]*\\[|\\][\\s\\[]*|[;,])\\s*",
                        }
                    },
                }
            },
            "mappings": {
                "test_result": {
                    "properties": {
                        "status_whiteboard": {
                            "type": "keyword",
                            "store": True,
                            "fields": {
                                "tokenized": {
                                    "type": "text",
                                    "analyzer": "whiteboard_tokens",
                                }
                            },
                        }
                    }
                }
            },
        }

        test = {
            "schema": schema,
            "data": [
                {"bug_id": 123, "status_whiteboard": "[test][fx21]"},
                {"bug_id": 124, "status_whiteboard": "[test]"},
                {"bug_id": 125},
            ],
            "query": {
                "select": ["bug_id"],
                "from": TEST_TABLE,
                "where": {"eq": {"status_whiteboard.tokenized": "test"}},
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [{"bug_id": 123}, {"bug_id": 124}],
            },
        }
        self.utils.execute_tests(test)

    def test_query_on_null_startswith(self):
        schema = {
            "mappings": {
                "test_result": {
                    "properties": {"name": {"type": "keyword", "store": True}}
                }
            }
        }

        test = {
            "schema": schema,
            "data": [],
            "query": {
                "where": {"prefix": {"no_name": "something"}},
                "from": TEST_TABLE,
            },
            "expecting_list": {"meta": {"format": "list"}, "data": []},
        }
        self.utils.execute_tests(test)

    def test_prefix_uses_prefix(self):
        test = {
            "data": [{"a": "test"}, {"a": "testkyle"}, {"a": None}],
            "query": {"from": TEST_TABLE, "where": {"prefix": {"a": "test"}}},
            "expecting_list": {
                "meta": {
                    "format": "list",
                    "es_query": {
                        "from": 0,
                        "query": {"prefix": {"a.~s~": "test"}},
                        "size": 10,
                    },
                },
                "data": [{"a": "test"}, {"a": "testkyle"}],
            },
        }

        self.utils.execute_tests(test)

    def test_bulk_query(self):
        data = wrap([{"a": "test" + text(i)} for i in range(1001)])
        expected = [{"a": r.a, "count": 1} for r in data]

        test = wrap({
            "data": data,
            "query": {
                "from": TEST_TABLE,
                "groupby": "a",
                "limit": len(data),
                "format": "list",
            },
            "expecting_list": {"data": expected},  # DUMMY< TO ENSURE LOADED
        })

        self.utils.execute_tests(test)
        result = http.post_json(
            url=self.utils.testing.query,
            json=set_default({"meta": {"big": True}, "limit": 100}, test.query),
        )

        timeout = Till(seconds=MINUTE.seconds)
        while not timeout:
            try:
                content = http.get(result.url)
                self.assertEqual(content, expected)
                break
            except Exception:
                Till(seconds=2).wait()
