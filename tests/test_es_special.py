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

from jx_base.meta_columns import META_COLUMNS_NAME
from mo_dots import wrap
from mo_times import Date
from pyLibrary.sql.sqlite import Sqlite, sql_insert
from tests.test_jx import BaseTestCase, TEST_TABLE


class TestESSpecial(BaseTestCase):
    """
    TESTS THAT COVER ES SPECIAL FEATURES
    """

    def test_query_on_es_base_field(self):
        schema = {
            "settings": {"analysis": {
                "analyzer": {"whiteboard_tokens": {
                    "type": "custom",
                    "tokenizer": "whiteboard_tokens_pattern",
                    "filter": ["stop"]
                }},
                "tokenizer": {"whiteboard_tokens_pattern": {
                    "type": "pattern",
                    "pattern": "\\s*([,;]*\\[|\\][\\s\\[]*|[;,])\\s*"
                }}
            }},
            "mappings": {"test_result": {
                "properties": {"status_whiteboard": {
                    "type": "keyword",
                    "store": True,
                    "fields": {"tokenized": {"type": "text", "analyzer": "whiteboard_tokens"}}
                }}
            }}
        }

        test = {
            "schema": schema,
            "data": [
                {
                    "bug_id": 123,
                    "status_whiteboard": "[test][fx21]"
                }
            ],
            "query": {
                "select": ["status_whiteboard"],
                "from": TEST_TABLE
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [{"status_whiteboard": "[test][fx21]"}]
            }
        }
        self.utils.execute_tests(test)

    def test_query_on_es_sub_field(self):
        schema = {
            "settings": {"analysis": {
                "analyzer": {"whiteboard_tokens": {
                    "type": "custom",
                    "tokenizer": "whiteboard_tokens_pattern",
                    "filter": ["stop"]
                }},
                "tokenizer": {"whiteboard_tokens_pattern": {
                    "type": "pattern",
                    "pattern": "\\s*([,;]*\\[|\\][\\s\\[]*|[;,])\\s*"
                }}
            }},
            "mappings": {"test_result": {
                "properties": {"status_whiteboard": {
                    "type": "keyword",
                    "store": True,
                    "fields": {"tokenized": {"type": "text", "analyzer": "whiteboard_tokens"}}
                }}
            }}
        }

        test = {
            "schema": schema,
            "data": [
                {
                    "bug_id": 123,
                    "status_whiteboard": "[test][fx21]"
                },
                {
                    "bug_id": 124,
                    "status_whiteboard": "[test]"
                },
                {
                    "bug_id": 125
                },
            ],
            "query": {
                "select": ["bug_id"],
                "from": TEST_TABLE,
                "where": {"eq": {"status_whiteboard.tokenized": "test"}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [{"bug_id": 123}, {"bug_id": 124}]
            }
        }
        self.utils.execute_tests(test)

    def test_query_on_null_startswith(self):
        schema = {
            "mappings": {"test_result": {
                "properties": {"name": {
                    "type": "keyword",
                    "store": True
                }}
            }}
        }

        test = {
            "schema": schema,
            "data": [
            ],
            "query": {
                "where": {"prefix": {"no_name": "something"}},
                "from": TEST_TABLE
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": []
            }
        }
        self.utils.execute_tests(test)

    def test_prefix_uses_prefix(self):
        test = {
            "data": [
                {"a": "test"},
                {"a": "testkyle"},
                {"a": None}
            ],
            "query": {
                "from": TEST_TABLE,
                "where": {"prefix": {"a": "test"}}
            },
            "expecting_list": {
                "meta": {
                    "format": "list",
                    "es_query": {
                        "from": 0,
                        "query": {"prefix": {"a.~s~": "test"}},
                        "size": 10
                    }
                },
                "data": [
                    {"a": "test"},
                    {"a": "testkyle"}
                ]
            }
        }

        self.utils.execute_tests(test)

    def test_bad_exists_properties(self):
        test = {
            "data": [{"~e~": 1}, {"~e~": 1}],
            "query": {
                "from": TEST_TABLE,
                "select": [{"name": "count", "aggregate": "count"}],
            },
            "expecting_list": {
                "meta": {"format": "value"},
                "data": {"count": 2}
            }
        }

        subtest = wrap(test)

        cont = self.utils.fill_container(subtest, typed=False)
        db = Sqlite(filename="metadata.localhost.sqlite")
        try:
            with db.transaction() as t:
                t.execute(sql_insert(
                    META_COLUMNS_NAME,
                    {
                        "name": ".",
                        "es_type": "object",
                        "jx_type": "exists",
                        "nested_path": '["."]',
                        "es_column": ".",
                        "es_index": cont.alias,
                        "last_updated": Date.now()
                    }
                ))
        except Exception as e:
            pass
        try:
            with db.transaction() as t:
                t.execute(sql_insert(
                    META_COLUMNS_NAME,
                    {
                        "name": "~e~",
                        "es_type": "long",
                        "jx_type": "exists",
                        "nested_path": '["."]',
                        "es_column": "~e~",
                        "es_index": cont.alias,
                        "last_updated": Date.now()

                    }
                ))
        except Exception as e:
            pass

        self.utils.send_queries(subtest)
