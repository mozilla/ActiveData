# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from tests.test_jx import BaseTestCase, TEST_TABLE


class TestESSpecial(BaseTestCase):

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
