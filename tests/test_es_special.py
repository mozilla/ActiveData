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

from unittest import skip

from active_data.actions import find_container
from jx_base.container import type2container
from jx_base.expressions import NULL
from jx_elasticsearch.es52 import ES52
from jx_python import jx
from mo_dots import Data, dict_to_data, list_to_data
from mo_future import text
from mo_http import http
from mo_times import Date
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
            "data": [{
                "bug_id": 123,
                "status_whiteboard": "[test][fx21]"
            }],
            "query": {
                "select": ["status_whiteboard"],
                "from": TEST_TABLE
            },
            "expecting_list": {
                "meta": {
                    "format": "list"
                },
                "data": [{
                    "status_whiteboard": "[test][fx21]"
                }],
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
            "schema":
            schema,
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
                "where": {
                    "eq": {
                        "status_whiteboard.tokenized": "test"
                    }
                },
            },
            "expecting_list": {
                "meta": {
                    "format": "list"
                },
                "data": [{
                    "bug_id": 123
                }, {
                    "bug_id": 124
                }],
            },
        }
        self.utils.execute_tests(test)

    def test_query_on_null_startswith(self):
        schema = {
            "mappings": {
                "test_result": {
                    "properties": {
                        "name": {
                            "type": "keyword",
                            "store": True
                        }
                    }
                }
            }
        }

        test = {
            "schema": schema,
            "data": [],
            "query": {
                "where": {
                    "prefix": {
                        "no_name": "something"
                    }
                },
                "from": TEST_TABLE,
            },
            "expecting_list": {
                "meta": {
                    "format": "list"
                },
                "data": []
            },
        }
        self.utils.execute_tests(test)

    def test_prefix_uses_prefix(self):
        test = {
            "data": [{
                "a": "test"
            }, {
                "a": "testkyle"
            }, {
                "a": None
            }],
            "query": {
                "from": TEST_TABLE,
                "where": {
                    "prefix": {
                        "a": "test"
                    }
                }
            },
            "expecting_list": {
                "meta": {
                    "format": "list",
                    "es_query": {
                        "from": 0,
                        "query": {
                            "prefix": {
                                "a.~s~": "test"
                            }
                        },
                        "size": 10,
                    },
                },
                "data": [{
                    "a": "test"
                }, {
                    "a": "testkyle"
                }],
            },
        }

        self.utils.execute_tests(test)

    def test_column_not_added(self):
        index1 = "testing_merge-20191214_000000"
        index2 = "testing_merge-20191215_000000"
        common = Data(
            alias="testing_merge",
            limit_replicas=True,
            limit_replicas_warning=False,
            read_only=False,
            typed=False
        )

        cluster = self.utils._es_cluster
        try:
            cluster.delete_index(index1)
        except Exception:
            pass

        try:
            cluster.delete_index(index2)
        except Exception:
            pass

        # COLUMN WITH ZERO RECORDS
        index1 = cluster.create_index(
            index=index1,
            schema={
                "mappings": {
                    "test": {
                        "properties": {
                            "missing_value": {
                                "type": "keyword",
                                "store": True
                            }
                        }
                    }
                }
            },
            kwargs=common
        )
        index1.add_alias(common.alias)

        # INDEX WITH ONE RECORD
        index2 = cluster.create_index(
            index=index2,
            schema={
                "mappings": {
                    "test": {
                        "properties": {
                            "one_value": {
                                "type": "keyword",
                                "store": True
                            }
                        }
                    }
                }
            },
            kwargs=common
        )
        index2.add_alias(common.alias)
        index2.add({"value": {"one_value": "a value!"}})
        index2.refresh()

        # FORCE metadata MERGE
        c = find_container(common.alias, Date.now())

        # TEST SCHEMAS
        indices = cluster.get_metadata(after=Date.now()).indices
        schema1 = indices[index1.settings.index]
        schema2 = indices[index2.settings.index]

        # THE one_value GOT PICKED UP
        self.assertEqual(
            schema1, {
                "mappings": {
                    "test": {
                        "properties": {
                            "one_value": {
                                "type": "keyword",
                                "store": True
                            }
                        }
                    }
                }
            }
        )
        # THE missing_value DID NOT GET PICKED UP
        self.assertEqual(schema2, {"mappings": {"test": {"properties": {"missing_value": NULL}}}})

        try:
            cluster.delete_index(index1)
        except Exception:
            pass

        try:
            cluster.delete_index(index1)
        except Exception:
            pass

    def test_no_update(self):
        data = jx.sort([{"a": "test" + text(i)} for i in range(10)], "a")

        test = dict_to_data({
            "data": data,
            "query": {
                "from": TEST_TABLE,
                "limit": len(data),
                "sort": "a"
            },
            "expecting_list": {
                "data": data
            },  # DUMMY, TO ENSURE LOADED
        })
        self.utils.execute_tests(test)
        test.query.clear = "."
        test.query.update = test.query['from']
        test.query['from'] = None

        def result():
            http.post_json(
                url=self.utils.testing.query,
                json=test.query,
            )

        self.assertRaises(Exception, result)

    def test_missing_column_removed_from_metadata(self):
        # MAKE INDEX WITH ALIAS
        # ADD TWO COLUMNS, USE ONLY ONE
        # MAKE ANOTHER INDEX
        # VERIFY NEW INDEX GETS USED COLUMN
        # VERIFY NEW INDEX DOES NOT HAVE UNUSED COLUMN
        # DROP OLD INDEX
        # VERIFY UNUSED COLUMN IS DROPPED FROM METADATA
        # VERIFY "USED" COLUMN STILL EXISTS (BUT HAS ZERO CARDINALITY)
        pass

    # @skip("still broken")
    def test_columns_not_leaked(self):
        alias = "testing_replace"
        index_name = "testing_replace-20200527_000000"
        cluster = self.utils._es_cluster
        common = Data(
            alias=alias,
            limit_replicas=True,
            limit_replicas_warning=False,
            read_only=False,
            typed=True,
            type=cluster.settings.type,
            schema={},
        )

        # MAKE INDEX WITH ALIAS
        try:
            cluster.delete_index(index_name)
        except Exception:
            pass
        index1 = cluster.create_index(index=index_name, kwargs=common)
        index1.add_alias(common.alias)

        # ADD DATA
        index1.add({"value": {"a": 1}})
        index1.refresh()
        # REGISTER JX QUERIES TO OPERATE ON ES
        type2container.setdefault("elasticsearch", ES52)
        # FORCE RELOAD
        found_container = find_container(alias, after=Date.now())

        # VERIFY SCHEMA OF DATA
        columns = found_container.snowflake.columns
        self.assertEqual(columns.get("es_column"), {'.', '_id', 'a', 'a.~n~', '~e~'})

        # DROP INDEX
        try:
            cluster.delete_index(index_name)
        except Exception:
            pass

        # MAKE NEW INDEX, WITH SAME NAME
        index1 = cluster.create_index(index=index_name, kwargs=common)
        index1.add_alias(common.alias)

        # ADD OTHER DATA
        index1.add({"value": {"b": 2}})
        index1.refresh()

        while index1.search({"query": {"match_all": {}}, "size": 0}).hits.hits.total < 1:
            pass

        # FORCE RELOAD
        new_found_container = find_container(alias, after=Date.now())

        # VERIFY OLD SCHEMA DOES NOT EXIST
        columns = list_to_data([
            c for c in new_found_container.snowflake.columns if c.cardinality != 0
        ])
        self.assertEqual(columns.get("es_column"), {'.', '_id', 'b', 'b.~n~', '~e~'})
