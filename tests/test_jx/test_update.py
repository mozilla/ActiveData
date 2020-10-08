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

from jx_base.expressions import NULL
from mo_dots import dict_to_data
from tests.test_jx import BaseTestCase


class TestUpdate(BaseTestCase):

    def test_new_field(self):
        settings = self.utils.fill_container(
            dict_to_data({"data": [
                {"a": 1, "b": 5},
                {"a": 3, "b": 4},
                {"a": 4, "b": 3},
                {"a": 6, "b": 2},
                {"a": 2}
            ]}),
            typed=False
        )
        import jx_elasticsearch
        container = jx_elasticsearch.new_instance(read_only=False, kwargs=self.utils._es_test_settings)
        container.update({
            "update": settings.alias,
            "set": {"c": {"add": ["a", "b"]}}
        })

        self.utils.send_queries({
            "query": {
                "from": settings.alias,
                "select": ["c", "a"]
            },
            "expecting_table": {
                "header": ["a", "c"],
                "data": [[1, 6], [3, 7], [4, 7], [6, 8], [2, NULL]]
            }
        })

    def test_delete(self):
        settings = self.utils.fill_container(
            dict_to_data({"data": [
                {"a": 1, "b": 5},
                {"a": 3, "b": 4},
                {"a": 4, "b": 3},
                {"a": 6, "b": 2},
                {"a": 2}
            ]}),
            typed=True
        )
        import jx_elasticsearch
        container = jx_elasticsearch.new_instance(read_only=False, kwargs=self.utils._es_test_settings)
        container.update({
            "update": settings.alias,
            "clear": ".",
            "where": {"lt": {"a": 4}}
        })

        self.utils.send_queries({
            "query": {
                "from": settings.alias,
                "sort":"a"
            },
            "expecting_list": {"data": [
                {"a": 4, "b": 3},
                {"a": 6, "b": 2}
            ]}
        })
