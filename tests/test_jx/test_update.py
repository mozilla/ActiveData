# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import division
from __future__ import unicode_literals

import jx_elasticsearch
from mo_dots import wrap
from test_jx import BaseTestCase, TEST_TABLE


class TestUpdate(BaseTestCase):

    def test_new_field(self):
        settings = self.utils.fill_container(
            wrap({"data": [
                {"a": 1, "b": 5},
                {"a": 3, "b": 4},
                {"a": 4, "b": 3},
                {"a": 6, "b": 2},
                {"a": 2}
            ]}),
            typed=True
        )
        container = jx_elasticsearch.new_instance(self._es_test_settings)
        container.update({
            "update": settings.index,
            "set": {"c": {"add": ["a", "b"]}}
        })

        self.utils.send_queries({
            "query": {
                "from": TEST_TABLE,
                "select": "c"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [6, 7, 7, 8, 2]
            }
        })
