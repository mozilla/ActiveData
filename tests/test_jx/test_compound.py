# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#

from __future__ import absolute_import, division, unicode_literals

from tests.test_jx import BaseTestCase, TEST_TABLE


class TestOther(BaseTestCase):
    def test_parallel(self):
        # TEST SHALLOW SELECTED EVEN IF DEEP NOT EXISTS
        test = {
            "data": [
                {"b": 8},
                {"a": [{"v": 1}], "b": 9},
                {"a": [{"v": 2}, {"v": 3}], "b": 10},
                {},
                {"a": [{"v": 4}]},
                {"a": [{"v": 5}, {"v": 6}]},
            ],
            "query": {
                "from": TEST_TABLE,
                "select": [{"name": "a", "value": {"from": "a", "select": "v"}}, "b"],
            },
        }

    def test_serial(self):
        # TEST SHALLOW MATCH ONLY IF DEEP SELECTED
        test = {
            "data": [
                {"b": 8},
                {"a": [{"v": 1}], "b": 9},
                {"a": [{"v": 2}, {"v": 3}], "b": 10},
                {},
                {"a": [{"v": 4}]},
                {"a": [{"v": 5}, {"v": 6}]},
            ],
            "query": {
                "from": TEST_TABLE,
                "select": [{"name": "a", "value": {"from": "a", "select": "v"}}, "b"],
                "where": {"exists": "a"}
            },
        }

