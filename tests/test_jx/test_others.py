# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#

from __future__ import absolute_import, division, unicode_literals

from mo_dots import to_data
from mo_json import value2json, json2value
from tests import error
from tests.test_jx import BaseTestCase, TEST_TABLE


class TestOther(BaseTestCase):

    def test_tuple_w_cubes(self):
        test = to_data({
            "data": [{"a": 1}, {"a": 2}],
            "query": {"from": TEST_TABLE},
            # "expecting_list": [{"a": 1}, {"a": 2}]
        })

        self.utils.fill_container(test)  # FILL AND MAKE DUMMY QUERY

        query = value2json({"tuple": [
            {"from": test.query['from'], "where": {"eq": {"a": 1}}, "meta": {"testing": True}},
            {"from": test.query['from'], "where": {"eq": {"a": 2}}, "meta": {"testing": True}},
        ]}).encode('utf8')
        # SEND  QUERY
        response = self.utils.try_till_response(self.utils.testing.query, data=query)

        if response.status_code != 200:
            error(response)

        result = json2value(response.all_content.decode('utf8'))

        self.assertEqual(result, {
            "data": [
                {"data": {".": [{"a": 1}]}},
                {"data": {".": [{"a": 2}]}},
            ]
        })

    def test_tuple(self):
        test = to_data({
            "data": [{"a": 1}, {"a": 2}],
            "query": {"from": TEST_TABLE},
            # "expecting_list": [{"a": 1}, {"a": 2}]
        })

        self.utils.fill_container(test)  # FILL AND MAKE DUMMY QUERY

        query = value2json({"tuple": [
            {"from": test.query['from'], "where": {"eq": {"a": 1}}, "format": "list", "meta": {"testing": True}},
            {"from": test.query['from'], "where": {"eq": {"a": 2}}, "format": "list", "meta": {"testing": True}},
        ]}).encode('utf8')
        # SEND  QUERY
        response = self.utils.try_till_response(self.utils.testing.query, data=query)

        if response.status_code != 200:
            error(response)

        result = json2value(response.all_content.decode('utf8'))

        self.assertEqual(result, {
            "data": [
                {"data": [{"a": 1}]},
                {"data": [{"a": 2}]},
            ]
        })

    def test_one_tuple(self):
        test = to_data({
            "data": [{"a": 1}, {"a": 2}],
            "query": {"from": TEST_TABLE},
            # "expecting_list": [{"a": 1}, {"a": 2}]
        })

        self.utils.fill_container(test)  # FILL AND MAKE DUMMY QUERY

        query = value2json({"tuple": {
            "from": test.query['from'],
            "where": {"eq": {"a": 1}},
            "format": "list",
            "meta": {"testing": True}
        }}).encode('utf8')
        # SEND  QUERY
        response = self.utils.try_till_response(self.utils.testing.query, data=query)

        if response.status_code != 200:
            error(response)

        result = json2value(response.all_content.decode('utf8'))

        self.assertEqual(result, {
            "data": [
                {"data": [{"a": 1}]},
            ]
        })


    def test_many_tuple(self):
        test = to_data({
            "data": [{"a": 1}, {"a": 2}],
            "query": {"from": TEST_TABLE},
            # "expecting_list": [{"a": 1}, {"a": 2}]
        })

        self.utils.fill_container(test)  # FILL AND MAKE DUMMY QUERY

        query = {
            "from": test.query['from'],
            "where": {"eq": {"a": 1}},
            "format": "list",
            "meta": {"testing": True}
        }
        expected = {"data": [{"a": 1}]}
        for i in range(40):
            query = {"tuple": query}
            expected = [expected]
        body = value2json(query).encode('utf8')
        # SEND  QUERY
        response = self.utils.try_till_response(self.utils.testing.query, data=body)

        if response.status_code != 200:
            error(response)

        result = json2value(response.all_content.decode('utf8'))
        self.assertEqual(result, {"data": expected})


    def test_zero_tuple(self):
        test = to_data({
            "data": [{"a": 1}, {"a": 2}],
            "query": {"from": TEST_TABLE},
            # "expecting_list": [{"a": 1}, {"a": 2}]
        })

        self.utils.fill_container(test)  # FILL AND MAKE DUMMY QUERY

        body = value2json({"tuple": {}}).encode('utf8')
        # SEND  QUERY
        response = self.utils.try_till_response(self.utils.testing.query, data=body)

        if response.status_code != 200:
            error(response)

        result = json2value(response.all_content.decode('utf8'))
        self.assertEqual(result, {"data": []})
