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
from active_data.app import OVERVIEW
from pyLibrary.parsers import URL

from tests.base_test_class import ActiveDataBaseTest


class TestBasicRequests(ActiveDataBaseTest):

    def test_empty_request(self):
        response = self._try_till_response(self.service_url, data=b"")
        self.assertEqual(response.status_code, 400)

    def test_root_request(self):
        url = URL(self.service_url)
        url.path=""
        url = str(url)
        response = self._try_till_response(url, data=b"")
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.all_content, OVERVIEW)





