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

from mo_dots import wrap

from mo_json import json2value, utf82unicode
from mo_logs import Log
from mo_logs.url import URL
from tests.test_jx import BaseTestCase


class TestSQL(BaseTestCase):

    def test_simple_request(self):
        if not self.utils.testing.tuid:
            Log.error("This test requires a `testing.tuid` parameter in the config file")

        response = self.utils.post_till_response(
            self.utils.testing.tuid,
            json={
                "from": "files",
                "where": {"and": [
                    {"eq": {"revision": "29dcc9cb77c372c97681a47496488ec6c623915d"}},
                    {"in": {"path": ["gfx/thebes/gfxFontVariations.h"]}}
                ]}
            }
        )
        self.assertEqual(response.status_code, 200)
        details = json2value(utf82unicode(response.content))

        list_response = wrap([
            {h: v for h, v in zip(details.header, r)}
            for r in details.data
        ])
        tuids = list_response[0].tuids

        assert len(tuids) == 41  # 41 lines expected
        assert len(set(tuids)) == 41  # tuids must be unique

