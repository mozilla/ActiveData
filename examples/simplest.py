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

import requests

result = requests.post("http://activedata.allizom.org/query", json={
    "from": "unittest",
    "where": {"and": [
        {"eq": {"result.test": "browser/base/content/test/general/browser_aboutHealthReport.js"}},
        {"gt": {"run.timestamp": "1454025600"}},
        {"neq": {"result.result": "SKIP"}}
    ]},
    "limit": 10000,
    "select": [
        "result.duration",
        "result.result",
        "build.branch",
        "run.timestamp",
        "build.platform",
        "build.type"
    ]
})


print result.content
