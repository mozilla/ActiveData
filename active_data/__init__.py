# encoding: utf-8
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

from active_data.actions import save_query
from pyLibrary.dot import wrap
from pyLibrary.times.dates import Date



request_log_queue = None


def record_request(request, query_, data, error):
    if request_log_queue == None:
        return

    log = wrap({
        "timestamp": Date.now(),
        "http_user_agent": request.headers.get("user_agent"),
        "http_accept_encoding": request.headers.get("accept_encoding"),
        "path": request.headers.environ["werkzeug.request"].full_path,
        "content_length": request.headers.get("content_length"),
        "remote_addr": request.remote_addr,
        "query": query_,
        "data": data,
        "error": error
    })
    log["from"] = request.headers.get("from")
    request_log_queue.add({"value": log})


