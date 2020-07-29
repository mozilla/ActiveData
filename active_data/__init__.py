# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from mo_dots import coalesce, dict_to_data
from mo_files import File
from mo_json import value2json
from mo_logs import Log
from mo_times.dates import Date

request_log_queue = None

OVERVIEW = File("active_data/public/index.html").read_bytes()


def record_request(request, query_, data, error):
    try:
        if request_log_queue == None:
            return

        if data and len(data)>10000:
            data = data[:10000]

        log = dict_to_data({
            "timestamp": Date.now(),
            "http_user_agent": request.headers.get("user_agent"),
            "http_accept_encoding": request.headers.get("accept_encoding"),
            "referer": request.headers.get("x-referer"),
            "path": request.headers.environ["werkzeug.request"].full_path,
            "content_length": request.headers.get("content_length"),
            "remote_addr": coalesce(request.headers.get("x-remote-addr"), request.remote_addr),
            "query_text": value2json(query_) if query_ else None,
            "data": data if data else None,
            "error": value2json(error) if error else None
        })
        log["from"] = request.headers.get('from')
        request_log_queue.add({"value": log})
    except Exception as e:
        Log.warning("Can not record", cause=e)


