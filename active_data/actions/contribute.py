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

import flask
from werkzeug.wrappers import Response

from active_data import record_request, cors_wrapper
from mo_files import File
from mo_logs import Log

CONTRIBUTE = File.new_instance("contribute.json").read_bytes()

@cors_wrapper
def send_contribute():
    """
    SEND THE contribute.json
    """
    try:
        record_request(flask.request, None, flask.request.get_data(), None)
        return Response(
            CONTRIBUTE,
            status=200
        )
    except Exception, e:
        Log.error("Could not return contribute.json", cause=e)


