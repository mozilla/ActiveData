# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

import flask
from werkzeug.wrappers import Response

from active_data import record_request
from mo_files import File
from mo_logs import Log
from pyLibrary.env.flask_wrappers import cors_wrapper

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
    except Exception as e:
        Log.error("Could not return contribute.json", cause=e)


