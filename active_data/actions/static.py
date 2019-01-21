# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

import mimetypes

import flask
from werkzeug.wrappers import Response

from active_data import record_request
from mo_files import File
from mo_logs import Log
from mo_threads.threads import RegisterThread
from mo_times.durations import DAY
from pyLibrary.env.flask_wrappers import cors_wrapper
from pyLibrary.meta import cache

STATIC_DIRECTORY = File.new_instance("active_data/public")


@cors_wrapper
def download(filename):
    """
    DOWNLOAD FILE CONTENTS
    :param filename:  URL PATH
    :return: Response OBJECT WITH FILE CONTENT
    """
    with RegisterThread():
        try:
            record_request(flask.request, None, flask.request.get_data(), None)
            content, status, mimetype = _read_file(filename)
            return Response(
                content,
                status=status,
                headers={
                    "Content-Type": mimetype
                }
            )
        except Exception as e:
            Log.error("Could not get file {{file}}", file=filename, cause=e)


@cors_wrapper
def send_favicon():
    with RegisterThread():
        try:
            record_request(flask.request, None, flask.request.get_data(), None)
            content, status, mimetype = _read_file("favicon.ico")
            return Response(
                content,
                status=status,
                headers={
                    "Content-Type": "image/x-icon"
                }
            )
        except Exception as e:
            Log.error("Could not get file {{file}}", file="favicon.ico", cause=e)


@cache(duration=DAY)
def _read_file(filename):
    try:
        file = STATIC_DIRECTORY / filename
        if not file.abspath.startswith(STATIC_DIRECTORY.abspath):
            return "", 404, "text/html"

        Log.note("Read {{file}}", file=file.abspath)
        mimetype, encoding = mimetypes.guess_type(file.extension)
        if not mimetype:
            mimetype = "text/html"
        return file.read_bytes(), 200, mimetype
    except Exception:
        return "", 404, "text/html"
