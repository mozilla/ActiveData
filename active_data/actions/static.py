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

import mimetypes

import flask
from werkzeug.wrappers import Response

from active_data import record_request, cors_wrapper
from pyLibrary.debugs.logs import Log
from pyLibrary.env.files import File
from pyLibrary.meta import cache
from pyLibrary.times.durations import DAY

STATIC_DIRECTORY = File.new_instance("active_data/public")

@cors_wrapper
def download(filename):
    """
    DOWNLOAD FILE CONTENTS
    :param filename:  URL PATH
    :return: Response OBJECT WITH FILE CONTENT
    """
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
    except Exception, e:
        Log.error("Could not get file {{file}}", file=filename, cause=e)


@cache(duration=DAY)
def _read_file(filename):
    try:
        file = File.new_instance(STATIC_DIRECTORY, filename)
        if not file.abspath.startswith(STATIC_DIRECTORY.abspath):
            return "", 404, "text/html"

        Log.note("Read {{file}}", file=file.abspath)
        mimetype, encoding = mimetypes.guess_type(file.extension)
        if not mimetype:
            mimetype = "text/html"
        return file.read_bytes(), 200, mimetype
    except Exception:
        return "", 404, "text/html"
