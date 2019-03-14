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
from flask import Response

from active_data import record_request
from active_data.actions import QUERY_TOO_LARGE, find_container, save_query, send_error, test_mode_wait
from jx_base.container import Container
from jx_python import jx
from mo_files import File
from mo_future import binary_type
from mo_json import json2value, value2json
from mo_logs import Except, Log
from mo_logs.strings import unicode2utf8, utf82unicode
import mo_math
from mo_threads.threads import RegisterThread
from mo_times.timer import Timer
from pyLibrary.env.flask_wrappers import cors_wrapper

BLANK = unicode2utf8(File("active_data/public/error.html").read())
QUERY_SIZE_LIMIT = 10*1024*1024


@cors_wrapper
def jx_query(path):
    with RegisterThread():
        try:
            with Timer("total duration") as query_timer:
                preamble_timer = Timer("preamble", silent=True)
                with preamble_timer:
                    if flask.request.headers.get("content-length", "") in ["", "0"]:
                        # ASSUME A BROWSER HIT THIS POINT, SEND text/html RESPONSE BACK
                        return Response(
                            BLANK,
                            status=400,
                            headers={
                                "Content-Type": "text/html"
                            }
                        )
                    elif int(flask.request.headers["content-length"]) > QUERY_SIZE_LIMIT:
                        Log.error(QUERY_TOO_LARGE)

                    request_body = flask.request.get_data().strip()
                    text = utf82unicode(request_body)
                    data = json2value(text)
                    record_request(flask.request, data, None, None)
                    if data.meta.testing:
                        test_mode_wait(data)

                translate_timer = Timer("translate", silent=True)
                with translate_timer:
                    frum = find_container(data['from'], after=None)
                    result = jx.run(data, container=frum)

                    if isinstance(result, Container):  #TODO: REMOVE THIS CHECK, jx SHOULD ALWAYS RETURN Containers
                        result = result.format(data.format)

                save_timer = Timer("save")
                with save_timer:
                    if data.meta.save:
                        try:
                            result.meta.saved_as = save_query.query_finder.save(data)
                        except Exception as e:
                            Log.warning("Unexpected save problem", cause=e)

                result.meta.timing.preamble = mo_math.round(preamble_timer.duration.seconds, digits=4)
                result.meta.timing.translate = mo_math.round(translate_timer.duration.seconds, digits=4)
                result.meta.timing.save = mo_math.round(save_timer.duration.seconds, digits=4)
                result.meta.timing.total = "{{TOTAL_TIME}}"  # TIMING PLACEHOLDER

                with Timer("jsonification", silent=True) as json_timer:
                    response_data = unicode2utf8(value2json(result))

            with Timer("post timer", silent=True):
                # IMPORTANT: WE WANT TO TIME OF THE JSON SERIALIZATION, AND HAVE IT IN THE JSON ITSELF.
                # WE CHEAT BY DOING A (HOPEFULLY FAST) STRING REPLACEMENT AT THE VERY END
                timing_replacement = (
                    b'"total":' + binary_type(mo_math.round(query_timer.duration.seconds, digits=4)) +
                    b', "jsonification":' + binary_type(mo_math.round(json_timer.duration.seconds, digits=4))
                )
                response_data = response_data.replace(b'"total":"{{TOTAL_TIME}}"', timing_replacement)
                Log.note("Response is {{num}} bytes in {{duration}}", num=len(response_data), duration=query_timer.duration)

                return Response(
                    response_data,
                    status=200,
                    headers={
                        "Content-Type": result.meta.content_type
                    }
                )
        except Exception as e:
            e = Except.wrap(e)
            return send_error(query_timer, request_body, e)


