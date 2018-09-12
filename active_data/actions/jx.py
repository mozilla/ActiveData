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
from mo_dots import listwrap

from active_data import record_request, cors_wrapper
from flask import Response
from mo_files import File
from mo_logs import Log
from mo_math import Math
from pyLibrary import convert

from active_data.actions import save_query, send_error, test_mode_wait
from mo_logs.exceptions import Except
from mo_logs.profiles import CProfiler
from mo_times.timer import Timer
from pyLibrary.queries import jx, wrap_from
from pyLibrary.queries.containers import Container

BLANK = convert.unicode2utf8(File("active_data/public/error.html").read())
QUERY_SIZE_LIMIT = 10*1024*1024


@cors_wrapper
def jx_query(path):
    with CProfiler():
        try:
            with Timer("total duration") as query_timer:
                preamble_timer = Timer("preamble")
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
                        Log.error("Query is too large")

                    request_body = flask.request.get_data().strip()
                    text = convert.utf82unicode(request_body)
                    data = convert.json2value(text)
                    record_request(flask.request, data, None, None)
                    if data.meta.testing:
                        test_mode_wait(data)

                translate_timer = Timer("translate")
                with translate_timer:
                    frum = wrap_from(data['from'])
                    if any(g == "result.test" or g.value == "result.test" for g in listwrap(data.groupby) + listwrap(data.edges)):
                        return send_error(query_timer, request_body, Except(template="not allowed to groupby result.test"))
                    result = jx.run(data, frum=frum)

                    if isinstance(result, Container):  #TODO: REMOVE THIS CHECK, jx SHOULD ALWAYS RETURN Containers
                        result = result.format(data.format)

                save_timer = Timer("save")
                with save_timer:
                    if data.meta.save:
                        try:
                            result.meta.saved_as = save_query.query_finder.save(data)
                        except Exception, e:
                            Log.warning("Unexpected save problem", cause=e)

                result.meta.timing.preamble = Math.round(preamble_timer.duration.seconds, digits=4)
                result.meta.timing.translate = Math.round(translate_timer.duration.seconds, digits=4)
                result.meta.timing.save = Math.round(save_timer.duration.seconds, digits=4)
                result.meta.timing.total = "{{TOTAL_TIME}}"  # TIMING PLACEHOLDER

                with Timer("jsonification") as json_timer:
                    response_data = convert.unicode2utf8(convert.value2json(result))

            with Timer("post timer"):
                # IMPORTANT: WE WANT TO TIME OF THE JSON SERIALIZATION, AND HAVE IT IN THE JSON ITSELF.
                # WE CHEAT BY DOING A (HOPEFULLY FAST) STRING REPLACEMENT AT THE VERY END
                timing_replacement = b'"total": ' + str(Math.round(query_timer.duration.seconds, digits=4)) +\
                                     b', "jsonification": ' + str(Math.round(json_timer.duration.seconds, digits=4))
                response_data = response_data.replace(b'"total": "{{TOTAL_TIME}}"', timing_replacement)
                Log.note("Response is {{num}} bytes in {{duration}}", num=len(response_data), duration=query_timer.duration)

                return Response(
                    response_data,
                    status=200,
                    headers={
                        "Content-Type": result.meta.content_type
                    }
                )
        except Exception, e:
            e = Except.wrap(e)
            return send_error(query_timer, request_body, e)


