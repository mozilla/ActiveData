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

from collections import Mapping

import flask
import moz_sql_parser
from active_data import record_request, cors_wrapper
from flask import Response
from mo_dots import wrap, listwrap
from mo_json import utf82unicode, json2value, value2json
from mo_logs import Log
from mo_math import Math

from active_data.actions import save_query, send_error, test_mode_wait
from active_data.actions.jx import BLANK, QUERY_SIZE_LIMIT
from mo_logs.exceptions import Except
from mo_testing.fuzzytestcase import assertAlmostEqual
from mo_times.timer import Timer
from pyLibrary.convert import unicode2utf8
from pyLibrary.queries import jx, wrap_from
from pyLibrary.queries.containers import Container


@cors_wrapper
def sql_query(path):
    query_timer = Timer("total duration")
    request_body = None
    try:
        with query_timer:
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
                text = utf82unicode(request_body)
                data = json2value(text)
                record_request(flask.request, data, None, None)
                if data.meta.testing:
                    test_mode_wait(data)

            translate_timer = Timer("translate")
            with translate_timer:
                if not data.sql:
                    Log.error("Expecting a `sql` parameter")
                jx_query = parse_sql(data.sql)
                frum = wrap_from(jx_query['from'])
                result = jx.run(jx_query, frum=frum)
                if isinstance(result, Container):  #TODO: REMOVE THIS CHECK, jx SHOULD ALWAYS RETURN Containers
                    result = result.format(jx_query.format)
                result.meta.jx_query = jx_query

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
                response_data = unicode2utf8(value2json(result))

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
    except Exception as e:
        e = Except.wrap(e)
        return send_error(query_timer, request_body, e)


KNOWN_SQL_AGGREGATES = {"sum", "count", "avg"}


def parse_sql(sql):
    query = wrap(moz_sql_parser.parse(sql))
    # PULL OUT THE AGGREGATES
    for s in listwrap(query.select):
        val = s.value
        # LOOK FOR GROUPBY COLUMN IN SELECT CLAUSE, REMOVE DUPLICATION
        for g in listwrap(query.groupby):
            try:
                assertAlmostEqual(g.value, val, "")
                g.name = s.name
                s.value = None  # MARK FOR REMOVAL
                break
            except Exception as e:
                pass

        if isinstance(val, Mapping):
            for a in KNOWN_SQL_AGGREGATES:
                if val[a]:
                    s.aggregate = a
                    s.value = val[a]
    query.select = [s for s in listwrap(query.select) if s.value != None]
    query.format = "table"
    return query
