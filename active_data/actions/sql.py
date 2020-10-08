# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

import flask
from flask import Response

import mo_math
import moz_sql_parser
from active_data import record_request
from active_data.actions import find_container, save_query, send_error, test_mode_wait
from active_data.actions.query import BLANK, QUERY_SIZE_LIMIT
from jx_base.container import Container
from jx_python import jx
from mo_dots import is_data, is_list, listwrap, unwraplist, to_data
from mo_files import mimetype
from mo_json import json2value, value2json
from mo_logs import Log, strings
from mo_logs.exceptions import Except
from mo_testing.fuzzytestcase import assertAlmostEqual
from mo_threads.threads import register_thread, MAIN_THREAD
from mo_times.timer import Timer
from pyLibrary.env.flask_wrappers import cors_wrapper


@cors_wrapper
@register_thread
def sql_query(path):
    query_timer = Timer("total duration")
    request_body = None
    try:
        with query_timer:
            preamble_timer = Timer("preamble", silent=True)
            with preamble_timer:
                if flask.request.headers.get("content-length", "") in ["", "0"]:
                    # ASSUME A BROWSER HIT THIS POINT, SEND text/html RESPONSE BACK
                    return Response(
                        BLANK, status=400, headers={"Content-Type": "text/html"}
                    )
                elif int(flask.request.headers["content-length"]) > QUERY_SIZE_LIMIT:
                    Log.error(
                        "Query must be under {{limit}}mb",
                        limit=QUERY_SIZE_LIMIT / 1024 / 1024,
                    )

                request_body = flask.request.get_data().strip()
                text = request_body.decode("utf8")
                data = json2value(text)
                record_request(flask.request, data, None, None)

            sql_translate_timer = Timer("sql translate", silent=True)
            with sql_translate_timer:
                if not data.sql:
                    Log.error("Expecting a `sql` parameter")
                jx_query = parse_sql(data.sql)
                if jx_query["from"] != None:
                    if data.meta.testing:
                        test_mode_wait(jx_query, MAIN_THREAD.please_stop)
                    frum = find_container(jx_query["from"], after=None)
                else:
                    frum = None
                result = jx.run(jx_query, container=frum)
                if isinstance(
                    result, Container
                ):  # TODO: REMOVE THIS CHECK, jx SHOULD ALWAYS RETURN Containers
                    result = result.format(jx_query.format)
                result.meta.jx_query = jx_query

            save_timer = Timer("save")
            with save_timer:
                if data.meta.save:
                    try:
                        result.meta.saved_as = save_query.query_finder.save(data)
                    except Exception as e:
                        Log.warning("Unexpected save problem", cause=e)

            result.meta.timing.preamble = mo_math.round(
                preamble_timer.duration.seconds, digits=4
            )
            result.meta.timing.sql_translate = mo_math.round(
                sql_translate_timer.duration.seconds, digits=4
            )
            result.meta.timing.save = mo_math.round(
                save_timer.duration.seconds, digits=4
            )
            result.meta.timing.total = "{{TOTAL_TIME}}"  # TIMING PLACEHOLDER

            with Timer("jsonification", silent=True) as json_timer:
                response_data = value2json(result).encode("utf8")

        with Timer("post timer", silent=True):
            # IMPORTANT: WE WANT TO TIME OF THE JSON SERIALIZATION, AND HAVE IT IN THE JSON ITSELF.
            # WE CHEAT BY DOING A (HOPEFULLY FAST) STRING REPLACEMENT AT THE VERY END
            timing_replacement = (
                b'"total": '
                + strings.round(query_timer.duration.seconds, digits=4).encode("utf8")
                + b', "jsonification": '
                + strings.round(json_timer.duration.seconds, digits=4).encode("utf8")
            )
            response_data = response_data.replace(
                b'"total":"{{TOTAL_TIME}}"', timing_replacement
            )
            Log.note(
                "Response is {{num}} bytes in {{duration}}",
                num=len(response_data),
                duration=query_timer.duration,
            )

            return Response(
                response_data, status=200, headers={"Content-Type": mimetype.JSON}
            )
    except Exception as e:
        e = Except.wrap(e)
        return send_error(query_timer, request_body, e)


KNOWN_SQL_AGGREGATES = {"sum", "count", "avg", "median", "percentile", "max", "min"}


def parse_sql(sql):
    # TODO: CONVERT tuple OF LITERALS INTO LITERAL LIST
    # # IF ALL MEMBERS OF A LIST ARE LITERALS, THEN MAKE THE LIST LITERAL
    # if all(isinstance(r, number_types) for r in output):
    #     pass
    # elif all(isinstance(r, number_types) or (is_data(r) and "literal" in r.keys()) for r in output):
    #     output = {"literal": [r['literal'] if is_data(r) else r for r in output]}
    query = to_data(moz_sql_parser.parse(sql))
    redundant_select = []
    # PULL OUT THE AGGREGATES
    for s in listwrap(query.select):
        val = s if s == "*" else s.value

        # EXTRACT KNOWN AGGREGATE FUNCTIONS
        if is_data(val):
            for a in KNOWN_SQL_AGGREGATES:
                value = val[a]
                if value != None:
                    if is_list(value):
                        # AGGREGATE WITH PARAMETERS  EG percentile(value, 0.90)
                        s.aggregate = a
                        s[a] = unwraplist(value[1::])
                        s.value = value[0]
                    else:
                        # SIMPLE AGGREGATE
                        s.aggregate = a
                        s.value = value
                    break

        # LOOK FOR GROUPBY COLUMN IN SELECT CLAUSE, REMOVE DUPLICATION
        for g in listwrap(query.groupby):
            try:
                assertAlmostEqual(g.value, val, "")
                g.name = s.name
                redundant_select.append(s)
                break
            except Exception:
                pass

    # REMOVE THE REDUNDANT select
    if is_list(query.select):
        for r in redundant_select:
            query.select.remove(r)
    elif query.select and redundant_select:
        query.select = None

    # RENAME orderby TO sort
    query.sort, query.orderby = query.orderby, None
    query.format = "table"
    return query
