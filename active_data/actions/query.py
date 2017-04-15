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
from flask import Response

from mo_logs import Log, strings
from mo_logs.exceptions import Except
from mo_logs.profiles import CProfiler
from mo_logs.strings import expand_template
from active_data import record_request, cors_wrapper
from active_data.actions import save_query
from mo_dots import coalesce, join_field, split_field, wrap, listwrap
from pyLibrary import convert
from mo_files import File
from mo_math import Math
from pyLibrary.queries import jx, meta, wrap_from
from pyLibrary.queries.containers import Container, STRUCT
from pyLibrary.queries.meta import TOO_OLD
from mo_testing.fuzzytestcase import assertAlmostEqual
from mo_threads import Till
from mo_times.dates import Date
from mo_times.durations import MINUTE
from mo_times.timer import Timer

BLANK = convert.unicode2utf8(File("active_data/public/error.html").read())
QUERY_SIZE_LIMIT = 10*1024*1024


@cors_wrapper
def query(path):
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
                    text = replace_vars(text, flask.request.args)
                    data = convert.json2value(text)
                    record_request(flask.request, data, None, None)
                    if data.meta.testing:
                        _test_mode_wait(data)

                translate_timer = Timer("translate")
                with translate_timer:
                    if data.sql:
                        data = parse_sql(data.sql)
                    frum = wrap_from(data['from'])
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
            return _send_error(query_timer, request_body, e)


def _test_mode_wait(query):
    """
    WAIT FOR METADATA TO ARRIVE ON INDEX
    :param query: dict() OF REQUEST BODY
    :return: nothing
    """
    try:
        m = meta.singlton
        now = Date.now()
        end_time = now + MINUTE

        # MARK COLUMNS DIRTY
        m.meta.columns.update({
            "clear": [
                "partitions",
                "count",
                "cardinality",
                "last_updated"
            ],
            "where": {"eq": {"es_index": join_field(split_field(query["from"])[0:1])}}
        })

        # BE SURE THEY ARE ON THE todo QUEUE FOR RE-EVALUATION
        cols = [c for c in m.get_columns(table_name=query["from"], force=True) if c.type not in STRUCT]
        for c in cols:
            Log.note("Mark {{column}} dirty at {{time}}", column=c.names["."], time=now)
            c.last_updated = now - TOO_OLD
            m.todo.push(c)

        while end_time > now:
            # GET FRESH VERSIONS
            cols = [c for c in m.get_columns(table_name=query["from"]) if c.type not in STRUCT]
            for c in cols:
                if not c.last_updated or c.cardinality == None :
                    Log.note(
                        "wait for column (table={{col.es_index}}, name={{col.es_column}}) metadata to arrive",
                        col=c
                    )
                    break
            else:
                break
            Till(seconds=1).wait()
        for c in cols:
            Log.note(
                "fresh column name={{column.name}} updated={{column.last_updated|date}} parts={{column.partitions}}",
                column=c
            )
    except Exception, e:
        Log.warning("could not pickup columns", cause=e)


def _send_error(active_data_timer, body, e):
    record_request(flask.request, None, body, e)
    Log.warning("Could not process\n{{body}}", body=body.decode("latin1"), cause=e)
    e = e.__data__()
    e.meta.timing.total = active_data_timer.duration.seconds

    # REMOVE TRACES, BECAUSE NICER TO HUMANS
    # def remove_trace(e):
    #     e.trace = e.trace[0:1:]
    #     for c in listwrap(e.cause):
    #         remove_trace(c)
    # remove_trace(e)

    return Response(
        convert.unicode2utf8(convert.value2json(e)),
        status=400
    )


def replace_vars(text, params=None):
    """
    REPLACE {{vars}} WITH ENVIRONMENTAL VALUES
    """
    start = 0
    var = strings.between(text, "{{", "}}", start)
    while var:
        replace = "{{" + var + "}}"
        index = text.find(replace, 0)
        if index==-1:
            Log.error("could not find {{var}} (including quotes)", var=replace)
        end = index + len(replace)

        try:
            replacement = unicode(Date(var).unix)
            text = text[:index] + replacement + text[end:]
            start = index + len(replacement)
        except Exception, _:
            start += 1

        var = strings.between(text, "{{", "}}", start)

    text = expand_template(text, coalesce(params, {}))
    return text


KNOWN_SQL_AGGREGATES = {"count", "sum"}


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
            except Exception, e:
                pass

        if isinstance(val, Mapping):
            for a in KNOWN_SQL_AGGREGATES:
                if val[a]:
                    s.aggregate = a
                    s.value = val[a]
    query.select = [s for s in listwrap(query.select) if s.value != None]
    query.format = "table"
    return query
