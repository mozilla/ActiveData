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
from flask import Response

from pyLibrary import convert, strings
from pyLibrary.debugs.exceptions import Except
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce
from pyLibrary.queries import jx, meta
from pyLibrary.queries.containers import Container
from pyLibrary.queries.meta import TOO_OLD
from pyLibrary.strings import expand_template
from pyLibrary.thread.threads import Thread
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import MINUTE
from pyLibrary.times.timer import Timer

from active_data.actions import save_query



def query(path):
    cprofiler = None

    if Log.cprofiler:
        import cProfile
        Log.note("starting cprofile for query")

        cprofiler = cProfile.Profile()
        cprofiler.enable()

    active_data_timer = Timer("total duration")
    body = flask.request.data
    try:
        with active_data_timer:
            if not body.strip():
                return Response(
                    convert.unicode2utf8(BLANK),
                    status=400,
                    headers={
                        "access-control-allow-origin": "*",
                        "content-type": "text/html"
                    }
                )

            text = convert.utf82unicode(body)
            text = replace_vars(text, flask.request.args)
            data = convert.json2value(text)
            record_request(flask.request, data, None, None)
            if data.meta.testing:
                _test_mode_wait(data)

            if Log.profiler or Log.cprofiler:
                # flask.run() DOES NOT HAVE PROFILING ON
                # THREAD CREATION IS DONE TO CAPTURE THE PROFILING DATA
                def run(please_stop):
                    return jx.run(data)
                thread = Thread.run("run query", run)
                result = thread.join()
            else:
                result = jx.run(data)

            if isinstance(result, Container):  #TODO: REMOVE THIS CHECK, jx SHOULD ALWAYS RETURN Containers
                result = result.format(data.format)

            if data.meta.save:
                result.meta.saved_as = save_query.query_finder.save(data)

        result.meta.timing.total = active_data_timer.duration

        response_data = convert.unicode2utf8(convert.value2json(result))
        Log.note("Response is {{num}} bytes", num=len(response_data))
        return Response(
            response_data,
            direct_passthrough=True,  # FOR STREAMING
            status=200,
            headers={
                "access-control-allow-origin": "*",
                "content-type": result.meta.content_type
            }
        )
    except Exception, e:
        e = Except.wrap(e)
        return _send_error(active_data_timer, body, e)



def _test_mode_wait(query):
    """
    WAIT FOR METADATA TO ARRIVE ON INDEX
    :param query: dict() OF REQUEST BODY
    :return: nothing
    """

    m = meta.singlton
    now = Date.now()
    end_time = now + MINUTE

    # MARK COLUMNS DIRTY
    with m.columns.locker:
        m.columns.update({
            "clear": [
                "partitions",
                "count",
                "cardinality",
                "last_updated"
            ],
            "where": {"eq": {"table": query["from"]}}
        })

    # BE SURE THEY ARE ON THE todo QUEUE FOR RE-EVALUATION
    cols = [c for c in m.get_columns(table=query["from"]) if c.type not in ["nested", "object"]]
    for c in cols:
        Log.note("Mark {{column}} dirty at {{time}}", column=c.name, time=now)
        c.last_updated = now - TOO_OLD
        m.todo.push(c)

    while end_time > now:
        # GET FRESH VERSIONS
        cols = [c for c in m.get_columns(table=query["from"]) if c.type not in ["nested", "object"]]
        for c in cols:
            if not c.last_updated or c.cardinality == None :
                Log.note(
                    "wait for column (table={{col.table}}, name={{col.name}}) metadata to arrive",
                    col=c
                )
                break
        else:
            break
        Thread.sleep(seconds=1)
    for c in cols:
        Log.note(
            "fresh column name={{column.name}} updated={{column.last_updated|date}} parts={{column.partitions}}",
            column=c
        )


def _send_error(active_data_timer, body, e):
    record_request(flask.request, None, body, e)
    Log.warning("Could not process\n{{body}}", body=body, cause=e)
    e = e.as_dict()
    e.meta.active_data_response_time = active_data_timer.duration
    return Response(
        convert.unicode2utf8(convert.value2json(e)),
        status=400,
        headers={
            "access-control-allow-origin": "*",
            "content-type": "application/json"
        }
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
