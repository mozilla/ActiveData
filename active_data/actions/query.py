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
from active_data import record_request
from active_data.actions import (
    QUERY_TOO_LARGE,
    find_container,
    save_query,
    send_error,
    test_mode_wait,
)
from jx_base.container import Container
from jx_python import jx
from mo_dots import Data, Null, is_data
from mo_files import File
from mo_future import binary_type, text
from mo_json import json2value, value2json
from mo_logs import Except, Log
from mo_threads import Queue, Signal
from mo_threads.threads import register_thread, MAIN_THREAD, Thread
from mo_times.timer import Timer
from pyLibrary.env.flask_wrappers import cors_wrapper

DEBUG = False
BLANK = File("active_data/public/error.html").read().encode("utf8")
QUERY_SIZE_LIMIT = 10 * 1024 * 1024
NUM_THREADS = 30

todo = Queue("todo queries")


@cors_wrapper
@register_thread
def jx_query(path):
    try:
        with Timer("total duration", verbose=DEBUG) as total_timer:
            preamble_timer = Timer("preamble", silent=True)
            with preamble_timer:
                if flask.request.headers.get("content-length", "") in ["", "0"]:
                    # ASSUME A BROWSER HIT THIS POINT, SEND text/html RESPONSE BACK
                    return Response(
                        BLANK, status=400, headers={"Content-Type": "text/html"}
                    )
                elif int(flask.request.headers["content-length"]) > QUERY_SIZE_LIMIT:
                    Log.error(QUERY_TOO_LARGE)

                request_body = flask.request.get_data().strip()
                text = request_body.decode("utf8")
                query = json2value(text)
                record_request(flask.request, query, None, None)

            result = []
            if query.tuple != None:
                execute_tuple_op(query, result, Null)
                result = Data(data=result[0])
            else:
                execute(query, result, Null)
                result = result[0]

            result.meta.timing.preamble = mo_math.round(
                preamble_timer.duration.seconds, digits=4
            )
            result.meta.timing.total = "{{TOTAL_TIME}}"  # TIMING PLACEHOLDER

            with Timer("jsonification", verbose=DEBUG) as json_timer:
                response_data = value2json(result).encode("utf8")

        with Timer("post timer", verbose=DEBUG):
            # IMPORTANT: WE WANT TO TIME OF THE JSON SERIALIZATION, AND HAVE IT IN THE JSON ITSELF.
            # WE CHEAT BY DOING A (HOPEFULLY FAST) STRING REPLACEMENT AT THE VERY END
            timing_replacement = (
                b'"total":'
                + binary_type(mo_math.round(total_timer.duration.seconds, digits=4))
                + b', "jsonification":'
                + binary_type(mo_math.round(json_timer.duration.seconds, digits=4))
            )
            response_data = response_data.replace(
                b'"total":"{{TOTAL_TIME}}"', timing_replacement
            )
            Log.note(
                "Response is {{num}} bytes in {{duration}}",
                num=len(response_data),
                duration=total_timer.duration,
            )

            return Response(
                response_data,
                status=200,
                headers={"Content-Type": result.meta.content_type},
            )
    except Exception as e:
        e = Except.wrap(e)
        return send_error(total_timer, request_body, e)


def execute(query, output, is_done):
    try:
        if query["tuple"] != None:
            return execute_tuple_op(query, output, is_done)

        if query.meta.testing:
            test_mode_wait(query, MAIN_THREAD.please_stop)
        find_table_timer = Timer("find container", verbose=DEBUG)
        with find_table_timer:
            frum = find_container(query["from"], after=None)

        translate_timer = Timer("translate", verbose=DEBUG)
        with translate_timer:
            query_result = jx.run(query, container=frum)

            # TODO: REMOVE THIS CHECK, jx SHOULD ALWAYS RETURN Containers
            if isinstance(query_result, Container):
                result = query_result.format(query.format)
            else:
                result = query_result
            output.append(result)

        save_timer = Timer("save", verbose=DEBUG)
        with save_timer:
            if query.meta.save:
                try:
                    result.meta.saved_as = save_query.query_finder.save(query)
                except Exception as cause:
                    Log.warning("Unexpected save problem", cause=cause)

        result.meta.timing.find_table = mo_math.round(
            find_table_timer.duration.seconds, digits=4
        )
        result.meta.timing.translate = mo_math.round(
            translate_timer.duration.seconds, digits=4
        )
        result.meta.timing.save = mo_math.round(save_timer.duration.seconds, digits=4)
    except Exception as cause:
        Log.error("could not execute expression {{expression}}", expression=query, cause=cause)
    finally:
        is_done.go()


def execute_tuple_op(query, output, is_done):
    try:
        items = query.tuple
        if items == None:
            output.append([])
            return
        if is_data(items):
            if not items:
                output.append([])
                return
            items = [items]
        work = [
            (q, [], Signal())
            for q in items
        ]
        todo.extend(work)
        # IF THERE IS MORE WORK, MIGHT AS WELL DO IT WHILE WE WAIT
        more = todo.pop_one()
        if more:
            execute(*more)
        for _, _, d in work:
            d.wait()
        output.append(tuple(r[0] for _, r, _ in work))
    except Exception as cause:
        Log.warning("Problem with tuple op", cause=cause)
    finally:
        is_done.go()


def worker(please_stop):
    while not please_stop:
        work = todo.pop(till=please_stop)
        execute(*work)


threads = [
    Thread.run("query thread " + text(i), worker)
    for i in range(NUM_THREADS)
]
