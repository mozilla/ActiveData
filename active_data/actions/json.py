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

from active_data.actions import find_container, send_error
from jx_base.container import Container
from jx_python import jx
from mo_dots import listwrap, unwraplist, wrap
from mo_json import value2json
from mo_logs import Except, Log
from mo_math import is_integer, is_number
from mo_threads.threads import register_thread
from mo_times.timer import Timer
from pyLibrary.env.flask_wrappers import cors_wrapper

_keep_import = value2json


@cors_wrapper
@register_thread
def get_raw_json(path):
    active_data_timer = Timer("total duration")
    body = flask.request.get_data()
    try:
        with active_data_timer:
            args = scrub_args(flask.request.args)
            limit = args.limit if args.limit else 10
            args.limit = None

            frum = find_container(path, after=None)
            result = jx.run({
                "from": path,
                "where": {"eq": args},
                "limit": limit,
                "format": "list"
            }, frum)

            if isinstance(result, Container):  # TODO: REMOVE THIS CHECK, jx SHOULD ALWAYS RETURN Containers
                result = result.format("list")

        result.meta.active_data_response_time = active_data_timer.duration

        response_data = value2json(result.data, pretty=True).encode('utf8')
        Log.note("Response is {{num}} bytes", num=len(response_data))
        return Response(
            response_data,
            status=200
        )
    except Exception as e:
        e = Except.wrap(e)
        return send_error(active_data_timer, body, e)


def scrub_args(args):
    output = {}
    for k, v in list(unwrap(args).items()):
        vs = []
        for v in listwrap(v):
            if is_integer(v):
                vs.append(int(v))
            elif is_number(v):
                vs.append(float(v))
            else:
                vs.append(v)
        output[k] = unwraplist(vs)
    return wrap(output)
