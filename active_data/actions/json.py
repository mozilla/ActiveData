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

from mo_logs import Log
from mo_logs.exceptions import Except
from active_data import cors_wrapper
from active_data.actions.jx import send_error
from mo_dots import Data, wrap
from pyLibrary import convert
from pyLibrary.queries import jx, wrap_from
from pyLibrary.queries.containers import Container
from mo_times.timer import Timer


@cors_wrapper
def get_raw_json(path):
    active_data_timer = Timer("total duration")
    body = flask.request.get_data()
    try:
        with active_data_timer:
            args = wrap(Data(**flask.request.args))
            limit = args.limit if args.limit else 10
            args.limit = None
            frum = wrap_from(path)
            result = jx.run({
                "from": path,
                "where": {"eq": args},
                "limit": limit,
                "format": "list"
            }, frum)

            if isinstance(result, Container):  #TODO: REMOVE THIS CHECK, jx SHOULD ALWAYS RETURN Containers
                result = result.format("list")

        result.meta.active_data_response_time = active_data_timer.duration

        response_data = convert.unicode2utf8(convert.value2json(result.data, pretty=True))
        Log.note("Response is {{num}} bytes", num=len(response_data))
        return Response(
            response_data,
            status=200
        )
    except Exception, e:
        e = Except.wrap(e)
        return send_error(active_data_timer, body, e)

