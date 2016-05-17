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

from active_data.actions.query import _send_error
from pyLibrary import convert
from pyLibrary.debugs.exceptions import Except
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict
from pyLibrary.dot import wrap
from pyLibrary.queries import jx
from pyLibrary.queries.containers import Container
from pyLibrary.times.timer import Timer


def get_raw_json(path):
    active_data_timer = Timer("total duration")
    body = flask.request.get_data()
    try:
        with active_data_timer:
            args = wrap(Dict(**flask.request.args))
            limit = args.limit if args.limit else 10
            args.limit = None
            result = jx.run({
                "from": path,
                "where": {"eq": args},
                "limit": limit,
                "format": "list"
            })

            if isinstance(result, Container):  #TODO: REMOVE THIS CHECK, jx SHOULD ALWAYS RETURN Containers
                result = result.format("list")

        result.meta.active_data_response_time = active_data_timer.duration

        response_data = convert.unicode2utf8(convert.value2json(result.data, pretty=True))
        Log.note("Response is {{num}} bytes", num=len(response_data))
        return Response(
            response_data,
            status=200,
            headers={
                "access-control-allow-origin": "*",
                "content-type": "text/plain"
            }
        )
    except Exception, e:
        e = Except.wrap(e)
        return _send_error(active_data_timer, body, e)

