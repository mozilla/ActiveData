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

import os

import flask
from flask import Flask, Response
from mo_dots import listwrap, coalesce
from mo_files import File
from mo_json import value2json, json2value
from mo_logs import Log
from mo_logs import constants, startup
from mo_logs.strings import utf82unicode, unicode2utf8

from pyLibrary.env.flask_wrappers import gzip_wrapper
from tuid.service import TUIDService

OVERVIEW = File("tuid/public/index.html").read_bytes()


app = Flask(__name__)


class TUIDApp(Flask):

    def run(self, *args, **kwargs):
        # ENSURE THE LOGGING IS CLEANED UP
        try:
            Flask.run(self, *args, **kwargs)
        except BaseException as e:  # MUST CATCH BaseException BECAUSE argparse LIKES TO EXIT THAT WAY, AND gunicorn WILL NOT REPORT
            Log.warning("Serious problem with TUID service construction!  Shutdown!", cause=e)
        finally:
            Log.stop()


flask_app = TUIDApp(__name__)
config = None
service = None


# @gzip_wrapper
def query_to_service_call(path):
    try:
        request_body = flask.request.get_data().strip()
        query = json2value(utf82unicode(request_body))

        # ENSURE THE QUERY HAS THE CORRECT FORM
        if query['from'] != 'files':
            Log.error("Can only handle queries on the `files` table")

        ands = listwrap(query.where['and'])
        if len(ands) != 2:
            Log.error(
                'expecting a simple where clause with following structure\n{{example|json}}',
                example={"and": [
                    {"eq": {"revision": "<REVISION>"}},
                    {"in": {"path": ["<path1>", "<path2>", "...", "<pathN>"]}}
                ]}
            )

        rev = None
        paths = None
        for a in query.where['and']:
            rev = coalesce(rev, a.eq.revision)
            paths = listwrap(coalesce(paths, a['in'].path, a.eq.path))

        # RETURN TUIDS
        response = service.get_tuids_from_files(paths, rev)
        return Response(
            _stream_table(response),
            status=200,
            headers={
                "Content-Type": "application/json"
            }
        )
    except Exception as e:
        Log.warning("could not handle request", cause=e)
        return Response(
            unicode2utf8(value2json(e, pretty=True)),
            status=400,
            headers={
                "Content-Type": "text/html"
            }
        )


def _stream_table(files):
    yield b'{"format":"table", "header":["path", "tuids"], "data":['
    for f in files:
        yield value2json(f).encode('utf8')
    yield b']}'



flask_app.add_url_rule(str('/query'), None, query_to_service_call, defaults={'path': ''}, methods=[str('GET'), str('POST')])
flask_app.add_url_rule(str('/query/'), None, query_to_service_call, defaults={'path': ''}, methods=[str('GET'), str('POST')])


@flask_app.route(str('/'), defaults={'path': ''}, methods=[str('OPTIONS'), str('HEAD')])
@flask_app.route(str('/<path:path>'), methods=[str('OPTIONS'), str('HEAD')])
def _head(path):
    return Response(b'', status=200)

@flask_app.route(str('/'), defaults={'path': ''}, methods=[str('GET'), str('POST')])
@flask_app.route(str('/<path:path>'), methods=[str('GET'), str('POST')])
def _default(path):
    return Response(
        OVERVIEW,
        status=200,
        headers={
            "Content-Type": "text/html"
        }
    )


if __name__ in ("__main__",):
    try:
        config = startup.read_settings(
            env_filename=os.environ.get('TUID_CONFIG')
        )

        constants.set(config.constants)
        Log.start(config.debug)
        service = TUIDService(config.tuid)
    except BaseException as e:  # MUST CATCH BaseException BECAUSE argparse LIKES TO EXIT THAT WAY, AND gunicorn WILL NOT REPORT
        try:
            Log.error("Serious problem with TUID service construction!  Shutdown!", cause=e)
        finally:
            Log.stop()

    if config.flask:
        if config.flask.port and config.args.process_num:
            config.flask.port += config.args.process_num

        flask_app.run(**config.flask)


