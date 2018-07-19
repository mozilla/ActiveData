# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import os

import flask
from flask import Flask, Response

from mo_hg.cache import Cache
from mo_json import value2json
from mo_logs import Log, constants, startup, Except
from mo_logs.strings import unicode2utf8
from pyLibrary.env.flask_wrappers import cors_wrapper

APP_NAME = "HG Relay"


class RelayApp(Flask):

    def run(self, *args, **kwargs):
        # ENSURE THE LOGGING IS CLEANED UP
        try:
            Flask.run(self, *args, **kwargs)
        except BaseException as e:  # MUST CATCH BaseException BECAUSE argparse LIKES TO EXIT THAT WAY, AND gunicorn WILL NOT REPORT
            Log.warning(APP_NAME + " service shutdown!", cause=e)
        finally:
            Log.stop()


flask_app = None
config = None
cache = None


@cors_wrapper
def relay_get(path):
    try:
        return cache.request("get", path, flask.request.headers)
    except Exception as e:
        e = Except.wrap(e)
        Log.warning("could not handle request", cause=e)
        return Response(
            unicode2utf8(value2json(e, pretty=True)),
            status=400,
            headers={
                "Content-Type": "text/html"
            }
        )


@cors_wrapper
def relay_post(path):
    try:
        return cache.request("post", path, flask.request.headers)
    except Exception as e:
        e = Except.wrap(e)
        Log.warning("could not handle request", cause=e)
        return Response(
            unicode2utf8(value2json(e, pretty=True)),
            status=400,
            headers={
                "Content-Type": "text/html"
            }
        )


def add(any_flask_app):
    global cache

    cache = Cache(config.cache)
    any_flask_app.add_url_rule(str('/<path:path>'), None, relay_get, methods=[str('GET')])
    any_flask_app.add_url_rule(str('/<path:path>'), None, relay_post, methods=[str('POST')])
    any_flask_app.add_url_rule(str('/'), None, relay_get, methods=[str('GET')])
    any_flask_app.add_url_rule(str('/'), None, relay_post, methods=[str('POST')])


if __name__ in ("__main__",):
    Log.note("Starting " + APP_NAME + " Service App...")
    flask_app = RelayApp(__name__)

    try:
        config = startup.read_settings(
            filename=os.environ.get('HG_RELAY_CONFIG')
        )
        constants.set(config.constants)
        Log.start(config.debug)

        add(flask_app)
        Log.note("Started " + APP_NAME + " Service")
    except BaseException as e:  # MUST CATCH BaseException BECAUSE argparse LIKES TO EXIT THAT WAY, AND gunicorn WILL NOT REPORT
        try:
            Log.error("Serious problem with " + APP_NAME + " service construction!  Shutdown!", cause=e)
        finally:
            Log.stop()

    if config.flask:
        if config.flask.port and config.args.process_num:
            config.flask.port += config.args.process_num
        Log.note("Running Flask...")
        flask_app.run(**config.flask)
