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

import sys
from _ssl import PROTOCOL_SSLv23
from collections import Mapping
from ssl import SSLContext
from tempfile import NamedTemporaryFile

import flask
from flask import Flask
from werkzeug.contrib.fixers import HeaderRewriterFix
from werkzeug.wrappers import Response

import active_data
from active_data import record_request
from active_data.actions import save_query
from active_data.actions.json import get_raw_json
from active_data.actions.query import query
from active_data.actions.save_query import SaveQueries, find_query
from active_data.actions.static import download
from pyLibrary import convert
from pyLibrary.debugs import constants, startup
from pyLibrary.debugs.logs import Log
from pyLibrary.env import elasticsearch
from pyLibrary.env.files import File
from pyLibrary.queries import containers
from pyLibrary.queries.meta import FromESMetadata
from pyLibrary.thread.threads import Thread

OVERVIEW = File("active_data/public/index.html").read()

app = Flask(__name__)
config = None

app.add_url_rule('/tools/<path:filename>', None, download)
app.add_url_rule('/find/<path:hash>', None, find_query)
app.add_url_rule('/query', None, query, defaults={'path': ''}, methods=['GET', 'POST'])
app.add_url_rule('/query/', None, query, defaults={'path': ''}, methods=['GET', 'POST'])
app.add_url_rule('/query/<path:path>', None, query, defaults={'path': ''}, methods=['GET', 'POST'])
app.add_url_rule('/json/<path:path>', None, get_raw_json, methods=['GET'])


@app.route('/', defaults={'path': ''}, methods=['GET', 'POST'])
@app.route('/<path:path>', methods=['GET', 'POST'])
def catch_all(path):
    record_request(flask.request, None, flask.request.get_data(), None)

    return Response(
        convert.unicode2utf8(OVERVIEW),
        status=400,
        headers={
            "access-control-allow-origin": "*",
            "content-type": "text/html"
        }
    )


def setup(settings=None):
    global config

    try:
        config = startup.read_settings(filename=settings)
        constants.set(config.constants)
        Log.start(config.debug)

        # PIPE REQUEST LOGS TO ES DEBUG
        if config.request_logs:
            request_logger = elasticsearch.Cluster(config.request_logs).get_or_create_index(config.request_logs)
            active_data.request_log_queue = request_logger.threaded_queue(max_size=2000)

        # SETUP DEFAULT CONTAINER, SO THERE IS SOMETHING TO QUERY
        containers.config.default = {
            "type": "elasticsearch",
            "settings": config.elasticsearch.copy()
        }

        # TURN ON /exit FOR WINDOWS DEBUGGING
        if config.flask.debug or config.flask.allow_exit:
            config.flask.allow_exit = None
            Log.warning("ActiveData is in debug mode")
            app.add_url_rule('/exit', 'exit', _exit)

        # TRIGGER FIRST INSTANCE
        FromESMetadata(config.elasticsearch)
        if config.saved_queries:
            setattr(save_query, "query_finder", SaveQueries(config.saved_queries))
        HeaderRewriterFix(app, remove_headers=['Date', 'Server'])

        setup_ssl()

        return app
    except Exception, e:
        Log.error("Serious problem with ActiveData service construction!  Shutdown!", cause=e)


def setup_ssl():
    config.flask.ssl_context = None

    if not config.flask.ssl_context:
        return

    ssl_flask = config.flask.copy()
    ssl_flask.debug = False
    ssl_flask.port = 443

    if isinstance(config.flask.ssl_context, Mapping):
        # EXPECTED PEM ENCODED FILE NAMES
        # `load_cert_chain` REQUIRES CONCATENATED LIST OF CERTS
        tempfile = NamedTemporaryFile(delete=False, suffix=".pem")
        try:
            tempfile.write(File(ssl_flask.ssl_context.certificate_file).read_bytes())
            if ssl_flask.ssl_context.certificate_chain_file:
                tempfile.write(File(ssl_flask.ssl_context.certificate_chain_file).read_bytes())
            tempfile.flush()
            tempfile.close()

            context = SSLContext(PROTOCOL_SSLv23)
            context.load_cert_chain(tempfile.name, keyfile=File(ssl_flask.ssl_context.privatekey_file).abspath)

            ssl_flask.ssl_context = context
        except Exception, e:
            Log.error("Could not handle ssl context construction", cause=e)
        finally:
            try:
                tempfile.delete()
            except Exception:
                pass

    def runner(please_stop):
        Log.warning("ActiveData listening on encrypted port {{port}}", port=ssl_flask.port)
        app.run(**ssl_flask)

    Thread.run("SSL Server", runner)

    if config.flask.ssl_context and config.flask.port != 80:
        Log.warning("ActiveData has SSL context, but is still listening on non-encrypted http port {{port}}", port=config.flask.port)

    config.flask.ssl_context = None


def _exit():
    shutdown = flask.request.environ.get('werkzeug.server.shutdown')
    if shutdown:
        shutdown()

    return Response(
        convert.unicode2utf8(OVERVIEW),
        status=400,
        headers={
            "access-control-allow-origin": "*",
            "content-type": "text/html"
        }
    )


if __name__ == "__main__":
    try:
        setup()
        app.run(**config.flask)
    finally:
        Log.stop()

    sys.exit(0)

