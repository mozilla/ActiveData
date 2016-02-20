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

import flask
from flask import Flask
from werkzeug.contrib.fixers import HeaderRewriterFix
from werkzeug.wrappers import Response

from active_data import record_request
from active_data.actions import save_query
from active_data.actions.query import query
from active_data.actions.save_query import SaveQueries, find_query
from pyLibrary import convert
from pyLibrary.debugs import constants, startup
from pyLibrary.debugs.logs import Log
from pyLibrary.env import elasticsearch
from pyLibrary.env.files import File
from pyLibrary.queries import containers
from pyLibrary.queries.meta import FromESMetadata

OVERVIEW = File("active_data/public/index.html").read()

app = Flask(__name__)
config = None


app.add_url_rule('/find/<path:hash>', 'find_query', find_query)
app.add_url_rule('/query/<path:hash>', 'query', query, defaults={'path': ''}, methods=['GET', 'POST'])


@app.route('/', defaults={'path': ''}, methods=['GET', 'POST'])
@app.route('/<path:path>', methods=['GET', 'POST'])
def overview(path):
    try:
        record_request(flask.request, None, flask.request.data, None)
    except Exception, e:
        Log.warning("Can not record", cause=e)

    return Response(
        convert.unicode2utf8(OVERVIEW),
        status=400,
        headers={
            "access-control-allow-origin": "*",
            "content-type": "text/html"
        }
    )


def setup(settings=None):
    global request_log_queue
    global config

    try:
        config = startup.read_settings(filename=settings)
        constants.set(config.constants)
        Log.start(config.debug)

        # PIPE REQUEST LOGS TO ES DEBUG
        if config.request_logs:
            request_logger = elasticsearch.Cluster(config.request_logs).get_or_create_index(config.request_logs)
            request_log_queue = request_logger.threaded_queue(max_size=2000)

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

        # SHUTDOWN LOGGING WHEN DONE
        app.do_teardown_appcontext(_teardown)
        return app
    except Exception, e:
        Log.error("Serious problem with ActiveData service construction!  Shutdown!", cause=e)


def main():
    global config

    setup()
    app.run(**config.flask)
    sys.exit(0)


def _teardown():
    print "stopping"
    Log.stop()


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
    main()

