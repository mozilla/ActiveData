# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

import os
from ssl import SSLContext

import flask
from _ssl import PROTOCOL_SSLv23
from flask import Flask, Response

import active_data
from active_data import OVERVIEW, record_request
from active_data.actions import save_query
from active_data.actions.contribute import send_contribute
from active_data.actions.json import get_raw_json
from active_data.actions.query import jx_query
from active_data.actions.save_query import SaveQueries, find_query
from active_data.actions.sql import sql_query
from active_data.actions.static import download, send_favicon
from jx_base import container
from jx_elasticsearch.es52 import agg_bulk, QueryStats
from jx_elasticsearch import elasticsearch
from mo_dots import is_data
from mo_files import File, TempFile
from mo_future import text
from mo_logs import Log, constants, machine_metadata, startup
from mo_threads import Thread, stop_main_thread
from mo_threads.threads import MAIN_THREAD, register_thread
from pyLibrary.env import  http
from pyLibrary.env.flask_wrappers import cors_wrapper, dockerflow, add_version


class ActiveDataApp(Flask):

    def run(self, *args, **kwargs):
        # ENSURE THE LOGGING IS CLEANED UP
        try:
            Flask.run(self, *args, **kwargs)
        except BaseException as e:  # MUST CATCH BaseException BECAUSE argparse LIKES TO EXIT THAT WAY, AND gunicorn WILL NOT REPORT
            if e.args and not e.args[0]:
                pass  # ASSUME NORMAL EXIT
            else:
                Log.warning("Serious problem with ActiveData service construction!  Shutdown!", cause=e)
        finally:
            Log.stop()
            stop_main_thread()

    def process_response(self, response):
        del response.headers['Date']
        del response.headers['Server']
        return response


flask_app = ActiveDataApp(__name__)

config = None


@flask_app.route('/', defaults={'path': ''}, methods=['OPTIONS', 'HEAD'])
@flask_app.route('/<path:path>', methods=['OPTIONS', 'HEAD'])
@cors_wrapper
def _head(path):
    return Response(b'', status=200)

flask_app.add_url_rule('/tools/<path:filename>', None, download)
flask_app.add_url_rule('/favicon.ico', None, send_favicon)
flask_app.add_url_rule('/contribute.json', None, send_contribute)
flask_app.add_url_rule('/find/<path:hash>', None, find_query)
flask_app.add_url_rule('/query', None, jx_query, defaults={'path': ''}, methods=['GET', 'POST'])
flask_app.add_url_rule('/query/', None, jx_query, defaults={'path': ''}, methods=['GET', 'POST'])
flask_app.add_url_rule('/query/<path:path>', None, jx_query, defaults={'path': ''}, methods=['GET', 'POST'])
flask_app.add_url_rule('/sql', None, sql_query, defaults={'path': ''}, methods=['GET', 'POST'])
flask_app.add_url_rule('/sql/', None, sql_query, defaults={'path': ''}, methods=['GET', 'POST'])
flask_app.add_url_rule('/json/<path:path>', None, get_raw_json, methods=['GET'])


@flask_app.route('/', defaults={'path': ''}, methods=['GET', 'POST'])
@flask_app.route('/<path:path>', methods=['GET', 'POST'])
@cors_wrapper
@register_thread
def _default(path):
    record_request(flask.request, None, flask.request.get_data(), None)

    return Response(
        OVERVIEW.encode('utf8'),
        status=200,
        headers={
            "Content-Type": "text/html"
        }
    )


def setup():
    global config

    config = startup.read_settings(
        default_filename=os.environ.get('ACTIVEDATA_CONFIG'),
        defs=[
            {
                "name": ["--process_num", "--process"],
                "help": "Additional port offset (for multiple Flask processes",
                "type": int,
                "dest": "process_num",
                "default": 0,
                "required": False
            }
        ]
    )

    constants.set(config.constants)
    Log.start(config.debug)

    agg_bulk.S3_CONFIG = config.bulk.s3

    File.new_instance("activedata.pid").write(text(machine_metadata.pid))

    # PIPE REQUEST LOGS TO ES DEBUG
    if config.request_logs:
        cluster = elasticsearch.Cluster(config.request_logs)
        request_logger = cluster.get_or_create_index(config.request_logs)
        active_data.request_log_queue = request_logger.threaded_queue(max_size=2000, period=1)

    if config.dockerflow:
        def backend_check():
            http.get_json(config.elasticsearch.host + ":" + text(config.elasticsearch.port))
        dockerflow(flask_app, backend_check)
    else:
        # IF NOT USING DOCKERFLOW, THEN RESPOND WITH A SIMPLER __version__
        add_version(flask_app)

    # SETUP DEFAULT CONTAINER, SO THERE IS SOMETHING TO QUERY
    container.config.default = {
        "type": "elasticsearch",
        "settings": config.elasticsearch.copy()
    }

    # TRIGGER FIRST INSTANCE
    if config.saved_queries:
        setattr(save_query, "query_finder", SaveQueries(config.saved_queries))

    # STARTUP QUERY STATS
    QueryStats(elasticsearch.Cluster(config.elasticsearch))

    if config.flask.port and config.args.process_num:
        config.flask.port += config.args.process_num

    # TURN ON /exit FOR WINDOWS DEBUGGING
    if config.flask.debug or config.flask.allow_exit:
        config.flask.allow_exit = None
        Log.warning("ActiveData is in debug mode")
        flask_app.add_url_rule('/exit', 'exit', _exit)

    if config.flask.ssl_context:
        if config.args.process_num:
            Log.error("can not serve ssl and multiple Flask instances at once")
        setup_flask_ssl()

    # ENSURE MAIN THREAD SHUTDOWN TRIGGERS Flask SHUTDOWN
    MAIN_THREAD.stopped.then(exit)


def setup_flask_ssl():
    config.flask.ssl_context = None

    if not config.flask.ssl_context:
        return

    ssl_flask = config.flask.copy()
    ssl_flask.debug = False
    ssl_flask.port = 443

    if is_data(config.flask.ssl_context):
        # EXPECTED PEM ENCODED FILE NAMES
        # `load_cert_chain` REQUIRES CONCATENATED LIST OF CERTS
        with TempFile() as tempfile:
            try:
                tempfile.write(File(ssl_flask.ssl_context.certificate_file).read_bytes())
                if ssl_flask.ssl_context.certificate_chain_file:
                    tempfile.write(File(ssl_flask.ssl_context.certificate_chain_file).read_bytes())
                tempfile.flush()
                tempfile.close()

                context = SSLContext(PROTOCOL_SSLv23)
                context.load_cert_chain(tempfile.name, keyfile=File(ssl_flask.ssl_context.privatekey_file).abspath)

                ssl_flask.ssl_context = context
            except Exception as e:
                Log.error("Could not handle ssl context construction", cause=e)

    def runner(please_stop):
        Log.warning("ActiveData listening on encrypted port {{port}}", port=ssl_flask.port)
        flask_app.run(**ssl_flask)

    Thread.run("SSL Server", runner)

    if config.flask.ssl_context and config.flask.port != 80:
        Log.warning("ActiveData has SSL context, but is still listening on non-encrypted http port {{port}}", port=config.flask.port)

    config.flask.ssl_context = None


@register_thread
def _exit():
    Log.note("Got request to shutdown")
    try:
        return Response(
            OVERVIEW.encode('utf8'),
            status=400,
            headers={
                "Content-Type": "text/html"
            }
        )
    finally:
        shutdown = flask.request.environ.get('werkzeug.server.shutdown')
        if shutdown:
            shutdown()
        else:
            Log.warning("werkzeug.server.shutdown does not exist")


if __name__ in ("__main__", "active_data.app"):
    try:
        setup()
    except BaseException as e:  # MUST CATCH BaseException BECAUSE argparse LIKES TO EXIT THAT WAY, AND gunicorn WILL NOT REPORT
        Log.error("Serious problem with ActiveData service construction!  Shutdown!", cause=e)
        stop_main_thread()

    if config.flask:
        flask_app.run(**config.flask)

