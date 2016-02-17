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

from active_data.actions import save_query
from active_data.actions.save_query import SaveQueries, find_query
from active_data.actions.static import download
from pyLibrary import convert, strings
from pyLibrary.debugs import constants, startup
from pyLibrary.debugs.logs import Log, Except
from pyLibrary.dot import wrap, coalesce, Dict
from pyLibrary.env import elasticsearch
from pyLibrary.env.files import File
from pyLibrary.queries import containers
from pyLibrary.queries import qb, meta
from pyLibrary.queries.containers import Container
from pyLibrary.queries.meta import FromESMetadata, TOO_OLD
from pyLibrary.strings import expand_template
from pyLibrary.thread.threads import Thread
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import MINUTE
from pyLibrary.times.timer import Timer

OVERVIEW = File("active_data/html/index.html").read()
BLANK = File("active_data/html/error.html").read()

app = Flask(__name__)
request_log_queue = None
config = None


app.add_url_rule('/tools/<path:filename>', 'download', download)
app.add_url_rule('/find/<path:hash>', 'find_query', find_query)


@app.route('/query', defaults={'path': ''}, methods=['GET', 'POST'])
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
                # MARK ALL QUERIES FOR TESTING SO FULL METADATA IS AVAILABLE BEFORE QUERY EXECUTION
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
                        "where": {"eq": {"table": data["from"]}}
                    })

                # BE SURE THEY ARE ON THE todo QUEUE FOR RE-EVALUATION
                cols = [c for c in m.get_columns(table=data["from"]) if c.type not in ["nested", "object"]]
                for c in cols:
                    Log.note("Mark {{column}} dirty at {{time}}", column=c.name, time=now)
                    c.last_updated = now - TOO_OLD
                    m.todo.push(c)

                while end_time > now:
                    # GET FRESH VERSIONS
                    cols = [c for c in m.get_columns(table=data["from"]) if c.type not in ["nested", "object"]]
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



            if Log.profiler or Log.cprofiler:
                # THREAD CREATION IS DONE TO CAPTURE THE PROFILING DATA
                def run(please_stop):
                    return qb.run(data)
                thread = Thread.run("run query", run)
                result = thread.join()
            else:
                result = qb.run(data)

            if isinstance(result, Container):  #TODO: REMOVE THIS CHECK, qb SHOULD ALWAYS RETURN Containers
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
        return send_error(active_data_timer, body, e)


@app.route('/json/<path:path>', methods=['GET'])
def get_raw_json(path):
    active_data_timer = Timer("total duration")
    body = flask.request.data
    try:
        with active_data_timer:
            args = wrap(Dict(**flask.request.args))
            limit = args.limit if args.limit else 10
            args.limit = None
            result = qb.run({
                "from": path,
                "where": {"eq": args},
                "limit": limit,
                "format": "list"
            })

            if isinstance(result, Container):  #TODO: REMOVE THIS CHECK, qb SHOULD ALWAYS RETURN Containers
                result = result.format("list")

        result.meta.active_data_response_time = active_data_timer.duration

        response_data = convert.unicode2utf8(convert.value2json(result.data, pretty=True))
        Log.note("Response is {{num}} bytes", num=len(response_data))
        return Response(
            response_data,
            direct_passthrough=True,  # FOR STREAMING
            status=200,
            headers={
                "access-control-allow-origin": "*",
                "content-type": "text/plain"
            }
        )
    except Exception, e:
        e = Except.wrap(e)
        return send_error(active_data_timer, body, e)


def send_error(active_data_timer, body, e):
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


def record_request(request, query_, data, error):
    if request_log_queue == None:
        return

    log = wrap({
        "timestamp": Date.now(),
        "http_user_agent": request.headers.get("user_agent"),
        "http_accept_encoding": request.headers.get("accept_encoding"),
        "path": request.headers.environ["werkzeug.request"].full_path,
        "content_length": request.headers.get("content_length"),
        "remote_addr": request.remote_addr,
        "query": query_,
        "data": data,
        "error": error
    })
    log["from"] = request.headers.get("from")
    request_log_queue.add({"value": log})


def main():
    # global default_elasticsearch
    global request_log_queue
    global config

    try:
        config = startup.read_settings()
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
            app.add_url_rule('/exit', 'exit', exit)



        # TRIGGER FIRST INSTANCE
        FromESMetadata(config.elasticsearch)
        if config.saved_queries:
            setattr(save_query, "query_finder", SaveQueries(config.saved_queries))
        HeaderRewriterFix(app, remove_headers=['Date', 'Server'])

        if config.flask.ssl_context:
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

        if config.flask.ssl_context:
            Log.warning("ActiveData has SSL context, but is still listening on non-encrypted http port {{port}}", port=config.flask.port)
        config.flask.ssl_context = None
        app.run(**config.flask)
    except Exception, e:
        Log.error("Serious problem with ActiveData service!  Shutdown completed!", cause=e)
    finally:
        Log.stop()

    sys.exit(0)


def exit():
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

