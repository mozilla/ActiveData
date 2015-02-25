# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from flask import Flask
import flask
from werkzeug.contrib.fixers import HeaderRewriterFix
from werkzeug.wrappers import Response

from pyLibrary import convert, strings, queries
from pyLibrary.debugs import constants, startup
from pyLibrary.debugs.logs import Log, Except
from pyLibrary.dot import Dict, unwrap
from pyLibrary.env import elasticsearch
from pyLibrary.env.files import File
from pyLibrary.queries import qb
from pyLibrary.times.dates import Date
from pyLibrary.times.timer import Timer


OVERVIEW = File("active_data/ActiveData.html").read()
BLANK = File("active_data/BlankQueryResponse.html").read()

app = Flask(__name__)
request_log_queue = None
default_elasticsearch = None


def record_request(request, query_, data, error):
    log = Dict(
        http_user_agent=request.headers.get("user_agent"),
        http_accept_encoding=request.headers.get("accept_encoding"),
        path=request.headers.environ["werkzeug.request"].full_path,
        content_length=request.headers.get("content_length"),
        remote_addr=request.remote_addr,
        query=query_,
        data=data,
        error=error
    )
    log["from"] = request.headers.get("from")
    request_log_queue.add({"value": log})


@app.route('/query', defaults={'path': ''}, methods=['GET', 'POST'])
def query(path):
    total_duration = Timer("total duration")
    try:
        with total_duration:
            body = flask.request.environ['body_copy']
            if not body.strip():
                return Response(
                    convert.unicode2utf8(BLANK),
                    status=400,
                    headers={
                        "access-control-allow-origin": "*",
                        "Content-type": "text/html"
                    }
                )



            text = convert.utf82unicode(body)
            text = replace_vars(text)
            data = convert.json2value(text)
            record_request(flask.request, data, None, None)
            result = qb.run(data)

        result.meta.active_data_response_time = total_duration.duration.seconds

        return Response(
            convert.unicode2utf8(convert.value2json(result)),
            direct_passthrough=True,  # FOR STREAMING
            status=200,
            headers={
                "access-control-allow-origin": "*",
                "Content-type": result.meta.content_type
            }
        )
    except Exception, e:
        e = Except.wrap(e)

        record_request(flask.request, None, flask.request.environ['body_copy'], e)
        Log.warning("problem", e)
        e = e.as_dict()
        e.meta.active_data_response_time = total_duration.duration.seconds

        return Response(
            convert.unicode2utf8(convert.value2json(e)),
            status=400,
            headers={
                "access-control-allow-origin": "*",
                "Content-type": "application/json"
            }
        )


@app.route('/', defaults={'path': ''}, methods=['GET', 'POST'])
@app.route('/<path:path>')
def overview(path):
    record_request(flask.request, None, flask.request.environ['body_copy'], None)

    return Response(
        convert.unicode2utf8(OVERVIEW),
        status=400,
        headers={
            "access-control-allow-origin": "*",
            "Content-type": "text/html"
        }
    )

def replace_vars(text):
    """
    REPLACE {{vars}} WITH ENVIRONMENTAL VALUES
    """
    var = strings.between(text, "\"{{", "}}\"")
    while var:
        text = text.replace("\"{{"+var+"}}\"", unicode(Date(var).unix))
        var = strings.between(text, "\"{{", "}}\"")
    return text



# Snagged from http://stackoverflow.com/questions/10999990/python-flask-how-to-get-whole-raw-post-body
class WSGICopyBody(object):
    def __init__(self, application):
        self.application = application

    def __call__(self, environ, start_response):
        from cStringIO import StringIO

        length = environ.get('CONTENT_LENGTH', '0')
        length = 0 if length == '' else int(length)

        body = environ['wsgi.input'].read(length)
        environ['body_copy'] = body
        environ['wsgi.input'] = StringIO(body)

        # Call the wrapped application
        app_iter = self.application(environ, self._sr_callback(start_response))

        # Return modified response
        return app_iter

    def _sr_callback(self, start_response):
        def callback(status, headers, exc_info=None):
            # Call upstream start_response
            start_response(status, headers, exc_info)

        return callback


app.wsgi_app = WSGICopyBody(app.wsgi_app)


def main():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        # PIPE REQUEST LOGS TO ES DEBUG
        request_logger = elasticsearch.Cluster(settings.request_logs).get_or_create_index(settings.request_logs)
        globals()["default_elasticsearch"] = elasticsearch.Index(settings.elasticsearch)
        globals()["request_log_queue"] = request_logger.threaded_queue(max_size=2000)

        queries.config.default = {
            "type": "elasticsearch",
            "settings": settings.elasticsearch.copy()
        }

        HeaderRewriterFix(app, remove_headers=['Date', 'Server'])
        app.run(**unwrap(settings.flask))
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()

