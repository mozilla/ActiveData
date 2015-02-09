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
from werkzeug.exceptions import abort
from werkzeug.wrappers import Response

from pyLibrary import convert
from pyLibrary.debugs import constants, startup
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict, wrap, unwrap
from pyLibrary.env import elasticsearch
from pyLibrary.queries import Q


app = Flask(__name__)
request_log_queue = None
default_elasticsearch = None


def record_request(request):
    log = Dict(
        http_user_agent=request.headers.get("user_agent"),
        http_accept_encoding=request.headers.get("accept_encoding"),
        path=request.headers.environ["werkzeug.request"].full_path,
        content_length=request.headers.get("content_length"),
        remote_addr=request.remote_addr
    )
    log["from"] = request.headers.get("from")
    request_log_queue.add(log)



def pre_filter_request(path, type):
    pass
    # if not flask.request.headers.get("from").strip():
    #     # RETURN A REQUEST FOR A FROM HEADER
    #     message = "Please add a 'From' header containing either an email address " \
    #               "of a person to contact, or a website describing the application. " \
    #               "In the event your application causes high load we would like to " \
    #               "contact you to explore ways to reducing it.  A simple ban is " \
    #               "inconvenient for both of us and does not solve the root cause."
    #     abort(401, message=message)

from2context = Dict()


@app.route('/query', defaults={'path': ''}, methods=['GET'])
@app.route('/<path:path>', methods=['GET'])
def query(path):
    try:
        record_request(flask.request)
        data = convert.json2value(convert.utf82unicode(flask.request.environ['body_copy']))
        result = Q.run(data)

        outbound_header = wrap({
            "access-control-allow-origin": "*"
        })

        return Response(
            convert.unicode2utf8(convert.value2json(result)),
            direct_passthrough=True, #FOR STREAMING
            status=200,
            headers=unwrap(outbound_header)
        )
    except Exception, e:
        Log.warning("problem", e)
        return Response(
            convert.unicode2utf8(convert.value2json(e)),
            status=400
        )


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

#
# @app.errorhandler(400)
# def handle_invalid_usage(error):
#     response = {
#         "status_code": error.code,
#         'message': convert.unicode2utf8(error.description.__json__())
#     }
#     return response
#
#
#
# class SimpleExcept(APIException):
#     status_code = 400
#
#
#     detail = 'Service temporarily unavailable, try again later.'
#
#





def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        constants.set(settings.constants)

        # PIPE REQUEST LOGS TO ES DEBUG
        request_logger = elasticsearch.Cluster(settings.request_logs).get_or_create_index(settings.request_logs)
        globals()["default_elasticsearch"] = elasticsearch.Index(settings.elasticsearch)
        globals()["request_log_queue"] = request_logger.threaded_queue(size=2000)

        HeaderRewriterFix(app, remove_headers=['Date', 'Server'])
        app.run(**unwrap(settings.flask))
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()

