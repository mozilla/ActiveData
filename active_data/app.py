# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import argparse
import codecs
import logging
from logging.handlers import RotatingFileHandler
import os
import random
from flask import Flask, json
import flask
import requests
import time
from werkzeug.contrib.fixers import HeaderRewriterFix
from werkzeug.exceptions import abort
import sys  # REQUIRED FOR DYNAMIC DEBUG

app = Flask(__name__)


def stream(raw_response):
    while True:
        block = raw_response.read(amt=65536, decode_content=False)
        if not block:
            return
        yield block


def listwrap(value):
    if value is None:
        return []
    elif isinstance(value, list):
        return value
    else:
        return [value]


class Except(Exception):
    def __init__(self, message):
        super(Exception, self).__init__(self, message)
        self._message = message

    @property
    def message(self):
        return self._message


@app.route('/', defaults={'path': ''}, methods=['HEAD'])
@app.route('/<path:path>', methods=['HEAD'])
def catch_all_head(path):
    return catch_all(path, "HEAD")

@app.route('/', defaults={'path': ''}, methods=['GET'])
@app.route('/<path:path>', methods=['GET'])
def catch_all_get(path):
    return catch_all(path, "get")


@app.route('/', defaults={'path': ''}, methods=['POST'])
@app.route('/<path:path>', methods=['POST'])
def catch_all_post(path):
    return catch_all(path, 'post')


def catch_all(path, type):
    try:
        data = flask.request.environ['body_copy']
        filter(type, path, data)

        #PICK RANDOM ES
        es = random.choice(listwrap(settings["elasticsearch"]))

        ## SEND REQUEST
        headers = {k: v for k, v in flask.request.headers if v is not None and v != "" and v != "null"}
        headers['content-type'] = 'application/json'

        response = requests.request(
            type,
            es["host"] + ":" + str(es["port"]) + "/" + path,
            data=data,
            stream=True,  # FOR STREAMING
            headers=headers,
            timeout=90
        )

        # ALLOW CROSS DOMAIN (BECAUSE ES IS USUALLY NOT ON SAME SERVER AS PAGE)
        outbound_header = dict(response.headers)
        outbound_header["access-control-allow-origin"] = "*"

        # LOG REQUEST TO ES
        request = flask.request
        uid = int(round(time.time() * 1000.0))
        slim_request = {
            "remote_addr": request.remote_addr,
            "method": request.method,
            "path": request.path,
            "request_length": len(data),
            "response_length": int(outbound_header["content-length"]) if "content-length" in outbound_header else None
        }
        try:
            requests.request(
                type,
                es["host"] + ":" + str(es["port"]) + "/debug/esfrontline/"+str(uid),
                data=json.dumps(slim_request),
                timeout=5
            )
        except Exception, e:
            pass

        logger.debug("path: {path}, request bytes={request_content_length}, response bytes={response_content_length}".format(
            path=path,
            # request_headers=dict(response.headers),
            request_content_length=len(data),
            # response_headers=outbound_header,
            response_content_length=int(outbound_header["content-length"]) if "content-length" in outbound_header else None
        ))

        ## FORWARD RESPONSE
        return flask.wrappers.Response(
            stream(response.raw),
            direct_passthrough=True, #FOR STREAMING
            status=response.status_code,
            headers=outbound_header
        )
    except Except, e:
        logger.warning(e.message)
        abort(400)
    except Exception, e:
        logger.exception(str(e))
        abort(400)


def filter(type, path_string, query):
    """
    THROW EXCEPTION IF THIS IS NOT AN ElasticSearch QUERY
    """
    try:
        if type.upper() == "HEAD":
            if path_string in ["", "/"]:
                return  # HEAD REQUESTS ARE ALLOWED
            else:
                raise Except("HEAD requests are generally not allowed")

        path = path_string.split("/")

        ## EXPECTING {index_name} "/" {type_name} "/" {_id}
        ## EXPECTING {index_name} "/" {type_name} "/_search"
        ## EXPECTING {index_name} "/_search"
        if len(path) == 2:
            if path[-1] not in ["_mapping", "_search"]:
                raise Except("request path must end with _mapping or _search")
        elif len(path) == 3:
            if path[-1] not in ["_mapping", "_search"]:
                raise Except("request path must end with _mapping or _search")
        else:
            raise Except('request must be of form: {index_name} "/" {type_name} "/_search" ')

        ## COMPARE TO WHITE LIST
        if path[0] not in settings["whitelist"]:
            raise Except('index not in whitelist: {index_name}'.format({"index_name": path[0]}))


        ## EXPECTING THE QUERY TO AT LEAST HAVE .query ATTRIBUTE
        if path[-1] == "_search" and json.loads(query).get("query", None) is None:
            raise Except("_search must have query")

        ## NO CONTENT ALLOWED WHEN ASKING FOR MAPPING
        if path[-1] == "_mapping" and len(query) > 0:
            raise Except("Can not provide content when requesting _mapping")

    except Exception, e:
        logger.warning(e.message)
        raise Except("Not allowed: {path}:\n{query}".format(path=path_string, query=query))


# Snagged from http://stackoverflow.com/questions/10999990/python-flask-how-to-get-whole-raw-post-body
# I SUSPECT THIS IS PREVENTING STREAMING
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

logger = None
settings = {}


def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument(*["--settings", "--settings-file", "--settings_file"], **{
            "help": "path to JSON file with settings",
            "type": str,
            "dest": "filename",
            "default": "./settings.json",
            "required": False
        })
        namespace = parser.parse_args()
        args = {k: getattr(namespace, k) for k in vars(namespace)}

        if not os.path.exists(args["filename"]):
            raise Except("Can not file settings file {filename}".format(filename=args["filename"]))

        with codecs.open(args["filename"], "r", encoding="utf-8") as file:
            json_data = file.read()
        globals()["settings"] = json.loads(json_data)
        settings["args"] = args
        settings["whitelist"] = listwrap(settings.get("whitelist", None))

        globals()["logger"] = logging.getLogger('esFrontLine')
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        for d in listwrap(settings["debug"]["log"]):
            if d.get("filename", None):
                fh = RotatingFileHandler(**d)
                fh.setLevel(logging.DEBUG)
                fh.setFormatter(formatter)
                logger.addHandler(fh)
            elif d.get("stream", None) in ("sys.stdout", "sys.stderr"):
                ch = logging.StreamHandler(stream=eval(d["stream"]))
                ch.setLevel(logging.DEBUG)
                ch.setFormatter(formatter)
                logger.addHandler(ch)

        HeaderRewriterFix(app, remove_headers=['Date', 'Server'])
        app.run(**settings["flask"])
    except Exception, e:
        print(str(e))


if __name__ == '__main__':
    main()
