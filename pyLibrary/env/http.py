# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

# MIMICS THE requests API (http://docs.python-requests.org/en/latest/)
# WITH ADDED default_headers THAT CAN BE SET USING pyLibrary.debugs.settings
# EG
# {"debug.constants":{
# "pyLibrary.env.http.default_headers={
# "From":"klahnakoski@mozilla.com"
#     }
# }}


from __future__ import unicode_literals
from __future__ import division

from requests import sessions, Response
from pyLibrary.dot import Dict, nvl
from pyLibrary.env.files_string import safe_size


FILE_SIZE_LIMIT = 100 * 1024 * 1024
MIN_READ_SIZE = 8 * 1024
default_headers = Dict()  # TODO: MAKE THIS VARIABLE A SPECIAL TYPE OF EXPECTED MODULE PARAMETER SO IT COMPLAINS IF NOT SET
default_timeout = 600
_warning_sent = False


def request(method, url, **kwargs):
    if not default_headers and not _warning_sent:
        globals()["_warning_sent"] = True
        from pyLibrary.debugs.logs import Log

        Log.warning("The pyLibrary.env.http module was meant to add extra "
                    "default headers to all requests, specifically the 'From' "
                    "header with a URL to the project, or email of developer. "
                    "Use the constants.set() function to set pyLibrary.env.http.default_headers"
        )

    session = sessions.Session()
    session.headers.update(default_headers)
    return session.request(method=method, url=url, **kwargs)


def get(url, **kwargs):
    kwargs.setdefault('allow_redirects', True)
    kwargs.setdefault('timeout', default_timeout)
    kwargs["stream"] = True
    return HttpResponse(request('get', url, **kwargs))


def options(url, **kwargs):
    kwargs.setdefault('allow_redirects', True)
    kwargs.setdefault('timeout', default_timeout)
    kwargs["stream"] = True
    return HttpResponse(request('options', url, **kwargs))


def head(url, **kwargs):
    kwargs.setdefault('allow_redirects', False)
    kwargs.setdefault('timeout', default_timeout)
    kwargs["stream"] = True
    return HttpResponse(request('head', url, **kwargs))


def post(url, data=None, **kwargs):
    kwargs.setdefault('timeout', default_timeout)
    kwargs["stream"] = True
    return HttpResponse(request('post', url, data=data, **kwargs))


def put(url, data=None, **kwargs):
    kwargs.setdefault('timeout', default_timeout)
    kwargs["stream"] = True
    return HttpResponse(request('put', url, data=data, **kwargs))


def patch(url, data=None, **kwargs):
    kwargs.setdefault('timeout', default_timeout)
    kwargs["stream"] = True
    return HttpResponse(request('patch', url, data=data, **kwargs))


def delete(url, **kwargs):
    kwargs.setdefault('timeout', default_timeout)
    kwargs["stream"] = True
    return HttpResponse(request('delete', url, **kwargs))


class HttpResponse(Response):
    def __new__(cls, resp):
        resp.__class__ = HttpResponse
        return resp

    def __init__(self, resp):
        pass
        self._cached_content = None

    @property
    def all_content(self):
        # Response.content WILL LEAK MEMORY (?BECAUSE OF PYPY"S POOR HANDLING OF GENERATORS?)
        # THE TIGHT, SIMPLE, LOOP TO FILL blocks PREVENTS THAT LEAK
        if self._cached_content is None:
            def read(size=None):
                if self.raw._fp.fp is not None:
                    return self.raw.read(amt=nvl(size, MIN_READ_SIZE), decode_content=True)
                else:
                    self.close()
                    return None

            self._cached_content = safe_size(Dict(read=read))

        if hasattr(self._cached_content, "read"):
            self._cached_content.seek(0)

        return self._cached_content

