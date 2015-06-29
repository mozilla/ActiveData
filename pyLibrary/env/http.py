# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

# MIMICS THE requests API (http://docs.python-requests.org/en/latest/)
# DEMANDS data IS A JSON-SERIALIZABLE STRUCTURE
# WITH ADDED default_headers THAT CAN BE SET USING pyLibrary.debugs.settings
# EG
# {"debug.constants":{
# "pyLibrary.env.http.default_headers={
# "From":"klahnakoski@mozilla.com"
#     }
# }}


from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from copy import copy

from requests import sessions, Response

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict, coalesce
from pyLibrary.env.big_data import safe_size, CompressedLines, ZipfileLines


FILE_SIZE_LIMIT = 100 * 1024 * 1024
MIN_READ_SIZE = 8 * 1024
default_headers = Dict()  # TODO: MAKE THIS VARIABLE A SPECIAL TYPE OF EXPECTED MODULE PARAMETER SO IT COMPLAINS IF NOT SET
default_timeout = 600

_warning_sent = False


def request(method, url, **kwargs):
    """
     JUST LIKE requests.request() BUT WITH DEFAULT HEADERS AND FIXES
     DEMANDS data IS ONE OF:
      * A JSON-SERIALIZABLE STRUCTURE, OR
      * LIST OF JSON-SERIALIZABLE STRUCTURES, OR
      * None

     THE BYTE_STRINGS (b"") ARE NECESSARY TO PREVENT httplib.py FROM **FREAKING OUT**
     IT APPEARS requests AND httplib.py SIMPLY CONCATENATE STRINGS BLINDLY, WHICH
     INCLUDES url AND headers
    """
    global _warning_sent
    if not default_headers and not _warning_sent:
        _warning_sent = True
        Log.warning("The pyLibrary.env.http module was meant to add extra "
                    "default headers to all requests, specifically the 'From' "
                    "header with a URL to the project, or email of developer. "
                    "Use the constants.set() function to set pyLibrary.env.http.default_headers"
        )

    session = sessions.Session()
    session.headers.update(default_headers)

    if isinstance(url, unicode):
        # httplib.py WILL **FREAK OUT** IF IT SEES ANY UNICODE
        url = url.encode("ascii")

    # if "data" not in kwargs:
    #     pass
    # elif kwargs["data"] == None:
    #     pass
    # elif isinstance(kwargs["data"], basestring):
    #     Log.error("Expecting `data` to be a structure")
    # elif isinstance(kwargs["data"], list):
    #     #CR-DELIMITED JSON IS ALSO ACCEPTABLE
    #     kwargs["data"] = b"\n".join(convert.unicode2utf8(convert.value2json(d)) for d in kwargs["data"])
    # else:
    #     kwargs["data"] = convert.unicode2utf8(convert.value2json(kwargs["data"]))

    _to_ascii_dict(kwargs)
    timeout = kwargs[b'timeout'] = coalesce(kwargs.get(b'timeout'), default_timeout)

    try:
        if len(coalesce(kwargs.get(b"data"))) > 1000:
            compressed = convert.bytes2zip(kwargs[b"data"])
            if b"headers" not in kwargs:
                kwargs[b"headers"] = {}
            kwargs[b"headers"][b'content-encoding'] = b'gzip'
            kwargs[b"data"] = compressed

            _to_ascii_dict(kwargs[b"headers"])
            return session.request(method=method, url=url, **kwargs)
        else:
            _to_ascii_dict(kwargs.get(b"headers"))
            return session.request(method=method, url=url, **kwargs)
    except Exception, e:
        if " Read timed out." in e:
            Log.error("Timeout failure (timeout was {{timeout}}", timeout=timeout, cause=e)
        else:
            Log.error("Request failure", e)


def _to_ascii_dict(headers):
    if headers is None:
        return
    for k, v in copy(headers).items():
        if isinstance(k, unicode):
            del headers[k]
            if isinstance(v, unicode):
                headers[k.encode("ascii")] = v.encode("ascii")
            else:
                headers[k.encode("ascii")] = v
        elif isinstance(v, unicode):
            headers[k] = v.encode("ascii")


def get(url, **kwargs):
    kwargs.setdefault(b'allow_redirects', True)
    kwargs[b"stream"] = True
    return HttpResponse(request(b'get', url, **kwargs))


def options(url, **kwargs):
    kwargs.setdefault(b'allow_redirects', True)
    kwargs[b"stream"] = True
    return HttpResponse(request(b'options', url, **kwargs))


def head(url, **kwargs):
    kwargs.setdefault(b'allow_redirects', False)
    kwargs[b"stream"] = True
    return HttpResponse(request(b'head', url, **kwargs))


def post(url, **kwargs):
    kwargs[b"stream"] = True
    return HttpResponse(request(b'post', url, **kwargs))


def put(url, **kwargs):
    return HttpResponse(request(b'put', url, **kwargs))


def patch(url, **kwargs):
    kwargs[b"stream"] = True
    return HttpResponse(request(b'patch', url, **kwargs))


def delete(url, **kwargs):
    kwargs[b"stream"] = True
    return HttpResponse(request(b'delete', url, **kwargs))


class HttpResponse(Response):
    def __new__(cls, resp):
        resp.__class__ = HttpResponse
        return resp

    def __init__(self, resp):
        pass
        self._cached_content = None

    @property
    def all_content(self):
        # response.content WILL LEAK MEMORY (?BECAUSE OF PYPY"S POOR HANDLING OF GENERATORS?)
        # THE TIGHT, SIMPLE, LOOP TO FILL blocks PREVENTS THAT LEAK
        if self._content is not False:
            self._cached_content = self._content
        elif self._cached_content is None:
            def read(size):
                if self.raw._fp.fp is not None:
                    return self.raw.read(amt=size, decode_content=True)
                else:
                    self.close()
                    return None

            self._cached_content = safe_size(Dict(read=read))

        if hasattr(self._cached_content, "read"):
            self._cached_content.seek(0)

        return self._cached_content

    @property
    def all_lines(self):
        try:
            content = self.raw.read(decode_content=False)
            if self.headers.get('content-encoding') == 'gzip':
                return CompressedLines(content)
            elif self.headers.get('content-type') == 'application/zip':
                return ZipfileLines(content)
            else:
                return convert.utf82unicode(content).split("\n")
        except Exception, e:
            Log.error("Not JSON", e)
        finally:
            self.close()
