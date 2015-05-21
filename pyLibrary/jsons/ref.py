# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from collections import Mapping

import os
from pyLibrary.dot import set_default, wrap
from pyLibrary.parsers import URL


DEBUG = False
convert = None
Log = None


def _late_import():
    global convert
    global Log
    from pyLibrary import convert
    from pyLibrary.debugs.logs import Log

    _ = convert
    _ = Log


def get(url):
    if not Log:
        _late_import()

    """
    USE json.net CONVENTIONS TO LINK TO INLINE OTHER JSON
    """
    if url.find("://") == -1:
        Log.error("{{url}} must have a prototcol (eg http://) declared",  url= url)
    if url.startswith("file://") and url[7] != "/":
        # RELATIVE
        if os.sep == "\\":
            url = "file:///" + os.getcwd().replace(os.sep, "/") + "/" + url[7:]
        else:
            url = "file://" + os.getcwd() + "/" + url[7:]

    if url[url.find("://") + 3] != "/":
        Log.error("{{url}} must be absolute",  url= url)
    doc = wrap({"$ref": url})

    phase1 = _replace_ref(doc, URL(""))  # BLANK URL ONLY WORKS IF url IS ABSOLUTE
    phase2 = _replace_locals(phase1, [phase1])
    return wrap(phase2)


def expand(doc, doc_url):
    """
    ASSUMING YOU ALREADY PULED THE doc FROM doc_url, YOU CAN STILL USE THE
    EXPANDING FEATURE
    """
    if doc_url.find("://") == -1:
        Log.error("{{url}} must have a prototcol (eg http://) declared",  url= doc_url)

    phase1 = _replace_ref(doc, URL(doc_url))  # BLANK URL ONLY WORKS IF url IS ABSOLUTE
    phase2 = _replace_locals(phase1, [phase1])
    return wrap(phase2)


def _replace_ref(node, url):
    if url.path.endswith("/"):
        url.path = url.path[:-1]

    if isinstance(node, Mapping):
        ref, raw_ref, node["$ref"] = URL(node["$ref"]), node["$ref"], None

        # RECURS
        return_value = node
        candidate = {}
        for k, v in node.items():
            new_v = _replace_ref(v, url)
            candidate[k] = new_v
            if new_v is not v:
                return_value = candidate
        if not ref:
            return return_value
        else:
            node = return_value

        if not ref.scheme and not ref.path:
            # DO NOT TOUCH LOCAL REF YET
            node["$ref"] = ref
            return node

        if not ref.scheme:
            # SCHEME RELATIVE IMPLIES SAME PROTOCOL AS LAST TIME, WHICH
            # REQUIRES THE CURRENT DOCUMENT'S SCHEME
            ref.scheme = url.scheme

        # FIND THE SCHEME AND LOAD IT
        if ref.scheme in scheme_loaders:
            new_value = scheme_loaders[ref.scheme](ref, url)
        else:
            raise Log.error("unknown protocol {{scheme}}",  scheme= ref.scheme)

        if ref.fragment:
            new_value = new_value[ref.fragment]

        if isinstance(new_value, Mapping):
            return set_default({}, node, new_value)
        elif node.keys() and new_value == None:
            return node
        else:
            return wrap(new_value)

    elif isinstance(node, list):
        candidate = [_replace_ref(n, url) for n in node]
        if all(p[0] is p[1] for p in zip(candidate, node)):
            return node
        return candidate

    return node


def _replace_locals(node, doc_path):
    if isinstance(node, Mapping):
        ref, node["$ref"] = node["$ref"], None

        # RECURS
        new_node = node
        candidate = {}
        for k, v in node.items():
            new_v = _replace_locals(v, [v] + doc_path)
            candidate[k] = new_v
            if new_v is not v:
                new_node = candidate

        if not ref:
            return new_node
        else:
            node = new_node
            # REFER TO SELF
            frag=ref.fragment
            if frag[0] == ".":
                # RELATIVE
                for i, p in enumerate(frag):
                    if p != ".":
                        if i>len(doc_path):
                            Log.error("{{frag|quote}} reaches up past the root document",  frag=frag)
                        new_value = doc_path[i-1][frag[i::]]
                        break
                else:
                    new_value = doc_path[len(frag) - 1]
            else:
                # ABSOLUTE
                new_value = doc_path[-1][frag]

        if node:
            return set_default({}, node, new_value)
        else:
            return wrap(new_value)

    elif isinstance(node, list):
        candidate = [_replace_locals(n, [n] + doc_path) for n in node]
        if all(p[0] is p[1] for p in zip(candidate, node)):
            return node
        return candidate

    return node


###############################################################################
## SCHEME LOADERS ARE BELOW THIS LINE
###############################################################################

def get_file(ref, url):
    from pyLibrary.env.files import File

    if ref.path.startswith("~"):
        home_path = os.path.expanduser("~")
        if os.sep == "\\":
            home_path = "/"+home_path.replace(os.sep, "/")
        if home_path.endswith("/"):
            home_path = home_path[:-1]

        ref.path = home_path + ref.path[1::]
    elif not ref.path.startswith("/"):
        # CONVERT RELATIVE TO ABSOLUTE
        ref.path = "/".join(url.path.split("/")[:-1]) + "/" + ref.path

    path = ref.path if os.sep != "\\" else ref.path[1::].replace("/", "\\")

    try:
        if DEBUG:
            Log.note("reading file {{path}}",  path=path)
        content = File(path).read()
    except Exception, e:
        content = None
        Log.error("Could not read file {{filename}}",  filename= path, cause=e)

    try:
        new_value = convert.json2value(content, params=ref.query, flexible=True, paths=True)
    except Exception, e:
        try:
            new_value = convert.ini2value(content)
        except Exception, f:
            raise Log.error("Can not read {{file}}",  file= path, cause=e)
    new_value = _replace_ref(new_value, ref)
    return new_value


def get_http(ref, url):
    from pyLibrary.env import http

    params = url.query
    new_value = convert.json2value(http.get(ref), params=params, flexible=True, paths=True)
    return new_value


def get_env(ref, url):
    # GET ENVIRONMENT VARIABLES
    ref = ref.host
    try:
        new_value = convert.json2value(os.environ[ref])
    except Exception, e:
        new_value = os.environ[ref]
    return new_value


def get_param(ref, url):
    # GET PARAMETERS FROM url
    param = url.query
    new_value = param[ref.host]
    return new_value


scheme_loaders = {
    "http": get_http,
    "file": get_file,
    "env": get_env,
    "param": get_param
}
