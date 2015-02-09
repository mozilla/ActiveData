# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


import os
from pyLibrary.dot import set_default, wrap


DEBUG = True
convert = None
Log = None


def _late_import():
    global convert
    global Log
    from pyLibrary import convert
    from pyLibrary.debugs.logs import Log


def get(url):
    if not Log:
        _late_import()

    """
    USE son.net CONVENTIONS TO LINK TO INLINE OTHER JSON
    """
    if url.find("://") == -1:
        Log.error("{{url}} must have a prototcol (eg http://) declared", {"url": url})
    if url.startswith("file://") and url[7] != "/":
        # RELATIVE
        if os.sep == "\\":
            url = "file:///" + os.getcwd().replace(os.sep, "/") + "/" + url[7:]
        else:
            url = "file://" + os.getcwd() + "/" + url[7:]

    if url[url.find("://") + 3] != "/":
        Log.error("{{url}} must be absolute", {"url": url})
    doc = wrap({"$ref": url})

    phase1 = _replace_ref(doc, '')  # BLANK URL ONLY WORKS IF url IS ABSOLUTE
    phase2 = _replace_locals(phase1, [phase1])
    return wrap(phase2)


def expand(doc, doc_url):
    """
    ASSUMING YOU ALREADY PULED THE doc FROM doc_url, YOU CAN STILL USE THE
    EXPANDING FEATURE
    """
    if doc_url.find("://") == -1:
        Log.error("{{url}} must have a prototcol (eg http://) declared", {"url": doc_url})

    phase1 = _replace_ref(doc, doc_url)  # BLANK URL ONLY WORKS IF url IS ABSOLUTE
    phase2 = _replace_locals(phase1, [phase1])
    return wrap(phase2)


def _replace_ref(node, url):
    if url.endswith("/"):
        url = url[:-1]

    if isinstance(node, dict):
        ref, node["$ref"] = node["$ref"], None

        if not ref:
            # RECURS
            return_value = node
            candidate = {}
            for k, v in node.items():
                new_v = _replace_ref(v, url)
                candidate[k] = new_v
                if new_v is not v:
                    return_value = candidate
            return return_value

        if ref.startswith("//"):
            # SCHEME RELATIVE IMPLIES SAME PROTOCOL AS LAST TIME, WHICH
            # REQUIRES THE CURRENT DOCUMENT'S SCHEME
            ref = url.split("://")[0] + ":" + ref

        if ref.find("#") >= 0:
            # LOOKING FOR THE IN-DOCUMENT REFERENCE (EXPECTED DOT-SEPARATED
            # PATH INTO DOCUMENT)
            ref, doc_path = ref.split("#")
        else:
            doc_path = None

        # FIND THE SCHEME AND LOAD IT
        scheme_end = ref.find("://")
        if scheme_end >= -1:
            scheme_name = ref[:scheme_end]
            if scheme_name in scheme_loaders:
                new_value = scheme_loaders[scheme_name](ref, url)
            else:
                raise Log.error("unknown protocol {{scheme}}", {"scheme": scheme_name})
        else:
            # DO NOT TOUCH LOCAL REF YET
            node["$ref"] = ref
            return node

        if doc_path:
            new_value = new_value[doc_path]

        if isinstance(new_value, dict):
            return set_default({}, node, new_value)
        else:
            return wrap(new_value)

    elif isinstance(node, list):
        candidate = [_replace_ref(n, url) for n in node]
        if all(p[0] is p[1] for p in zip(candidate, node)):
            return node
        return candidate

    return node


def _replace_locals(node, doc_path):
    if isinstance(node, dict):
        ref, node["$ref"] = node["$ref"], None

        if not ref:
            # RECURS
            return_value = node
            candidate = {}
            for k, v in node.items():
                new_v = _replace_locals(v, [v] + doc_path)
                candidate[k] = new_v
                if new_v is not v:
                    return_value = candidate
            return return_value
        else:
            # REFER TO SELF
            if ref[0] == ".":
                # RELATIVE
                for i, p in enumerate(ref):
                    if p != ".":
                        new_value = doc_path[i][ref[i::]]
                        break
                else:
                    new_value = doc_path[len(ref) - 1]
            else:
                # ABSOLUTE
                new_value = doc_path[-1][ref]

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

    if ref[7] == "~":
        home_path = os.path.expanduser("~")
        if os.sep == "\\":
            home_path = home_path.replace(os.sep, "/")
        if home_path.endswith("/"):
            home_path = home_path[:-1]

        ref = "file:///" + home_path + ref[8:]
    elif ref[7] != "/":
        # CONVERT RELATIVE TO ABSOLUTE
        ref = ("/".join(url.split("/")[:-1])) + ref[6::]
    path = ref[7::] if os.sep != "\\" else ref[8::].replace("/", "\\")
    try:
        content = File(path).read()
    except Exception, e:
        Log.error("Could not read file {{filename}}", {"filename": path}, e)

    try:
        new_value = convert.json2value(content, flexible=True, paths=True)
    except Exception, e:
        try:
            new_value = convert.ini2value(content)
        except Exception, f:
            raise Log.error("Can not read {{file}}", {"file": path}, e)
    new_value = _replace_ref(new_value, ref)
    return new_value


def get_http(ref, url):
    from pyLibrary.env import http

    new_value = convert.json2value(http.get(ref), flexible=True, paths=True)
    return new_value


def get_env(ref, url):
    # GET ENVIRONMENT VARIABLES
    ref = ref[6::]
    try:
        new_value = convert.json2value(os.environ[ref])
    except Exception, e:
        new_value = os.environ[ref]
    return new_value


scheme_loaders = {
    "http": get_http,
    "file": get_file,
    "env": get_env
}
