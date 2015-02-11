from urlparse import urlparse, parse_qs
from pyLibrary.dot import Null, nvl
from pyLibrary.dot.dicts import Dict


convert = None
Log = None


def _late_import():
    global convert
    global Log
    from pyLibrary import convert
    from pyLibrary.debugs.logs import Log


names = ["path", "query", "fragment"]
indicator = ["/", "?", "#"]


def parse(output, suffix, curr, next):
    if next == len(indicator):
        output[names[curr]] = suffix
        return

    e = suffix.find(indicator[next])
    if e == -1:
        parse(output, suffix, curr, next + 1)
    else:
        output[names[curr]] = suffix[:e:]
        parse(output, suffix[e + 1::], next, next + 1)


def URL(value):
    if value == None:
        return Null

    if not convert:
        _late_import()
    if value.startswith("file://") or value.startswith("//"):
        # urlparse DOES NOT WORK IN THESE CASES
        scheme, suffix = value.split("//")
        output = Dict(
            scheme=scheme.rstrip(":")
        )
        parse(output, suffix, 0, 1)
    else:
        output = urlparse(value)

    query = parse_qs(nvl(output.query, ""))
    for k, v in query.copy().items():
        if not isinstance(v, list):
            Log.error("not expected fom rthe parse_qs() function")
        v = [_decode(vv) for vv in v]
        if len(v) == 1:
            v = v[0]
        query[k] = v

    return Dict(
        scheme=output.scheme,
        host=output.netloc,
        port=output.port,
        path=output.path,
        query=query,
        fragmen=output.fragment
    )


def _decode(v):
    if isinstance(v, basestring):
        try:
            return convert.json2value(v)
        except Exception:
            pass
    return v
