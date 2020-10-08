# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from mo_dots import Data, Null, coalesce, is_data, is_list, to_data, is_many, unwraplist
from mo_future import PY2, is_text, text, unichr, urlparse, is_binary
from mo_logs import Log


class URL(object):
    """
    JUST LIKE urllib.parse() [1], BUT CAN HANDLE JSON query PARAMETERS

    [1] https://docs.python.org/3/library/urllib.parse.html
    """

    def __new__(cls, value, *args, **kwargs):
        if isinstance(value, URL):
            return value
        else:
            return object.__new__(cls)

    def __init__(self, value, port=None, path=None, query=None, fragment=None):
        if isinstance(value, URL):
            return
        try:
            self.scheme = None
            self.host = None
            self.port = port
            self.path = path
            self.query = query
            self.fragment = fragment

            if value == None:
                return

            if value.startswith("file://") or value.startswith("//"):
                # urlparse DOES NOT WORK IN THESE CASES
                scheme, suffix = value.split("//", 2)
                self.scheme = scheme.rstrip(":")
                parse(self, suffix, 0, 1)
                self.query = to_data(url_param2value(self.query))
            else:
                output = urlparse(value)
                self.scheme = output.scheme
                self.port = coalesce(port, output.port)
                self.host = output.netloc.split(":")[0]
                self.path = coalesce(path, output.path)
                self.query = coalesce(query, to_data(url_param2value(output.query)))
                self.fragment = coalesce(fragment, output.fragment)
        except Exception as e:
            Log.error(u"problem parsing {{value}} to URL", value=value, cause=e)

    def __nonzero__(self):
        if (
            self.scheme
            or self.host
            or self.port
            or self.path
            or self.query
            or self.fragment
        ):
            return True
        return False

    def __bool__(self):
        if (
            self.scheme
            or self.host
            or self.port
            or self.path
            or self.query
            or self.fragment
        ):
            return True
        return False

    def __truediv__(self, other):
        if not is_text(other):
            Log.error(u"Expecting text path")
        output = self.__copy__()
        output.path = output.path.rstrip("/") + "/" + other.lstrip("/")
        return output

    def __add__(self, other):
        if not is_data(other):
            Log.error("can only add data for query parameters")
        output = self.__copy__()
        output.query += other
        return output

    def __unicode__(self):
        return self.__str__().decode("utf8")  # ASSUME chr<128 ARE VALID UNICODE

    def __copy__(self):
        output = URL(None)
        output.scheme = self.scheme
        output.host = self.host
        output.port = self.port
        output.path = self.path
        output.query = self.query
        output.fragment = self.fragment
        return output

    def decode(self, encoding=""):
        return text(self).decode(encoding)

    def __data__(self):
        return str(self)

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return text(self) == text(other)

    def __str__(self):
        url = ""
        if self.host:
            url = self.host
        if self.scheme:
            url = self.scheme + "://" + url
        if self.port:
            url = url + ":" + str(self.port)
        if self.path:
            if self.path[0] == text("/"):
                url += str(self.path)
            else:
                url += "/" + str(self.path)
        if self.query:
            url = url + "?" + value2url_param(self.query)
        if self.fragment:
            url = url + "#" + value2url_param(self.fragment)
        return url


def int2hex(value, size):
    return (("0" * size) + hex(value)[2:])[-size:]


def hex2chr(hex):
    try:
        return unichr(int(hex, 16))
    except Exception as e:
        raise e


if PY2:
    _map2url = {chr(i): chr(i) for i in range(32, 128)}
    for c in "{}<>;/?:@&=+$%,+":
        _map2url[c] = "%" + str(int2hex(ord(c), 2))
    for i in range(128, 256):
        _map2url[chr(i)] = "%" + str(int2hex(i, 2))
    _map2url[chr(32)] = "+"
else:
    _map2url = {i: unichr(i) for i in range(32, 128)}
    for c in b"{}<>;/?:@&=+$%,+":
        _map2url[c] = "%" + int2hex(c, 2)
    for i in range(128, 256):
        _map2url[i] = "%" + str(int2hex(i, 2))
    _map2url[32] = "+"


names = ["path", "query", "fragment"]
indicator = ["/", "?", "#"]


def parse(output, suffix, curr, next):
    if next == len(indicator):
        output.__setattr__(names[curr], suffix)
        return

    e = suffix.find(indicator[next])
    if e == -1:
        parse(output, suffix, curr, next + 1)
    else:
        output.__setattr__(names[curr], suffix[:e:])
        parse(output, suffix[e + 1 : :], next, next + 1)


def hex2byte(v):
    return bytes([int(v, 16)])


def url_param2value(param):
    """
    CONVERT URL QUERY PARAMETERS INTO DICT
    """
    if param == None:
        return Null
    if param == None:
        return Null

    def _decode(vs):
        from mo_json import json2value

        results = []
        for v in vs.split(","):
            output = []
            i = 0
            # WE MUST TRACK THE STATE OF UTF* DECODING, IF ILLEGITIMATE ENCODING
            # THEN ASSUME LATIN1
            utf_remaining = 0
            start = 0
            while i < len(v):
                c = v[i]
                if utf_remaining:
                    if c == "%":
                        try:
                            hex = v[i + 1 : i + 3]
                            if hex.strip() == hex:
                                d = int(v[i + 1 : i + 3], 16)
                                if d & 0xC0 == 0x80:  # 10XX XXXX
                                    utf_remaining -= 1
                                    b = bytes([d])
                                    output.append(b)
                                    i += 3
                                    continue
                        except Exception:
                            pass
                    # missing continuation byte (# 10XX XXXX), try again
                    output = output[:-utf_remaining]
                    utf_remaining = 0
                    i = start
                    output.append(b"%")
                    i += 1
                else:
                    if c == "+":
                        output.append(b" ")
                        i += 1
                    elif c == "%":
                        try:
                            hex_pair = v[i + 1 : i + 3]
                            if hex_pair.strip() != hex_pair:
                                output.append(b"%")
                                i += 1
                                continue

                            d = int(hex_pair, 16)
                            if d & 0x80:
                                p = bin(d)[2:].find("0")
                                if p <= 1:
                                    output.append(b"%")
                                    i += 1
                                else:
                                    utf_remaining = p - 1
                                    start = i
                                    b = bytes([d])
                                    output.append(b)
                                    i += 3
                            else:
                                b = bytes([d])
                                output.append(b)
                                i += 3
                        except Exception:
                            output.append(b"%")
                            i += 1
                    else:
                        try:
                            output.append(c.encode("latin1"))
                        except Exception:
                            # WE EXPECT BYTES, BUT SOMEONE WILL GIVE US UNICODE STRINGS
                            output.append(c.encode("utf8"))
                        i += 1

            if utf_remaining:
                # missing continuation byte, try again
                output = output[:-utf_remaining] + [v[start:].encode("latin1")]

            output = b"".join(output).decode("utf8")
            try:
                output = json2value(output)
            except Exception:
                pass
            results.append(output)
        return unwraplist(results)

    query = Data()
    for p in param.split("&"):
        if not p:
            continue
        if p.find("=") == -1:
            k = p
            v = True
        else:
            k, v = p.split("=")
            k = _decode(k)
            v = _decode(v)

        u = query.get(k)
        if u is None:
            query[k] = v
        elif is_list(u):
            u += [v]
        else:
            query[k] = [u, v]

    return query


def value2url_param(value):
    """
    :param value:
    :return: ascii URL
    """
    from mo_json import value2json, json2value

    def _encode(value):
        return "".join(_map2url[c] for c in value.encode("utf8"))

    if value == None:
        return None

    if is_data(value):
        value_ = to_data(value)
        output = "&".join(
            kk + "=" + vv
            for k, v in sorted(value_.leaves(), key=lambda p: p[0])
            for kk, vv in [(value2url_param(k), value2url_param(v))]
            if vv or vv == 0
        )
    elif is_text(value):
        try:
            json2value(value)
            output = _encode(value2json(value))
        except Exception:
            output = _encode(value)
    elif is_binary(value):
        output = "".join(_map2url[c] for c in value)
    elif is_many(value):
        output = ",".join(
            vv for v in value for vv in [value2url_param(v)] if vv or vv == 0
        )
    else:
        output = _encode(value2json(value))
    return output
