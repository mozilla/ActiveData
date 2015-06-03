# encoding: utf-8
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
from __future__ import absolute_import

import HTMLParser
import StringIO
import base64
import cgi
from collections import Mapping
import datetime
from decimal import Decimal
import gzip
import hashlib
from io import BytesIO
import json
import re
from tempfile import TemporaryFile

from pyLibrary import strings
from pyLibrary.dot import wrap, wrap_dot, unwrap
from pyLibrary.collections.multiset import Multiset
from pyLibrary.debugs.logs import Log, Except
from pyLibrary.env.big_data import FileString, safe_size
from pyLibrary.jsons import quote
from pyLibrary.jsons.encoder import json_encoder
from pyLibrary.strings import expand_template
from pyLibrary.times.dates import Date


"""
DUE TO MY POOR MEMORY, THIS IS A LIST OF ALL CONVERSION ROUTINES
"""
def value2json(obj, pretty=False):
    try:
        json = json_encoder(obj, pretty=pretty)
        if json == None:
            Log.note(str(type(obj)) + " is not valid{{type}}JSON",  type= " (pretty) " if pretty else " ")
            Log.error("Not valid JSON: " + str(obj) + " of type " + str(type(obj)))
        return json
    except Exception, e:
        Log.error("Can not encode into JSON: {{value}}",  value= repr(obj), cause=e)


def remove_line_comment(line):
    mode = 0  # 0=code, 1=inside_string, 2=escaping
    for i, c in enumerate(line):
        if c == '"':
            if mode == 0:
                mode = 1
            elif mode == 1:
                mode = 0
            else:
                mode = 1
        elif c == '\\':
            if mode == 0:
                mode = 0
            elif mode == 1:
                mode = 2
            else:
                mode = 1
        elif mode == 2:
            mode = 1
        elif c == "#" and mode == 0:
            return line[0:i]
        elif c == "/" and mode == 0 and line[i + 1] == "/":
            return line[0:i]
    return line



def json2value(json_string, params={}, flexible=False, paths=False):
    """
    :param json_string: THE JSON
    :param params: STANDARD JSON PARAMS
    :param flexible: REMOVE COMMENTS
    :param paths: ASSUME JSON KEYS ARE DOT-DELIMITED
    :return: Python value
    """
    if isinstance(json_string, str):
        Log.error("only unicode json accepted")

    try:
        if flexible:
            # REMOVE """COMMENTS""", # COMMENTS, //COMMENTS, AND \n \r
            # DERIVED FROM https://github.com/jeads/datasource/blob/master/datasource/bases/BaseHub.py# L58
            json_string = re.sub(r"\"\"\".*?\"\"\"", r"\n", json_string, flags=re.MULTILINE)
            json_string = "\n".join(remove_line_comment(l) for l in json_string.split("\n"))
            # ALLOW DICTIONARY'S NAME:VALUE LIST TO END WITH COMMA
            json_string = re.sub(r",\s*\}", r"}", json_string)
            # ALLOW LISTS TO END WITH COMMA
            json_string = re.sub(r",\s*\]", r"]", json_string)

        if params:
            json_string = expand_template(json_string, params)


        # LOOKUP REFERENCES
        value = wrap(json_decoder(json_string))

        if paths:
            value = wrap_dot(value)

        return value

    except Exception, e:
        e = Except.wrap(e)
        if "Expecting '" in e and "' delimiter: line" in e:
            line_index = int(strings.between(e.message, " line ", " column ")) - 1
            column = int(strings.between(e.message, " column ", " ")) - 1
            line = json_string.split("\n")[line_index]
            if column > 20:
                sample = "..." + line[column - 20:]
                pointer = "   " + (" " * 20) + "^"
            else:
                sample = line
                pointer = (" " * column) + "^"

            if len(sample) > 43:
                sample = sample[:43] + "..."

            Log.error("Can not decode JSON at:\n\t" + sample + "\n\t" + pointer + "\n")

        if len(json_string)>1000:
            json_string = json_string[0:50] + " ... <snip " + strings.comma(len(json_string)) + " characters> ... " + json_string[len(json_string)-50:len(json_string)]
        base_str = unicode2utf8(json_string)
        hexx_str = bytes2hex(base_str, " ")
        char_str = " " + ("  ".join((latin12unicode(c) if ord(c) >= 32 else ".") for c in base_str))
        Log.error("Can not decode JSON:\n" + char_str + "\n" + hexx_str + "\n", e)


def string2datetime(value, format=None):
    return Date(value, format).value


def str2datetime(value, format=None):
    return string2datetime(value, format)


def datetime2string(value, format="%Y-%m-%d %H:%M:%S"):
    return Date(value).format(format=format)


def datetime2str(value, format="%Y-%m-%d %H:%M:%S"):
    return Date(value).format(format=format)


def datetime2unix(d):
    try:
        if d == None:
            return None
        elif isinstance(d, datetime.datetime):
            epoch = datetime.datetime(1970, 1, 1)
        elif isinstance(d, datetime.date):
            epoch = datetime.date(1970, 1, 1)
        else:
            Log.error("Can not convert {{value}} of type {{type}}",  value= d,  type= d.__class__)

        diff = d - epoch
        return Decimal(long(diff.total_seconds() * 1000000)) / 1000000
    except Exception, e:
        Log.error("Can not convert {{value}}",  value= d, cause=e)


def datetime2milli(d):
    return datetime2unix(d) * 1000


def timedelta2milli(v):
    return v.total_seconds()


def unix2datetime(u):
    try:
        if u == None:
            return None
        if u == 9999999999: # PYPY BUG https://bugs.pypy.org/issue1697
            return datetime.datetime(2286, 11, 20, 17, 46, 39)
        return datetime.datetime.utcfromtimestamp(u)
    except Exception, e:
        Log.error("Can not convert {{value}} to datetime",  value= u, cause=e)


def milli2datetime(u):
    if u == None:
        return None
    return unix2datetime(u / 1000.0)


def dict2Multiset(dic):
    if dic == None:
        return None

    output = Multiset()
    output.dic = unwrap(dic).copy()
    return output


def multiset2dict(value):
    """
    CONVERT MULTISET TO dict THAT MAPS KEYS TO MAPS KEYS TO KEY-COUNT
    """
    if value == None:
        return None
    return dict(value.dic)


def table2list(
    column_names, # tuple of columns names
    rows          # list of tuples
):
    return wrap([dict(zip(column_names, r)) for r in rows])

def table2tab(
    column_names, # tuple of columns names
    rows          # list of tuples
):
    def row(r):
        return "\t".join(map(value2json, r))

    return row(column_names)+"\n"+("\n".join(row(r) for r in rows))



def list2tab(rows):
    columns = set()
    for r in wrap(rows):
        columns |= set(k for k, v in r.leaves())
    keys = list(columns)

    output = []
    for r in wrap(rows):
        output.append("\t".join(value2json(r[k]) for k in keys))

    return "\t".join(keys) + "\n" + "\n".join(output)


def list2table(rows):
    columns = set()
    for r in rows:
        columns |= set(r.keys())
    keys = list(columns)

    output = []
    for r in rows:
        output.append([r[k] for k in keys])

    return wrap({
        "header": keys,
        "data": output
    })


def value2string(value):
    # PROPER NULL HANDLING
    if value == None:
        return None
    return unicode(value)


def value2quote(value):
    # RETURN PRETTY PYTHON CODE FOR THE SAME
    if isinstance(value, basestring):
        return string2quote(value)
    else:
        return repr(value)


def string2quote(value):
    if value == None:
        return "None"
    return quote(value)


string2regexp = re.escape


def string2url(value):
    if isinstance(value, unicode):
        return "".join([_map2url[c] for c in unicode2latin1(value)])
    elif isinstance(value, str):
        return "".join([_map2url[c] for c in value])
    else:
        Log.error("Expecting a string")


def value2url(value):
    if value == None:
        Log.error("")

    if isinstance(value, Mapping):
        output = "&".join([value2url(k) + "=" + (value2url(v) if isinstance(v, basestring) else value2url(value2json(v))) for k,v in value.items()])
    elif isinstance(value, unicode):
        output = "".join([_map2url[c] for c in unicode2latin1(value)])
    elif isinstance(value, str):
        output = "".join([_map2url[c] for c in value])
    elif hasattr(value, "__iter__"):
        output = ",".join(value2url(v) for v in value)
    else:
        output = unicode(value)
    return output


def url_param2value(param):
    """
    CONVERT URL QUERY PARAMETERS INTO DICT
    """
    if isinstance(param, unicode):
        param = param.encode("ascii")

    def _decode(v):
        output = []
        i = 0
        while i < len(v):
            c = v[i]
            if c == "%":
                d = hex2bytes(v[i + 1:i + 3])
                output.append(d)
                i += 3
            else:
                output.append(c)
                i += 1

        output = (b"".join(output)).decode("latin1")
        try:
            return json2value(output)
        except Exception:
            pass
        return output


    query = {}
    for p in param.split(b'&'):
        if not p:
            continue
        if p.find(b"=") == -1:
            k = p
            v = True
        else:
            k, v = p.split(b"=")
            v = _decode(v)

        u = query.get(k)
        if u is None:
            query[k] = v
        elif isinstance(u, list):
            u += [v]
        else:
            query[k] = [u, v]

    return query


def html2unicode(value):
    # http://stackoverflow.com/questions/57708/convert-xml-html-entities-into-unicode-string-in-python
    return HTMLParser.HTMLParser().unescape(value)


def unicode2html(value):
    return cgi.escape(value)


def unicode2latin1(value):
    output = value.encode("latin1")
    return output


def quote2string(value):
    if value[0] == "\"" and value[-1] == "\"":
        value = value[1:-1]

    return value.replace("\\\\", "\\").replace("\\\"", "\"").replace("\\'", "'").replace("\\\n", "\n").replace("\\\t", "\t")

# RETURN PYTHON CODE FOR THE SAME

def value2code(value):
    return repr(value)


def DataFrame2string(df, columns=None):
    output = StringIO.StringIO()
    try:
        df.to_csv(output, sep="\t", header=True, cols=columns, engine='python')
        return output.getvalue()
    finally:
        output.close()


def ascii2char(ascii):
    return chr(ascii)


def char2ascii(char):
    return ord(char)


def ascii2unicode(value):
    return value.decode("latin1")


def latin12hex(value):
    return value.encode("hex")


def int2hex(value, size):
    return (("0" * size) + hex(value)[2:])[-size:]


def hex2bytes(value):
    return value.decode("hex")


def bytes2hex(value, separator=" "):
    return separator.join("%02X" % ord(x) for x in value)


def base642bytearray(value):
    return bytearray(base64.b64decode(value))


def base642bytes(value):
    return base64.b64decode(value)


def bytes2base64(value):
    return base64.b64encode(value).decode("utf8")


def bytes2sha1(value):
    if isinstance(value, unicode):
        Log.error("can not convert unicode to sha1")
    sha = hashlib.sha1(value)
    return sha.hexdigest()

def value2intlist(value):
    if value == None:
        return None
    elif hasattr(value, '__iter__'):
        output = [int(d) for d in value if d != "" and d != None]
        return output
    elif value.strip() == "":
        return None
    else:
        return [int(value)]


def value2int(value):
    if value == None:
        return None
    else:
        return int(value)


def value2number(v):
    try:
        if isinstance(v, float) and round(v, 0) != v:
            return v
            # IF LOOKS LIKE AN INT, RETURN AN INT
        return int(v)
    except Exception:
        try:
            return float(v)
        except Exception, e:
            Log.error("Not a number ({{value}})",  value= v, cause=e)


def utf82unicode(value):
    return value.decode('utf8')


def unicode2utf8(value):
    return value.encode('utf8')


def latin12unicode(value):
    if isinstance(value, unicode):
        Log.error("can not convert unicode from latin1")
    try:
        return unicode(value.decode('iso-8859-1'))
    except Exception, e:
        Log.error("Can not convert {{value|quote}} to unicode",  value= value)


def pipe2value(value):
    type = value[0]
    if type == '0':
        return None
    if type == 'n':
        return value2number(value[1::])

    if type != 's' and type != 'a':
        Log.error("unknown pipe type ({{type}}) in {{value}}",  type= type,  value= value)

    # EXPECTING MOST STRINGS TO NOT HAVE ESCAPED CHARS
    output = _unPipe(value)
    if type == 's':
        return output

    return [pipe2value(v) for v in output.split("|")]


def zip2bytes(compressed):
    """
    UNZIP DATA
    """
    if hasattr(compressed, "read"):
        return gzip.GzipFile(fileobj=compressed, mode='r')

    buff = BytesIO(compressed)
    archive = gzip.GzipFile(fileobj=buff, mode='r')
    return safe_size(archive)


def bytes2zip(bytes):
    """
    RETURN COMPRESSED BYTES
    """
    if hasattr(bytes, "read"):
        buff = TemporaryFile()
        archive = gzip.GzipFile(fileobj=buff, mode='w')
        for b in bytes:
            archive.write(b)
        archive.close()
        buff.seek(0)
        return FileString(buff)

    buff = BytesIO()
    archive = gzip.GzipFile(fileobj=buff, mode='w')
    archive.write(bytes)
    archive.close()
    return buff.getvalue()


def ini2value(ini_content):
    """
    INI FILE CONTENT TO Dict
    """
    from ConfigParser import ConfigParser

    buff = StringIO.StringIO(ini_content)
    config = ConfigParser()
    config._read(buff, "dummy")

    output = {}
    for section in config.sections():
        output[section]=s = {}
        for k, v in config.items(section):
            s[k]=v
    return wrap(output)













_map2url = {chr(i): latin12unicode(chr(i)) for i in range(32, 256)}
for c in " {}<>;/?:@&=+$,":
    _map2url[c] = "%" + int2hex(ord(c), 2)


def _unPipe(value):
    s = value.find("\\", 1)
    if s < 0:
        return value[1::]

    result = ""
    e = 1
    while True:
        c = value[s + 1]
        if c == 'p':
            result = result + value[e:s] + '|'
            s += 2
            e = s
        elif c == '\\':
            result = result + value[e:s] + '\\'
            s += 2
            e = s
        else:
            s += 1

        s = value.find("\\", s)
        if s < 0:
            break
    return result + value[e::]

json_decoder = json.JSONDecoder().decode
