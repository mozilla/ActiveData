# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import base64
import io
import re
import shutil
from datetime import datetime
from mimetypes import MimeTypes
from tempfile import mkdtemp

import os
from mo_dots import get_module, coalesce
from mo_logs import Log, Except

mime = MimeTypes()

class File(object):
    """
    ASSUMES ALL FILE CONTENT IS UTF8 ENCODED STRINGS
    """

    def __new__(cls, filename, buffering=2 ** 14, suffix=None):
        if isinstance(filename, File):
            return filename
        else:
            return object.__new__(cls)

    def __init__(self, filename, buffering=2 ** 14, suffix=None, mime_type=None):
        """
        YOU MAY SET filename TO {"path":p, "key":k} FOR CRYPTO FILES
        """
        self._mime_type = mime_type
        if filename == None:
            Log.error(u"File must be given a filename")
        elif isinstance(filename, File):
            return
        elif isinstance(filename, basestring):
            self.key = None
            if filename==b".":
                self._filename = b""
            elif filename.startswith(b"~"):
                home_path = os.path.expanduser(b"~")
                if os.sep == b"\\":
                    home_path = home_path.replace(os.sep, b"/")
                if home_path.endswith(b"/"):
                    home_path = home_path[:-1]
                filename = home_path + filename[1::]
            self._filename = filename.replace(os.sep, b"/")  # USE UNIX STANDARD
        else:
            self.key = base642bytearray(filename.key)
            self._filename = b"/".join(filename.path.split(os.sep))  # USE UNIX STANDARD

        while self._filename.find(b".../") >= 0:
            # LET ... REFER TO GRANDPARENT, .... REFER TO GREAT-GRAND-PARENT, etc...
            self._filename = self._filename.replace(b".../", b"../../")
        self.buffering = buffering


        if suffix:
            self._filename = File.add_suffix(self._filename, suffix)

    @classmethod
    def new_instance(cls, *path):
        return File(join_path(*path))

    @property
    def timestamp(self):
        output = os.path.getmtime(self.abspath)
        return output

    @property
    def filename(self):
        return self._filename.replace(b"/", os.sep)

    @property
    def abspath(self):
        if self._filename.startswith(b"~"):
            home_path = os.path.expanduser(b"~")
            if os.sep == b"\\":
                home_path = home_path.replace(os.sep, b"/")
            if home_path.endswith(b"/"):
                home_path = home_path[:-1]

            return home_path + self._filename[1::]
        else:
            if os.sep == b"\\":
                return os.path.abspath(self._filename).replace(os.sep, b"/")
            else:
                return os.path.abspath(self._filename)

    @staticmethod
    def add_suffix(filename, suffix):
        """
        ADD suffix TO THE filename (NOT INCLUDING THE FILE EXTENSION)
        """
        path = filename.split(b"/")
        parts = path[-1].split(b".")
        i = max(len(parts) - 2, 0)
        parts[i] = parts[i] + suffix
        path[-1] = b".".join(parts)
        return b"/".join(path)

    @property
    def extension(self):
        parts = self._filename.split(b"/")[-1].split(b".")
        if len(parts) == 1:
            return b""
        else:
            return parts[-1]

    @property
    def name(self):
        parts = self.abspath.split(b"/")[-1].split(b".")
        if len(parts) == 1:
            return parts[0]
        else:
            return b".".join(parts[0:-1])

    @property
    def mime_type(self):
        if not self._mime_type:
            if self.abspath.endswith(".json"):
                self._mime_type = "application/json"
            else:
                self._mime_type, _ = mime.guess_type(self.abspath)
                if not self._mime_type:
                    self._mime_type = "application/binary"
        return self._mime_type

    def find(self, pattern):
        """
        :param pattern: REGULAR EXPRESSION TO MATCH NAME (NOT INCLUDING PATH)
        :return: LIST OF File OBJECTS THAT HAVE MATCHING NAME
        """
        output = []
        def _find(dir):
            if re.match(pattern, dir._filename.split(b"/")[-1]):
                output.append(dir)
            if dir.is_directory():
                for c in dir.children:
                    _find(c)
        _find(self)
        return output

    def set_extension(self, ext):
        """
        RETURN NEW FILE WITH GIVEN EXTENSION
        """
        path = self._filename.split(b"/")
        parts = path[-1].split(b".")
        if len(parts) == 1:
            parts.append(ext)
        else:
            parts[-1] = ext

        path[-1] = b".".join(parts)
        return File(b"/".join(path))

    def set_name(self, name):
        """
        RETURN NEW FILE WITH GIVEN EXTENSION
        """
        path = self._filename.split(b"/")
        parts = path[-1].split(b".")
        if len(parts) == 1:
            path[-1] = name
        else:
            path[-1] = name + b"." + parts[-1]
        return File(b"/".join(path))

    def backup_name(self, timestamp=None):
        """
        RETURN A FILENAME THAT CAN SERVE AS A BACKUP FOR THIS FILE
        """
        suffix = datetime2string(coalesce(timestamp, datetime.now()), "%Y%m%d_%H%M%S")
        return File.add_suffix(self._filename, suffix)

    def read(self, encoding=b"utf8"):
        with open(self._filename, b"rb") as f:
            content = f.read().decode(encoding)
            if self.key:
                return get_module(u"mo_math.crypto").decrypt(content, self.key)
            else:
                return content

    def read_lines(self, encoding=b"utf8"):
        with open(self._filename, b"rb") as f:
            for line in f:
                yield line.decode(encoding).rstrip()

    def read_json(self, encoding=b"utf8", flexible=True, leaves=True):
        content = self.read(encoding=encoding)
        value = get_module(u"mo_json").json2value(content, flexible=flexible, leaves=leaves)
        abspath = self.abspath
        if os.sep == b"\\":
            abspath = b"/" + abspath.replace(os.sep, b"/")
        return get_module(b"mo_json_config").expand(value, b"file://" + abspath)

    def is_directory(self):
        return os.path.isdir(self._filename)

    def read_bytes(self):
        try:
            if not self.parent.exists:
                self.parent.create()
            with open(self._filename, b"rb") as f:
                return f.read()
        except Exception as e:
            Log.error(u"Problem reading file {{filename}}", filename=self.abspath, cause=e)

    def write_bytes(self, content):
        if not self.parent.exists:
            self.parent.create()
        with open(self._filename, b"wb") as f:
            f.write(content)

    def write(self, data):
        if not self.parent.exists:
            self.parent.create()
        with open(self._filename, b"wb") as f:
            if isinstance(data, list) and self.key:
                Log.error(u"list of data and keys are not supported, encrypt before sending to file")

            if isinstance(data, list):
                pass
            elif isinstance(data, basestring):
                data=[data]
            elif hasattr(data, b"__iter__"):
                pass

            for d in data:
                if not isinstance(d, unicode):
                    Log.error(u"Expecting unicode data only")
                if self.key:
                    f.write(get_module(u"crypto").encrypt(d, self.key).encode(b"utf8"))
                else:
                    f.write(d.encode(b"utf8"))

    def __iter__(self):
        # NOT SURE HOW TO MAXIMIZE FILE READ SPEED
        # http://stackoverflow.com/questions/8009882/how-to-read-large-file-line-by-line-in-python
        # http://effbot.org/zone/wide-finder.htm
        def output():
            try:
                path = self._filename
                if path.startswith(b"~"):
                    home_path = os.path.expanduser(b"~")
                    path = home_path + path[1::]

                with io.open(path, b"rb") as f:
                    for line in f:
                        yield line.decode('utf8').rstrip()
            except Exception as e:
                Log.error(u"Can not read line from {{filename}}", filename=self._filename, cause=e)

        return output()

    def append(self, content):
        """
        add a line to file
        """
        if not self.parent.exists:
            self.parent.create()
        with open(self._filename, b"ab") as output_file:
            if isinstance(content, str):
                Log.error(u"expecting to write unicode only")
            output_file.write(content.encode(b"utf8"))
            output_file.write(b"\n")

    def __len__(self):
        return os.path.getsize(self.abspath)

    def add(self, content):
        return self.append(content)

    def extend(self, content):
        try:
            if not self.parent.exists:
                self.parent.create()
            with open(self._filename, b"ab") as output_file:
                for c in content:
                    if isinstance(c, str):
                        Log.error(u"expecting to write unicode only")

                    output_file.write(c.encode(b"utf8"))
                    output_file.write(b"\n")
        except Exception as e:
            Log.error(u"Could not write to file", e)

    def delete(self):
        try:
            if os.path.isdir(self._filename):
                shutil.rmtree(self._filename)
            elif os.path.isfile(self._filename):
                os.remove(self._filename)
            return self
        except Exception as e:
            e = Except.wrap(e)
            if u"The system cannot find the path specified" in e:
                return
            Log.error(u"Could not remove file", e)

    def backup(self):
        path = self._filename.split(b"/")
        names = path[-1].split(b".")
        if len(names) == 1 or names[0] == '':
            backup = File(self._filename + b".backup " + datetime.utcnow().strftime(b"%Y%m%d %H%M%S"))
        else:
            backup = File.new_instance(
                b"/".join(path[:-1]),
                b".".join(names[:-1]) + b".backup " + datetime.now().strftime("%Y%m%d %H%M%S") + "." + names[-1]
            )
        File.copy(self, backup)
        return backup

    def create(self):
        try:
            os.makedirs(self._filename)
        except Exception as e:
            Log.error(u"Could not make directory {{dir_name}}",  dir_name= self._filename, cause=e)

    @property
    def children(self):
        return [File(self._filename + b"/" + c) for c in os.listdir(self.filename)]

    @property
    def leaves(self):
        for c in os.listdir(self.abspath):
            child = File(self._filename + b"/" + c)
            if child.is_directory():
                for l in child.leaves:
                    yield l
            else:
                yield child

    @property
    def parent(self):
        if not self._filename:
            return File(b"..")
        elif self._filename.endswith(b".."):
            return File(self._filename+b"/..")
        else:
            return File(b"/".join(self._filename.split(b"/")[:-1]))

    @property
    def exists(self):
        if self._filename in [b"", b"."]:
            return True
        try:
            return os.path.exists(self._filename)
        except Exception as e:
            return False

    def __bool__(self):
        return self.__nonzero__()

    def __nonzero__(self):
        """
        USED FOR FILE EXISTENCE TESTING
        """
        if self._filename in [b"", b"."]:
            return True
        try:
            return os.path.exists(self._filename)
        except Exception as e:
            return False

    @classmethod
    def copy(cls, from_, to_):
        _copy(File(from_), File(to_))

    def __unicode__(self):
        return self.abspath


class TempDirectory(File):
    def __new__(cls, *args, **kwargs):
        return object.__new__(cls)

    def __init__(self):
        File.__init__(self, mkdtemp())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.delete()


def _copy(from_, to_):
    if from_.is_directory():
        for c in os.listdir(from_.abspath):
            _copy(File.new_instance(from_, c), File.new_instance(to_, c))
    else:
        File.new_instance(to_).write_bytes(File.new_instance(from_).read_bytes())


def base642bytearray(value):
    if value == None:
        return bytearray(b"")
    else:
        return bytearray(base64.b64decode(value))


def datetime2string(value, format="%Y-%m-%d %H:%M:%S"):
    try:
        return value.strftime(format)
    except Exception as e:
        Log.error(u"Can not format {{value}} with {{format}}", value=value, format=format, cause=e)


def join_path(*path):
    def scrub(i, p):
        if isinstance(p, File):
            p = p.abspath
        if p == b"/":
            return b"."
        p = p.replace(os.sep, b"/")
        if p[-1] == b'/':
            p = p[:-1]
        if i > 0 and p[0] == b'/':
            p = p[1:]
        return p

    scrubbed = []
    for i, p in enumerate(path):
        scrubbed.extend(scrub(i, p).split(b"/"))
    simpler = []
    for s in scrubbed:
        if s == ".":
            pass
        elif s == "..":
            if simpler:
                simpler.pop()
            else:
                simpler.append(s)
        else:
            simpler.append(s)
    if not simpler:
        joined = "."
    else:
        joined = b'/'.join(simpler)
    return joined

