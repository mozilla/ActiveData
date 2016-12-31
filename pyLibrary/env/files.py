# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


import io
import os
import shutil
from datetime import datetime

from pyLibrary import convert
from pyDots import coalesce
from pyLibrary.maths import crypto
from pyLibrary.strings import utf82unicode


class File(object):
    """
    ASSUMES ALL FILE CONTENT IS UTF8 ENCODED STRINGS
    """

    def __init__(self, filename, buffering=2 ** 14, suffix=None):
        """
        YOU MAY SET filename TO {"path":p, "key":k} FOR CRYPTO FILES
        """
        if filename == None:
            from pyLibrary.debugs.logs import Log

            Log.error("File must be given a filename")
        elif isinstance(filename, basestring):
            self.key = None
            if filename.startswith("~"):
                home_path = os.path.expanduser("~")
                if os.sep == "\\":
                    home_path = home_path.replace(os.sep, "/")
                if home_path.endswith("/"):
                    home_path = home_path[:-1]
                filename = home_path + filename[1::]
            self._filename = filename.replace(os.sep, "/")  # USE UNIX STANDARD
        else:
            self.key = convert.base642bytearray(filename.key)
            self._filename = "/".join(filename.path.split(os.sep))  # USE UNIX STANDARD

        while self._filename.find(".../") >= 0:
            # LET ... REFER TO GRANDPARENT, .... REFER TO GREAT-GRAND-PARENT, etc...
            self._filename = self._filename.replace(".../", "../../")
        self.buffering = buffering


        if suffix:
            self._filename = File.add_suffix(self._filename, suffix)

    @classmethod
    def new_instance(cls, *path):
        def scrub(i, p):
            if isinstance(p, File):
                p = p.abspath
            p = p.replace(os.sep, "/")
            if p[-1] == '/':
                p = p[:-1]
            if i > 0 and p[0] == '/':
                p = p[1:]
            return p

        return File('/'.join(scrub(i, p) for i, p in enumerate(path)))


    @property
    def filename(self):
        return self._filename.replace("/", os.sep)

    @property
    def abspath(self):
        if self._filename.startswith("~"):
            home_path = os.path.expanduser("~")
            if os.sep == "\\":
                home_path = home_path.replace(os.sep, "/")
            if home_path.endswith("/"):
                home_path = home_path[:-1]

            return home_path + self._filename[1::]
        else:
            if os.sep == "\\":
                return os.path.abspath(self._filename).replace(os.sep, "/")
            else:
                return os.path.abspath(self._filename)

    @staticmethod
    def add_suffix(filename, suffix):
        """
        ADD suffix TO THE filename (NOT INCLUDING THE FILE EXTENSION)
        """
        path = filename.split("/")
        parts = path[-1].split(".")
        i = max(len(parts) - 2, 0)
        parts[i] = parts[i] + suffix
        path[-1] = ".".join(parts)
        return "/".join(path)

    @property
    def extension(self):
        parts = self._filename.split("/")[-1].split(".")
        if len(parts) == 1:
            return ""
        else:
            return parts[-1]

    @property
    def name(self):
        parts = self._filename.split("/")[-1].split(".")
        if len(parts) == 1:
            return parts[0]
        else:
            return ".".join(parts[0:-1])

    def set_extension(self, ext):
        """
        RETURN NEW FILE WITH GIVEN EXTENSION
        """
        path = self._filename.split("/")
        parts = path[-1].split(".")
        if len(parts) == 1:
            parts.append(ext)
        else:
            parts[-1] = ext

        path[-1] = ".".join(parts)
        return File("/".join(path))

    def set_name(self, name):
        """
        RETURN NEW FILE WITH GIVEN EXTENSION
        """
        path = self._filename.split("/")
        parts = path[-1].split(".")
        if len(parts) == 1:
            path[-1] = name
        else:
            path[-1] = name + "." + parts[-1]
        return File("/".join(path))

    def backup_name(self, timestamp=None):
        """
        RETURN A FILENAME THAT CAN SERVE AS A BACKUP FOR THIS FILE
        """
        suffix = convert.datetime2string(coalesce(timestamp, datetime.now()), "%Y%m%d_%H%M%S")
        return File.add_suffix(self._filename, suffix)

    def read(self, encoding="utf8"):
        with open(self._filename, "rb") as f:
            content = f.read().decode(encoding)
            if self.key:
                return crypto.decrypt(content, self.key)
            else:
                return content

    def read_json(self, encoding="utf8"):
        from pyLibrary.jsons import ref

        content = self.read(encoding=encoding)
        value = convert.json2value(content, flexible=True, leaves=True)
        abspath = self.abspath
        if os.sep == "\\":
            abspath = "/" + abspath.replace(os.sep, "/")
        return ref.expand(value, "file://" + abspath)

    def is_directory(self):
        return os.path.isdir(self._filename)

    def read_bytes(self):
        try:
            if not self.parent.exists:
                self.parent.create()
            with open(self._filename, "rb") as f:
                return f.read()
        except Exception, e:
            from pyLibrary.debugs.logs import Log

            Log.error("Problem reading file {{filename}}", self.abspath)

    def write_bytes(self, content):
        if not self.parent.exists:
            self.parent.create()
        with open(self._filename, "wb") as f:
            f.write(content)

    def write(self, data):
        if not self.parent.exists:
            self.parent.create()
        with open(self._filename, "wb") as f:
            if isinstance(data, list) and self.key:
                from pyLibrary.debugs.logs import Log

                Log.error("list of data and keys are not supported, encrypt before sending to file")

            if isinstance(data, list):
                pass
            elif isinstance(data, basestring):
                data=[data]
            elif hasattr(data, "__iter__"):
                pass

            for d in data:
                if not isinstance(d, unicode):
                    from pyLibrary.debugs.logs import Log

                    Log.error("Expecting unicode data only")
                if self.key:
                    f.write(crypto.encrypt(d, self.key).encode("utf8"))
                else:
                    f.write(d.encode("utf8"))

    def __iter__(self):
        # NOT SURE HOW TO MAXIMIZE FILE READ SPEED
        # http://stackoverflow.com/questions/8009882/how-to-read-large-file-line-by-line-in-python
        # http://effbot.org/zone/wide-finder.htm
        def output():
            try:
                path = self._filename
                if path.startswith("~"):
                    home_path = os.path.expanduser("~")
                    path = home_path + path[1::]

                with io.open(path, "rb") as f:
                    for line in f:
                        yield utf82unicode(line)
            except Exception, e:
                from pyLibrary.debugs.logs import Log

                Log.error("Can not read line from {{filename}}", filename=self._filename, cause=e)

        return output()

    def append(self, content):
        """
        add a line to file
        """
        if not self.parent.exists:
            self.parent.create()
        with open(self._filename, "ab") as output_file:
            if isinstance(content, str):
                from pyLibrary.debugs.logs import Log

                Log.error("expecting to write unicode only")
            output_file.write(content.encode("utf-8"))
            output_file.write(b"\n")

    def add(self, content):
        return self.append(content)

    def extend(self, content):
        try:
            if not self.parent.exists:
                self.parent.create()
            with open(self._filename, "ab") as output_file:
                for c in content:
                    if isinstance(c, str):
                        from pyLibrary.debugs.logs import Log

                        Log.error("expecting to write unicode only")

                    output_file.write(c.encode("utf-8"))
                    output_file.write(b"\n")
        except Exception, e:
            from pyLibrary.debugs.logs import Log

            Log.error("Could not write to file", e)

    def delete(self):
        try:
            if os.path.isdir(self._filename):
                shutil.rmtree(self._filename)
            elif os.path.isfile(self._filename):
                os.remove(self._filename)
            return self
        except Exception, e:
            if e.strerror == "The system cannot find the path specified":
                return
            from pyLibrary.debugs.logs import Log

            Log.error("Could not remove file", e)

    def backup(self):
        path = self._filename.split("/")
        names = path[-1].split(".")
        if len(names) == 1 or names[0] == '':
            backup = File(self._filename + ".backup " + datetime.utcnow().strftime("%Y%m%d %H%M%S"))
        else:
            backup = File.new_instance(
                "/".join(path[:-1]),
                ".".join(names[:-1]) + ".backup " + datetime.now().strftime("%Y%m%d %H%M%S") + "." + names[-1]
            )
        File.copy(self, backup)
        return backup

    def create(self):
        try:
            os.makedirs(self._filename)
        except Exception, e:
            from pyLibrary.debugs.logs import Log

            Log.error("Could not make directory {{dir_name}}",  dir_name= self._filename, cause=e)

    @property
    def children(self):
        return [File(self._filename + "/" + c) for c in os.listdir(self.filename)]

    @property
    def parent(self):
        return File("/".join(self._filename.split("/")[:-1]))

    @property
    def exists(self):
        if self._filename in ["", "."]:
            return True
        try:
            return os.path.exists(self._filename)
        except Exception, e:
            return False

    def __bool__(self):
        return self.__nonzero__()


    def __nonzero__(self):
        """
        USED FOR FILE EXISTENCE TESTING
        """
        if self._filename in ["", "."]:
            return True
        try:
            return os.path.exists(self._filename)
        except Exception, e:
            return False

    @classmethod
    def copy(cls, from_, to_):
        File.new_instance(to_).write_bytes(File.new_instance(from_).read_bytes())
