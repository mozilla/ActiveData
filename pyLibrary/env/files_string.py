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
# }
# }}


from __future__ import unicode_literals
from __future__ import division
import os
from tempfile import TemporaryFile
from pyLibrary.debugs.logs import Log


MIN_READ_SIZE = 8 * 1024
MAX_STRING_SIZE = 1 * 1024 * 1024


class FileString(object):
    """
    ACTS LIKE A STRING, BUT IS A FILE
    """

    def __init__(self, file):
        self.file = file

    def decode(self, encoding):
        if encoding != "utf8":
            Log.error("can not handle {{encoding}}", {"encoding": encoding})
        self.encoding = encoding
        return self

    def split(self, sep):
        if sep != "\n":
            Log.error("Can only split by lines")
        self.file.seek(0)
        return LazyLines(self.file, self.encoding)

    def __len__(self):
        temp = self.file.tell()
        self.file.seek(0, 2)
        file_length = self.file.tell()
        self.file.seek(temp)
        return file_length

    def __add__(self, other):
        self.file.seek(0, 2)
        self.file.write(other)

    def __radd__(self, other):
        new_file = TemporaryFile()
        new_file.write(other)
        self.file.seek(0)
        for l in self.file:
            new_file.write(l)
        new_file.seek(0)
        return FileString(new_file)

    def __getattr__(self, attr):
        return getattr(self.file, attr)

    def __del__(self):
        self.file, temp = None, self.file
        if temp:
            temp.close()

    def __iter__(self):
        self.file.seek(0)
        return self.file


def safe_size(source):
    """
    READ THE source UP TO SOME LIMIT, THEN COPY TO A FILE IF TOO BIG
    """

    total_bytes = 0
    bytes = []
    b = source.read(MIN_READ_SIZE)
    while b:
        total_bytes += len(b)
        bytes.append(b)
        if total_bytes > MAX_STRING_SIZE:
            data = FileString(TemporaryFile())
            for bb in bytes:
                data.write(bb)
            del bytes
            del bb
            b = source.read(MIN_READ_SIZE)
            while b:
                data.write(b)
                b = source.read(MIN_READ_SIZE)
            data.seek(0)
            return data
        b = source.read(MIN_READ_SIZE)

    data = b"".join(bytes)
    del bytes
    return data


class LazyLines(object):
    def __init__(self, source, encoding):
        self.iter = (l.decode(encoding) for l in source)
        self.last = None
        self.next = 0

    def __getslice__(self, i, j):
        if i == self.next:
            return self
        Log.error("Do not know how to slice this generator")

    def __iter__(self):
        def output():
            while True:
                self.last = self.iter.next()
                self.next += 1
                yield self.last

        return output()

    def __getitem__(self, item):
        try:
            if item == self.next:
                self.last = self.iter.next()
                self.next += 1
                return self.last
            elif item == self.next - 1:
                return self.last
            else:
                Log.error("can not index out-of-order too much")
        except Exception, e:
            Log.error("Problem indexing", e)


