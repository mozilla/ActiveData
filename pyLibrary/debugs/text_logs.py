# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from pyLibrary.debugs.exceptions import suppress_exception
from pyLibrary.strings import expand_template
from pyLibrary.thread.lock import Lock

_Except = None
_Queue = None
_Thread = None
_Till = None
_File = None
_Log = None


def _delayed_imports():
    global _Except
    global _Queue
    global _Thread
    global _Till
    global _File
    global _Log

    from pyLibrary.debugs.exceptions import Except as _Except
    from pyLibrary.thread.threads import Queue as _Queue
    from pyLibrary.thread.threads import Thread as _Thread
    from pyLibrary.thread.threads import Till as _Till
    from pyLibrary.env.files import File as _File
    from pyLibrary.debugs.logs import Log as _Log

    _ = _Except
    _ = _Queue
    _ = _Thread
    _ = _Till
    _ = _File
    _ = _Log


class TextLog(object):
    """
    ABSTRACT BASE CLASS FOR JSON LOGGING
    """
    def write(self, template, params):
        pass

    def stop(self):
        pass


class TextLog_usingFile(TextLog):
    def __init__(self, file):
        assert file

        if not _Log:
            _delayed_imports()

        self.file = _File(file)
        if self.file.exists:
            self.file.backup()
            self.file.delete()

        self.file_lock = Lock("file lock for logging")

    def write(self, template, params):
        try:
            with self.file_lock:
                self.file.append(expand_template(template, params))
        except Exception, e:
            _Log.warning("Problem writing to file {{file}}, waiting...", file=file.name, cause=e)
            _Till(seconds=5).wait()


class TextLog_usingThread(TextLog):

    def __init__(self, logger):
        if not _Log:
            _delayed_imports()
        if not isinstance(logger, TextLog):
            _Log.error("Expecting a TextLog")

        self.queue = _Queue("Queue for " + self.__class__.__name__, max=10000, silent=True, allow_add_after_close=True)
        self.logger = logger

        def worker(logger, please_stop):
            try:
                while not please_stop:
                    _Till(seconds=1).wait()
                    logs = self.queue.pop_all()
                    for log in logs:
                        if log is _Thread.STOP:
                            please_stop.go()
                        else:
                            logger.write(**log)
            finally:
                logger.stop()

        self.thread = _Thread("Thread for " + self.__class__.__name__, worker, logger)
        self.thread.parent.remove_child(self.thread)  # LOGGING WILL BE RESPONSIBLE FOR THREAD stop()
        self.thread.start()

    def write(self, template, params):
        try:
            self.queue.add({"template": template, "params": params})
            return self
        except Exception, e:
            e = _Except.wrap(e)
            raise e  # OH NO!

    def stop(self):
        with suppress_exception:
            self.queue.add(_Thread.STOP)  # BE PATIENT, LET REST OF MESSAGE BE SENT
            self.thread.join()
            self.logger.stop()

        with suppress_exception:
            self.queue.close()


class TextLog_usingMulti(TextLog):
    def __init__(self):
        self.many = []

    def write(self, template, params):
        bad = []
        for m in self.many:
            try:
                m.write(template, params)
            except Exception, e:
                bad.append(m)
                if not _Log:
                    _delayed_imports()

                _Log.warning("Logger failed!  It will be removed: {{type}}", type=m.__class__.__name__, cause=e)
        with suppress_exception:
            for b in bad:
                self.many.remove(b)

        return self

    def add_log(self, logger):
        if logger == None:
            if not _Log:
                _delayed_imports()

            _Log.warning("Expecting a non-None logger")

        self.many.append(logger)
        return self

    def remove_log(self, logger):
        self.many.remove(logger)
        return self

    def clear_log(self):
        self.many = []

    def stop(self):
        for m in self.many:
            with suppress_exception:
                m.stop()


class TextLog_usingStream(TextLog):
    def __init__(self, stream):
        assert stream
        self.stream = stream

    def write(self, template, params):
        value = expand_template(template, params)
        if isinstance(value, unicode):
            value = value.encode('utf8')
        self.stream.write(value + b"\n")

    def stop(self):
        pass

