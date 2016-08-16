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

import sys

from pyLibrary.debugs.exceptions import suppress_exception
from pyLibrary.thread.threads import Thread, Lock
from pyLibrary.strings import expand_template

DEBUG_LOGGING = False

_Log = None
_Except = None
_Queue = None


def _delayed_imports():
    global _Log
    global _Except
    global _Queue

    from pyLibrary.debugs.logs import Log as _Log
    from pyLibrary.debugs.exceptions import Except as _Except
    from pyLibrary.thread.threads import Queue as _Queue

    _ = _Log
    _ = _Except
    _ = _Queue

class TextLog(object):
    def write(self, template, params):
        pass

    def stop(self):
        pass


class TextLog_usingFile(TextLog):
    def __init__(self, file):
        assert file

        from pyLibrary.env.files import File

        self.file = File(file)
        if self.file.exists:
            self.file.backup()
            self.file.delete()

        self.file_lock = Lock("file lock for logging")

    def write(self, template, params):
        with self.file_lock:
            self.file.append(expand_template(template, params))


class TextLog_usingThread(TextLog):

    def __init__(self, logger):
        if not _Log:
            _delayed_imports()

        self.queue = _Queue("logs", max=10000, silent=True)
        self.logger = logger

        def worker(please_stop):
            while not please_stop:
                Thread.sleep(1)
                logs = self.queue.pop_all()
                for log in logs:
                    if log is Thread.STOP:
                        if DEBUG_LOGGING:
                            sys.stdout.write("TextLog_usingThread.worker() sees stop, filling rest of queue\n")
                        please_stop.go()
                    else:
                        self.logger.write(**log)

        self.thread = Thread("log thread", worker)
        self.thread.parent.remove_child(self.thread)  # LOGGING WILL BE RESPONSIBLE FOR THREAD stop()
        self.thread.start()

    def write(self, template, params):
        try:
            self.queue.add({"template": template, "params": params})
            return self
        except Exception, e:
            e = _Except.wrap(e)
            sys.stdout.write("IF YOU SEE THIS, IT IS LIKELY YOU FORGOT TO RUN Log.start() FIRST\n")
            raise e  # OH NO!

    def stop(self):
        try:
            if DEBUG_LOGGING:
                sys.stdout.write("injecting stop into queue\n")
            self.queue.add(Thread.STOP)  # BE PATIENT, LET REST OF MESSAGE BE SENT
            self.thread.join()
            if DEBUG_LOGGING:
                sys.stdout.write("TextLog_usingThread telling logger to stop\n")
            self.logger.stop()
        except Exception, e:
            if DEBUG_LOGGING:
                raise e

        try:
            self.queue.close()
        except Exception, f:
            if DEBUG_LOGGING:
                raise f


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
                sys.stdout.write("a logger failed")
                if not _Log:
                    _delayed_imports()

                _Log.warning("Logger failed!  It will be removed: {{type}}", type=m.__class__.__name__, cause=e)
        with suppress_exception:
            for b in bad:
                self.many.remove(b)

        return self

    def add_log(self, logger):
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
        self.stream.write(value+b"\n")

    def stop(self):
        pass

