# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
# THIS THREADING MODULE IS PERMEATED BY THE please_stop SIGNAL.
# THIS SIGNAL IS IMPORTANT FOR PROPER SIGNALLING WHICH ALLOWS
# FOR FAST AND PREDICTABLE SHUTDOWN AND CLEANUP OF THREADS

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from thread import allocate_lock as _allocate_lock

import sys
from time import time

_Log = None
DEBUG = False
DEBUG_SIGNAL = False


def _late_import():
    global _Log

    from pyLibrary.debugs.logs import Log as _Log

    _ = _Log


class Signal(object):
    """
    SINGLE-USE THREAD SAFE SIGNAL

    go() - ACTIVATE SIGNAL (DOES NOTHING IF SIGNAL IS ALREADY ACTIVATED)
    wait() - PUT THREAD IN WAIT STATE UNTIL SIGNAL IS ACTIVATED
    on_go() - METHOD FOR OTHER THREAD TO RUN WHEN ACTIVATING SIGNAL
    """

    __slots__ = ["_name", "lock", "_go", "job_queue", "waiting_threads"]

    def __init__(self, name=None):
        if DEBUG:
            if not _Log:
                _late_import()
            _Log.note("New signal {{name|quote}}", name=name)
        self._name = name
        self.lock = _allocate_lock()
        self._go = False
        self.job_queue = None
        self.waiting_threads = None

    def __str__(self):
        return str(self._go)

    def __bool__(self):
        return self._go

    def __nonzero__(self):
        with self.lock:
            return self._go

    def wait(self):
        """
        PUT THREAD IN WAIT STATE UNTIL SIGNAL IS ACTIVATED
        """

        with self.lock:
            if self._go:
                return True
            stopper = _allocate_lock()
            stopper.acquire()
            if not self.waiting_threads:
                self.waiting_threads = [stopper]
            else:
                self.waiting_threads.append(stopper)

        if DEBUG:
            if not _Log:
                _late_import()
            _Log.note("wait for go {{name|quote}}", name=self.name)
        stopper.acquire()
        if DEBUG:
            if not _Log:
                _late_import()
            _Log.note("GOing! {{name|quote}}", name=self.name)
        return True

    def go(self):
        """
        ACTIVATE SIGNAL (DOES NOTHING IF SIGNAL IS ALREADY ACTIVATED)
        """
        if DEBUG:
            if not _Log:
                _late_import()
            _Log.note("GO! {{name|quote}}", name=self.name)

        with self.lock:
            if DEBUG:
                _Log.note("internal GO! {{name|quote}}", name=self.name)
            if self._go:
                return
            self._go = True
            jobs, self.job_queue = self.job_queue, None
            threads, self.waiting_threads = self.waiting_threads, None

        if threads:
            if DEBUG:
                _Log.note("Release {{num}} threads", num=len(threads))
            for t in threads:
                t.release()

        if jobs:
            for j in jobs:
                try:
                    j()
                except Exception, e:
                    if not _Log:
                        _late_import()
                    _Log.warning("Trigger on Signal.go() failed!", cause=e)

    def on_go(self, target):
        """
        RUN target WHEN SIGNALED
        """
        if not target:
            if not _Log:
                _late_import()
            _Log.error("expecting target")

        with self.lock:
            if self._go:
                if DEBUG_SIGNAL:
                    if not _Log:
                        _late_import()
                    _Log.note("Signal {{name|quote}} already triggered, running job immediately", name=self.name)
                target()
            else:
                if DEBUG:
                    if not _Log:
                        _late_import()
                    _Log.note("Adding target to signal {{name|quote}}", name=self.name)
                if not self.job_queue:
                    self.job_queue = [target]
                else:
                    self.job_queue.append(target)

    @property
    def name(self):
        if not self._name:
            return "anonymous signal"
        else:
            return self._name

    def __str__(self):
        return self.name.decode(unicode)

    def __or__(self, other):
        if other == None:
            return self
        if not isinstance(other, Signal):
            if not _Log:
                _late_import()
            _Log.error("Expecting OR with other signal")

        output = Signal(self.name + " | " + other.name)
        self.on_go(output.go)
        other.on_go(output.go)
        return output

    def __ror__(self, other):
        return self.__or__(other)

    def __and__(self, other):
        if other == None:
            return self
        if not isinstance(other, Signal):
            if not _Log:
                _late_import()
            _Log.error("Expecting OR with other signal")

        if DEBUG:
            output = Signal(self.name+" and "+other.name)
        else:
            output = Signal(self.name+" and "+other.name)

        gen = BinaryAndSignals(output)
        self.on_go(gen.advance)
        other.on_go(gen.advance)
        return output


class BinaryAndSignals(object):
    __slots__ = ["signal", "inc", "locker"]

    def __init__(self, signal):
        self.signal = signal
        self.locker = _allocate_lock()
        self.inc = 0

    def advance(self):
        with self.locker:
            if self.inc is 0:
                self.inc = 1
            else:
                self.signal.go()
