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
from time import sleep as _sleep
from time import time as _time

from pyLibrary.thread.signal import Signal
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import Duration

DEBUG = True
INTERVAL = 0.1
next_ping = _time()
done = Signal("Timers shutdown")
done.go()

class Till(Signal):
    """
    TIMEOUT AS A SIGNAL
    """
    enabled = False
    all_timers = []
    locker = _allocate_lock()


    def __new__(cls, till=None, timeout=None, seconds=None):
        if not Till.enabled:
            return done
        elif till is None and timeout is None and seconds is None:
            return None
        else:
            return object.__new__(cls)

    def __init__(self, till=None, timeout=None, seconds=None):
        global next_ping

        Signal.__init__(self, "a timeout")
        if till != None:
            timeout = Date(till).unix
        elif timeout != None:
            timeout = _time() + Duration(timeout).seconds
        elif seconds != None:
            timeout = _time() + seconds

        with Till.locker:
            next_ping = min(next_ping, timeout)
            Till.all_timers.append((timeout, self))

    @classmethod
    def daemon(cls, please_stop):
        global next_ping

        Till.enabled = True
        try:
            while not please_stop:
                now = _time()
                with Till.locker:
                    if next_ping > now:
                        _sleep(min(next_ping - now, INTERVAL))
                        continue

                    next_ping = now + INTERVAL
                    work = None
                    if Till.all_timers:
                        Till.all_timers.sort(key=lambda r: r[0])
                        for i, (t, s) in enumerate(Till.all_timers):
                            if now < t:
                                work, Till.all_timers[:i] = Till.all_timers[:i], []
                                next_ping = min(next_ping, Till.all_timers[0][0])
                                break
                        else:
                            work, Till.all_timers = Till.all_timers, []

                if work:
                    for t, s in work:
                        s.go()

        except Exception, e:
            from pyLibrary.debugs.logs import Log

            Log.warning("timer shutdown", cause=e)
        finally:
            Till.enabled = False
            # TRIGGER ALL REMAINING TIMERS RIGHT NOW
            with Till.locker:
                work, Till.all_timers = Till.all_timers, []
            for t, s in work:
                s.go()

