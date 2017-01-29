
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

import types
from collections import deque
from datetime import datetime, timedelta
from time import time

from mo_threads.lock import Lock
from mo_threads.signal import Signal
from mo_threads.threads import Thread, THREAD_STOP
from mo_threads.till import Till
from mo_times.dates import Date
from mo_times.durations import SECOND
from pyDots import coalesce, Null

_convert = None
_Except = None
_CProfiler = None
_Log = None
DEBUG = False

MAX_DATETIME = datetime(2286, 11, 20, 17, 46, 39)
DEFAULT_WAIT_TIME = timedelta(minutes=10)

datetime.strptime('2012-01-01', '%Y-%m-%d')  # http://bugs.python.org/issue7980


def _late_import():
    global _convert
    global _Except
    global _CProfiler
    global _Log

    from mo_logs.exceptions import Except as _Except
    from mo_logs.profiles import CProfiler as _CProfiler
    from mo_logs import Log as _Log

    _ = _convert
    _ = _Except
    _ = _CProfiler
    _ = _Log




class Queue(object):
    """
     SIMPLE MESSAGE QUEUE, multiprocessing.Queue REQUIRES SERIALIZATION, WHICH
     IS DIFFICULT TO USE JUST BETWEEN THREADS (SERIALIZATION REQUIRED)
    """

    def __init__(self, name, max=None, silent=False, unique=False, allow_add_after_close=False):
        """
        max - LIMIT THE NUMBER IN THE QUEUE, IF TOO MANY add() AND extend() WILL BLOCK
        silent - COMPLAIN IF THE READERS ARE TOO SLOW
        unique - SET True IF YOU WANT ONLY ONE INSTANCE IN THE QUEUE AT A TIME
        """
        self.name = name
        self.max = coalesce(max, 2 ** 10)
        self.silent = silent
        self.allow_add_after_close=allow_add_after_close
        self.unique = unique
        self.keep_running = True
        self.lock = Lock("lock for queue " + name)
        self.queue = deque()
        self.next_warning = Date.now()  # FOR DEBUGGING

    def __iter__(self):
        while self.keep_running:
            try:
                value = self.pop()
                if value is not None and value is not THREAD_STOP:
                    yield value
            except Exception, e:
                _Log.warning("Tell me about what happened here", e)

        _Log.note("queue iterator is done")

    def add(self, value, timeout=None):
        if not self.keep_running and not self.allow_add_after_close:
            _Log.error("Do not add to closed queue")

        with self.lock:
            if value is THREAD_STOP:
                # INSIDE THE lock SO THAT EXITING WILL RELEASE wait()
                self.queue.append(value)
                self.keep_running = False
                return

            self._wait_for_queue_space(timeout=None)
            if self.keep_running:
                if self.unique:
                    if value not in self.queue:
                        self.queue.append(value)
                else:
                    self.queue.append(value)
        return self

    def push(self, value):
        """
        SNEAK value TO FRONT OF THE QUEUE
        """
        if not self.keep_running and not self.allow_add_after_close:
            _Log.error("Do not push to closed queue")

        with self.lock:
            self._wait_for_queue_space()
            if self.keep_running:
                self.queue.appendleft(value)
        return self

    def pop_message(self, till=None):
        """
        RETURN TUPLE (message, payload) CALLER IS RESPONSIBLE FOR CALLING message.delete() WHEN DONE
        DUMMY IMPLEMENTATION FOR DEBUGGING
        """

        if till is not None and not isinstance(till, Signal):
            _Log.error("Expecting a signal")
        return Null, self.pop(till=till)

    def extend(self, values):
        if not self.keep_running and not self.allow_add_after_close:
            _Log.error("Do not push to closed queue")

        with self.lock:
            # ONCE THE queue IS BELOW LIMIT, ALLOW ADDING MORE
            self._wait_for_queue_space()
            if self.keep_running:
                if self.unique:
                    for v in values:
                        if v is THREAD_STOP:
                            self.keep_running = False
                            continue
                        if v not in self.queue:
                            self.queue.append(v)
                else:
                    for v in values:
                        if v is THREAD_STOP:
                            self.keep_running = False
                            continue
                        self.queue.append(v)
        return self

    def _wait_for_queue_space(self, timeout=DEFAULT_WAIT_TIME):
        """
        EXPECT THE self.lock TO BE HAD, WAITS FOR self.queue TO HAVE A LITTLE SPACE
        """
        wait_time = 5 * SECOND

        now = Date.now()
        if timeout != None:
            time_to_stop_waiting = now + timeout
        else:
            time_to_stop_waiting = None

        if self.next_warning < now:
            self.next_warning = now + wait_time

        while self.keep_running and len(self.queue) >= self.max:
            if now > time_to_stop_waiting:
                if not _Log:
                    _late_import()
                _Log.error(Thread.TIMEOUT)

            if self.silent:
                self.lock.wait(Till(till=time_to_stop_waiting))
            else:
                self.lock.wait(Till(timeout=wait_time))
                if len(self.queue) > self.max:
                    now = Date.now()
                    if self.next_warning < now:
                        self.next_warning = now + wait_time
                        _Log.alert(
                            "Queue by name of {{name|quote}} is full with ({{num}} items), thread(s) have been waiting {{wait_time}} sec",
                            name=self.name,
                            num=len(self.queue),
                            wait_time=wait_time
                        )

    def __len__(self):
        with self.lock:
            return len(self.queue)

    def __nonzero__(self):
        with self.lock:
            return any(r != THREAD_STOP for r in self.queue)

    def pop(self, till=None):
        """
        WAIT FOR NEXT ITEM ON THE QUEUE
        RETURN THREAD_STOP IF QUEUE IS CLOSED
        RETURN None IF till IS REACHED AND QUEUE IS STILL EMPTY

        :param till:  A `Signal` to stop waiting and return None
        :return:  A value, or a THREAD_STOP or None
        """
        if till is not None and not isinstance(till, Signal):
            _Log.error("expecting a signal")

        with self.lock:
            while self.keep_running:
                if self.queue:
                    value = self.queue.popleft()
                    return value
                if not self.lock.wait(till=till):
                    return None
        if DEBUG or not self.silent:
            _Log.note(self.name + " queue stopped")
        return THREAD_STOP

    def pop_all(self):
        """
        NON-BLOCKING POP ALL IN QUEUE, IF ANY
        """
        with self.lock:
            output = list(self.queue)
            self.queue.clear()

        if self.keep_running:
            return output
        else:
            return output + [THREAD_STOP]

    def pop_one(self):
        """
        NON-BLOCKING POP IN QUEUE, IF ANY
        """
        with self.lock:
            if not self.keep_running:
                return [THREAD_STOP]
            elif not self.queue:
                return None
            else:
                v =self.queue.pop()
                if v is THREAD_STOP:  # SENDING A STOP INTO THE QUEUE IS ALSO AN OPTION
                    self.keep_running = False
                return v

    def close(self):
        with self.lock:
            self.keep_running = False

    def commit(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class ThreadedQueue(Queue):
    """
    DISPATCH TO ANOTHER (SLOWER) queue IN BATCHES OF GIVEN size
    TODO: Check that this queue is not dropping items at shutdown
    """

    def __init__(
        self,
        name,
        queue,  # THE SLOWER QUEUE
        batch_size=None,  # THE MAX SIZE OF BATCHES SENT TO THE SLOW QUEUE
        max_size=None,  # SET THE MAXIMUM SIZE OF THE QUEUE, WRITERS WILL BLOCK IF QUEUE IS OVER THIS LIMIT
        period=None,  # MAX TIME BETWEEN FLUSHES TO SLOWER QUEUE
        silent=False,  # WRITES WILL COMPLAIN IF THEY ARE WAITING TOO LONG
        error_target=None  # CALL THIS WITH ERROR **AND THE LIST OF OBJECTS ATTEMPTED**
                           # BE CAREFUL!  THE THREAD MAKING THE CALL WILL NOT BE YOUR OWN!
                           # DEFAULT BEHAVIOUR: THIS WILL KEEP RETRYING WITH WARNINGS
    ):
        if not _Log:
            _late_import()

        batch_size = coalesce(batch_size, int(max_size / 2) if max_size else None, 900)
        max_size = coalesce(max_size, batch_size * 2)  # REASONABLE DEFAULT
        period = coalesce(period, SECOND).seconds

        Queue.__init__(self, name=name, max=max_size, silent=silent)

        def worker_bee(please_stop):
            def stopper():
                self.add(THREAD_STOP)

            please_stop.on_go(stopper)

            _buffer = []
            _post_push_functions = []
            now = time()
            next_push = Till(till=now + period)  # THE TIME WE SHOULD DO A PUSH
            last_push = now - period

            def push_to_queue():
                queue.extend(_buffer)
                del _buffer[:]
                for f in _post_push_functions:
                    f()
                del _post_push_functions[:]

            while not please_stop:
                try:
                    if not _buffer:
                        item = self.pop()
                        now = time()
                        if now > last_push + period:
                            # _Log.note("delay next push")
                            next_push = Till(till=now + period)
                    else:
                        item = self.pop(till=next_push)
                        now = time()

                    if item is THREAD_STOP:
                        push_to_queue()
                        please_stop.go()
                        break
                    elif isinstance(item, types.FunctionType):
                        _post_push_functions.append(item)
                    elif item is not None:
                        _buffer.append(item)

                except Exception, e:
                    e = _Except.wrap(e)
                    if error_target:
                        try:
                            error_target(e, _buffer)
                        except Exception, f:
                            _Log.warning(
                                "`error_target` should not throw, just deal",
                                name=name,
                                cause=f
                            )
                    else:
                        _Log.warning(
                            "Unexpected problem",
                            name=name,
                            cause=e
                        )

                try:
                    if len(_buffer) >= batch_size or next_push:
                        next_push = Till(till=now + period)
                        if _buffer:
                            push_to_queue()
                            last_push = now = time()

                except Exception, e:
                    e = _Except.wrap(e)
                    if error_target:
                        try:
                            error_target(e, _buffer)
                        except Exception, f:
                            _Log.warning(
                                "`error_target` should not throw, just deal",
                                name=name,
                                cause=f
                            )
                    else:
                        _Log.warning(
                            "Problem with {{name}} pushing {{num}} items to data sink",
                            name=name,
                            num=len(_buffer),
                            cause=e
                        )

            if _buffer:
                # ONE LAST PUSH, DO NOT HAVE TIME TO DEAL WITH ERRORS
                push_to_queue()

        self.thread = Thread.run("threaded queue for " + name, worker_bee, parent_thread=self)

    def add(self, value, timeout=None):
        with self.lock:
            self._wait_for_queue_space(timeout=timeout)
            if self.keep_running:
                self.queue.append(value)
            # if Random.range(0, 50) == 0:
            #     sizes = wrap([{"id":i["id"], "size":len(convert.value2json(i))} for i in self.queue if isinstance(i, Mapping)])
            #     size=sum(sizes.size)
            #     if size>50000000:
            #         from pyLibrary.queries import jx
            #
            #         biggest = jx.sort(sizes, "size").last().id
            #         _Log.note("Big record {{id}}", id=biggest)
            #     _Log.note("{{name}} has {{num}} items with json size of {{size|comma}}", name=self.name, num=len(self.queue), size=size)
        return self

    def extend(self, values):
        with self.lock:
            # ONCE THE queue IS BELOW LIMIT, ALLOW ADDING MORE
            self._wait_for_queue_space()
            if self.keep_running:
                self.queue.extend(values)
            _Log.note("{{name}} has {{num}} items", name=self.name, num=len(self.queue))
        return self

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        self.add(THREAD_STOP)
        if isinstance(b, BaseException):
            self.thread.please_stop.go()
        self.thread.join()

    def stop(self):
        self.add(THREAD_STOP)
        self.thread.join()
