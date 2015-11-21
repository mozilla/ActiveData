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

from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from collections import deque
from copy import copy
from datetime import datetime, timedelta
import thread
import threading
import time
import sys

from pyLibrary import strings
from pyLibrary.dot import coalesce, Dict
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import SECOND, MINUTE, Duration


_Log = None
_Except = None
DEBUG = True
MAX_DATETIME = datetime(2286, 11, 20, 17, 46, 39)
DEFAULT_WAIT_TIME = timedelta(minutes=5)

def _late_import():
    global _Log
    global _Except

    from pyLibrary.debugs.logs import Log as _Log
    from pyLibrary.debugs.logs import Except as _Except

    _ = _Log
    _ = _Except


class Lock(object):
    """
    SIMPLE LOCK (ACTUALLY, A PYTHON threadind.Condition() WITH notify() BEFORE EVERY RELEASE)
    """

    def __init__(self, name=""):
        self.monitor = threading.Condition()
        # if not name:
        # if "extract_stack" not in globals():
        # from pyLibrary.debugs.logs import extract_stack
        #
        #     self.name = extract_stack(1)[0].method


    def __enter__(self):
        # with pyLibrary.times.timer.Timer("get lock"):
        self.monitor.acquire()
        return self

    def __exit__(self, a, b, c):
        self.monitor.notify()
        self.monitor.release()

    def wait(self, timeout=None, till=None):
        if till:
            timeout = (till - Date.now()).seconds
            if timeout < 0:
                return
        if isinstance(timeout, Duration):
            timeout = timeout.seconds

        self.monitor.wait(timeout=float(timeout) if timeout else None)

    def notify_all(self):
        self.monitor.notify_all()


class Queue(object):
    """
     SIMPLE MESSAGE QUEUE, multiprocessing.Queue REQUIRES SERIALIZATION, WHICH
     IS DIFFICULT TO USE JUST BETWEEN THREADS (SERIALIZATION REQUIRED)
    """

    def __init__(self, name, max=None, silent=False, unique=False):
        """
        max - LIMIT THE NUMBER IN THE QUEUE, IF TOO MANY add() AND extend() WILL BLOCK
        silent - COMPLAIN IF THE READERS ARE TOO SLOW
        unique - SET True IF YOU WANT ONLY ONE INSTANCE IN THE QUEUE AT A TIME
        """
        self.name = name
        self.max = coalesce(max, 2 ** 10)
        self.silent = silent
        self.unique = unique
        self.keep_running = True
        self.lock = Lock("lock for queue " + name)
        self.queue = deque()
        self.next_warning = Date.now()  # FOR DEBUGGING

    def __iter__(self):
        while self.keep_running:
            try:
                value = self.pop()
                if value is not Thread.STOP:
                    yield value
            except Exception, e:
                _Log.warning("Tell me about what happened here", e)

        _Log.note("queue iterator is done")


    def add(self, value, timeout=None):
        with self.lock:
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
        with self.lock:
            self._wait_for_queue_space()
            if self.keep_running:
                self.queue.appendleft(value)
        return self

    def extend(self, values):
        with self.lock:
            # ONCE THE queue IS BELOW LIMIT, ALLOW ADDING MORE
            self._wait_for_queue_space()
            if self.keep_running:
                if self.unique:
                    for v in values:
                        if v not in self.queue:
                            self.queue.append(v)
                else:
                    self.queue.extend(values)
        return self

    def _wait_for_queue_space(self, timeout=DEFAULT_WAIT_TIME):
        """
        EXPECT THE self.lock TO BE HAD, WAITS FOR self.queue TO HAVE A LITTLE SPACE
        """
        wait_time = 5 * SECOND

        now = Date.now()
        if timeout:
            time_to_stop_waiting = now + timeout
        else:
            time_to_stop_waiting = datetime(2286, 11, 20, 17, 46, 39)

        if self.next_warning < now:
            self.next_warning = now + wait_time

        while self.keep_running and len(self.queue) > self.max:
            if time_to_stop_waiting < now:
                if not _Log:
                    _late_import()
                _Log.error(Thread.TIMEOUT)

            if self.silent:
                self.lock.wait()
            else:
                self.lock.wait(wait_time)
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
            return any(r != Thread.STOP for r in self.queue)

    def pop(self, till=None, timeout=None):
        """
        WAIT FOR NEXT ITEM ON THE QUEUE
        RETURN Thread.STOP IF QUEUE IS CLOSED
        IF till IS PROVIDED, THEN pop() CAN TIMEOUT AND RETURN None
        """

        if timeout:
            till = Date.now() + timeout

        with self.lock:
            if till == None:
                while self.keep_running:
                    if self.queue:
                        value = self.queue.popleft()
                        if value is Thread.STOP:  # SENDING A STOP INTO THE QUEUE IS ALSO AN OPTION
                            self.keep_running = False
                        return value

                    try:
                        self.lock.wait()
                    except Exception, e:
                        pass
            else:
                while self.keep_running:
                    if self.queue:
                        value = self.queue.popleft()
                        if value is Thread.STOP:  # SENDING A STOP INTO THE QUEUE IS ALSO AN OPTION
                            self.keep_running = False
                        return value
                    elif Date.now() > till:
                        break

                    try:
                        self.lock.wait(till=till)
                    except Exception, e:
                        pass
                if self.keep_running:
                    return None

        _Log.note("queue stopped")
        return Thread.STOP


    def pop_all(self):
        """
        NON-BLOCKING POP ALL IN QUEUE, IF ANY
        """
        with self.lock:
            if not self.keep_running:
                return [Thread.STOP]
            if not self.queue:
                return []

            for v in self.queue:
                if v is Thread.STOP:  # SENDING A STOP INTO THE QUEUE IS ALSO AN OPTION
                    self.keep_running = False

            output = list(self.queue)
            self.queue.clear()
            return output

    def pop_one(self):
        """
        NON-BLOCKING POP IN QUEUE, IF ANY
        """
        with self.lock:
            if not self.keep_running:
                return [Thread.STOP]
            elif not self.queue:
                return None
            else:
                v =self.queue.pop()
                if v is Thread.STOP:  # SENDING A STOP INTO THE QUEUE IS ALSO AN OPTION
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


class AllThread(object):
    """
    RUN ALL ADDED FUNCTIONS IN PARALLEL, BE SURE TO HAVE JOINED BEFORE EXIT
    """

    def __init__(self):
        if not _Log:
            _late_import()
        self.threads = []

    def __enter__(self):
        return self

    # WAIT FOR ALL QUEUED WORK TO BE DONE BEFORE RETURNING
    def __exit__(self, type, value, traceback):
        self.join()

    def join(self):
        exceptions = []
        try:
            for t in self.threads:
                response = t.join()
                if "exception" in response:
                    exceptions.append(response["exception"])
        except Exception, e:
            _Log.warning("Problem joining", e)

        if exceptions:
            _Log.error("Problem in child threads", exceptions)


    def add(self, target, *args, **kwargs):
        """
        target IS THE FUNCTION TO EXECUTE IN THE THREAD
        """
        t = Thread.run(target.__name__, target, *args, **kwargs)
        self.threads.append(t)


class MainThread(object):
    def __init__(self):
        self.name = "Main Thread"
        self.id = thread.get_ident()
        self.children = []

    def add_child(self, child):
        self.children.append(child)

    def remove_child(self, child):
        try:
            self.children.remove(child)
        except Exception, _:
            pass

    def stop(self):
        """
        BLOCKS UNTIL ALL THREADS HAVE STOPPED
        """
        children = copy(self.children)
        for c in reversed(children):
            if c.name:
                _Log.note("Stopping thread {{name|quote}}", name=c.name)
            c.stop()
        for c in children:
            c.join()


MAIN_THREAD = MainThread()

ALL_LOCK = Lock("threads ALL_LOCK")
ALL = dict()
ALL[thread.get_ident()] = MAIN_THREAD


class Thread(object):
    """
    join() ENHANCED TO ALLOW CAPTURE OF CTRL-C, AND RETURN POSSIBLE THREAD EXCEPTIONS
    run() ENHANCED TO CAPTURE EXCEPTIONS
    """

    num_threads = 0
    STOP = "stop"
    TIMEOUT = "TIMEOUT"


    def __init__(self, name, target, *args, **kwargs):
        if not _Log:
            _late_import()
        self.id = -1
        self.name = name
        self.target = target
        self.end_of_thread = None
        self.synch_lock = Lock("response synch lock")
        self.args = args

        # ENSURE THERE IS A SHARED please_stop SIGNAL
        self.kwargs = copy(kwargs)
        self.kwargs["please_stop"] = self.kwargs.get("please_stop", Signal())
        self.please_stop = self.kwargs["please_stop"]

        self.thread = None
        self.stopped = Signal()
        self.cprofiler = None
        self.children = []

        if "parent_thread" in kwargs:
            del self.kwargs["parent_thread"]
            self.parent = kwargs["parent_thread"]
        else:
            self.parent = Thread.current()
            self.parent.add_child(self)


    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if isinstance(type, BaseException):
            self.please_stop.go()

        # TODO: AFTER A WHILE START KILLING THREAD
        self.join()
        self.args = None
        self.kwargs = None

    def start(self):
        if not _Log:
            _late_import()

        try:
            self.thread = thread.start_new_thread(Thread._run, (self, ))
            return self
        except Exception, e:
            _Log.error("Can not start thread", e)

    def stop(self):
        for c in copy(self.children):
            c.stop()
        self.please_stop.go()

    def add_child(self, child):
        self.children.append(child)

    def remove_child(self, child):
        try:
            self.children.remove(child)
        except Exception, e:
            _Log.error("not expected", e)

    def _run(self):
        if _Log.cprofiler:
            import cProfile
            _Log.note("starting cprofile for thread {{thread}}", thread=self.name)

            self.cprofiler = cProfile.Profile()
            self.cprofiler.enable()

        self.id = thread.get_ident()
        with ALL_LOCK:
            ALL[self.id] = self

        try:
            if self.target is not None:
                a, k, self.args, self.kwargs = self.args, self.kwargs, None, None
                response = self.target(*a, **k)
                with self.synch_lock:
                    self.end_of_thread = Dict(response=response)
        except Exception, e:
            with self.synch_lock:
                self.end_of_thread = Dict(exception=_Except.wrap(e))
            try:
                _Log.fatal("Problem in thread {{name|quote}}", name=self.name, cause=e)
            except Exception, f:
                sys.stderr.write("ERROR in thread: " + str(self.name) + " " + str(e) + "\n")
        finally:
            children = copy(self.children)
            for c in children:
                try:
                    c.stop()
                except Exception:
                    pass

            for c in children:
                try:
                    c.join()
                except Exception, _:
                    pass

            self.stopped.go()
            del self.target, self.args, self.kwargs
            with ALL_LOCK:
                del ALL[self.id]

            if self.cprofiler:
                import pstats

                if DEBUG:
                    _Log.note("Adding cprofile stats for thread {{thread|quote}}", thread=self.name)
                self.cprofiler.disable()
                _Log.cprofiler_stats.add(pstats.Stats(self.cprofiler))
                del self.cprofiler

    def is_alive(self):
        return not self.stopped

    def join(self, timeout=None, till=None):
        """
        RETURN THE RESULT {"response":r, "exception":e} OF THE THREAD EXECUTION (INCLUDING EXCEPTION, IF EXISTS)
        """
        if timeout is not None:
            if till is None:
                till = datetime.utcnow() + timedelta(seconds=timeout)
            else:
                _Log.error("Can not except both `timeout` and `till`")

        children = copy(self.children)
        for c in children:
            c.join(till=till)

        if till is None:
            while True:
                with self.synch_lock:
                    for i in range(10):
                        if self.stopped:
                            self.parent.remove_child(self)
                            if not self.end_of_thread.exception:
                                return self.end_of_thread.response
                            else:
                                _Log.error("Thread did not end well", cause=self.end_of_thread.exception)
                        self.synch_lock.wait(0.5)

                if DEBUG:
                    _Log.note("Waiting on thread {{thread|json}}", thread=self.name)
        else:
            self.stopped.wait_for_go(till=till)
            if self.stopped:
                self.parent.remove_child(self)
                if not self.end_of_thread.exception:
                    return self.end_of_thread.response
                else:
                    _Log.error("Thread did not end well", cause=self.end_of_thread.exception)
            else:
                from pyLibrary.debugs.logs import Except

                raise Except(type=Thread.TIMEOUT)

    @staticmethod
    def run(name, target, *args, **kwargs):
        if not _Log:
            _late_import()

        # ENSURE target HAS please_stop ARGUMENT
        if "please_stop" not in target.__code__.co_varnames:
            _Log.error("function must have please_stop argument for signalling emergency shutdown")

        Thread.num_threads += 1

        output = Thread(name, target, *args, **kwargs)
        output.start()
        return output

    @staticmethod
    def sleep(seconds=None, till=None, timeout=None, please_stop=None):

        if please_stop is not None or isinstance(till, Signal):
            if isinstance(till, Signal):
                please_stop = till
                till = MAX_DATETIME

            if seconds is not None:
                till = datetime.utcnow() + timedelta(seconds=seconds)
            elif timeout is not None:
                till = datetime.utcnow() + timedelta(seconds=timeout.seconds)
            elif till is None:
                till = MAX_DATETIME

            while not please_stop:
                time.sleep(1)
                if till < datetime.utcnow():
                    break
            return

        if seconds != None:
            time.sleep(seconds)
        elif till != None:
            if isinstance(till, datetime):
                duration = (till - datetime.utcnow()).total_seconds()
            else:
                duration = (till - datetime.utcnow()).total_seconds

            if duration > 0:
                try:
                    time.sleep(duration)
                except Exception, e:
                    raise e
        else:
            while True:
                time.sleep(10)


    @staticmethod
    def wait_for_shutdown_signal(
        please_stop=False,  # ASSIGN SIGNAL TO STOP EARLY
        allow_exit=False  # ALLOW "exit" COMMAND ON CONSOLE TO ALSO STOP THE APP
    ):
        """
        SLEEP UNTIL keyboard interrupt
        """
        if not isinstance(please_stop, Signal):
            please_stop = Signal()

        please_stop.on_go(lambda: thread.start_new_thread(lambda: MAIN_THREAD.stop(), ()))

        if Thread.current() != MAIN_THREAD:
            if not _Log:
                _late_import()
            _Log.error("Only the main thread can sleep forever (waiting for KeyboardInterrupt)")

        try:
            if allow_exit:
                _wait_for_exit(please_stop)
            else:
                _wait_for_interrupt(please_stop)
        except (KeyboardInterrupt, SystemExit), _:
            please_stop.go()
            _Log.alert("SIGINT Detected!  Stopping...")

        MAIN_THREAD.stop()

    @staticmethod
    def current():
        id = thread.get_ident()
        with ALL_LOCK:
            try:
                return ALL[id]
            except KeyError, e:
                return MAIN_THREAD


class Signal(object):
    """
    SINGLE-USE THREAD SAFE SIGNAL

    go() - ACTIVATE SIGNAL (DOES NOTHING IF SIGNAL IS ALREADY ACTIVATED)
    wait_for_go() - PUT THREAD IN WAIT STATE UNTIL SIGNAL IS ACTIVATED
    is_go() - TEST IF SIGNAL IS ACTIVATED, DO NOT WAIT (you can also check truthiness)
    on_go() - METHOD FOR OTHER THREAD TO RUN WHEN ACTIVATING SIGNAL
    """

    def __init__(self):
        self.lock = Lock()
        self._go = False
        self.job_queue = []

    def __str__(self):
        return str(self._go)

    def __bool__(self):
        with self.lock:
            return self._go

    def __nonzero__(self):
        with self.lock:
            return self._go


    def wait_for_go(self, timeout=None, till=None):
        """
        PUT THREAD IN WAIT STATE UNTIL SIGNAL IS ACTIVATED
        """
        with self.lock:
            while not self._go:
                self.lock.wait(timeout=timeout, till=till)

            return True

    def go(self):
        """
        ACTIVATE SIGNAL (DOES NOTHING IF SIGNAL IS ALREADY ACTIVATED)
        """
        with self.lock:
            if self._go:
                return

            self._go = True
            jobs = self.job_queue
            self.job_queue = []
            self.lock.notify_all()

        for j in jobs:
            try:
                j()
            except Exception, e:
                _Log.warning("Trigger on Signal.go() failed!", e)

    def is_go(self):
        """
        TEST IF SIGNAL IS ACTIVATED, DO NOT WAIT
        """
        with self.lock:
            return self._go

    def on_go(self, target):
        """
        RUN target WHEN SIGNALED
        """
        with self.lock:
            if self._go:
                target()
            else:
                self.job_queue.append(target)


class ThreadedQueue(Queue):
    """
    TODO: Check that this queue is not dropping items at shutdown
    DISPATCH TO ANOTHER (SLOWER) queue IN BATCHES OF GIVEN size
    """

    def __init__(
        self,
        name,
        queue,  # THE SLOWER QUEUE
        batch_size=None,  # THE MAX SIZE OF BATCHES SENT TO THE SLOW QUEUE
        max_size=None,  # SET THE MAXIMUM SIZE OF THE QUEUE, WRITERS WILL BLOCK IF QUEUE IS OVER THIS LIMIT
        period=None,  # MAX TIME BETWEEN FLUSHES TO SLOWER QUEUE
        silent=False  # WRITES WILL COMPLAIN IF THEY ARE WAITING TOO LONG
    ):
        if not _Log:
            _late_import()

        batch_size = coalesce(batch_size, int(coalesce(max_size, 0) / 2), 900)
        max_size = coalesce(max_size, batch_size * 2)  # REASONABLE DEFAULT
        period = coalesce(period, SECOND)
        bit_more_time = 5 * SECOND

        Queue.__init__(self, name=name, max=max_size, silent=silent)

        def worker_bee(please_stop):
            def stopper():
                self.add(Thread.STOP)

            please_stop.on_go(stopper)

            _buffer = []
            next_time = Date.now() + period  # THE TIME WE SHOULD DO A PUSH

            while not please_stop:
                try:
                    if not _buffer:
                        item = self.pop()
                        now = Date.now()

                        if item is Thread.STOP:
                            queue.extend(_buffer)
                            please_stop.go()
                            break
                        elif item is not None:
                            _buffer.append(item)

                        next_time = now + period  # NO NEED TO SEND TOO EARLY
                        continue

                    item = self.pop(till=next_time)
                    now = Date.now()

                    if item is Thread.STOP:
                        queue.extend(_buffer)
                        please_stop.go()
                        break
                    elif item is not None:
                        _buffer.append(item)

                except Exception, e:
                    _Log.warning(
                        "Unexpected problem",
                        name=name,
                        cause=e
                    )

                try:
                    if len(_buffer) >= batch_size or now > next_time:
                        next_time = now + period
                        if _buffer:
                            queue.extend(_buffer)
                            _buffer = []
                            # A LITTLE MORE TIME TO FILL THE NEXT BUFFER
                            now = Date.now()
                            if now > next_time:
                                next_time = now + bit_more_time

                except Exception, e:
                    _Log.warning(
                        "Problem with {{name}} pushing {{num}} items to data sink",
                        name=name,
                        num=len(_buffer),
                        cause=e
                    )

            if _buffer:
                # ONE LAST PUSH, DO NOT HAVE TIME TO DEAL WITH ERRORS
                queue.extend(_buffer)

        self.thread = Thread.run("threaded queue for " + name, worker_bee, parent_thread=self)

    def add(self, value, timeout=None):
        with self.lock:
            self._wait_for_queue_space(timeout=timeout)
            if self.keep_running:
                self.queue.append(value)
        return self

    def extend(self, values):
        with self.lock:
            # ONCE THE queue IS BELOW LIMIT, ALLOW ADDING MORE
            self._wait_for_queue_space()
            if self.keep_running:
                self.queue.extend(values)
        return self


    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        self.add(Thread.STOP)
        if isinstance(b, BaseException):
            self.thread.please_stop.go()
        self.thread.join()

    def stop(self):
        self.add(Thread.STOP)
        self.thread.join()



def _wait_for_exit(please_stop):
    """
    /dev/null SPEWS INFINITE LINES, DO NOT POLL AS OFTEN
    """
    cr_count = 0  # COUNT NUMBER OF BLANK LINES

    while not please_stop:
        # if DEBUG:
        #     Log.note("inside wait-for-shutdown loop")
        if cr_count > 30:
            Thread.sleep(seconds=3, please_stop=please_stop)
        try:
            line = sys.stdin.readline()
        except Exception, e:
            if "Bad file descriptor" in e:
                _wait_for_interrupt(please_stop)
                break

        # if DEBUG:
        #     Log.note("read line {{line|quote}}, count={{count}}", line=line, count=cr_count)
        if line == "":
            cr_count += 1
        else:
            cr_count = -1000000  # NOT /dev/null

        if strings.strip(line) == "exit":
            _Log.alert("'exit' Detected!  Stopping...")
            return


def _wait_for_interrupt(please_stop):
    while not please_stop:
        if DEBUG:
            _Log.note("inside wait-for-shutdown loop")
        try:
            Thread.sleep(please_stop=please_stop)
        except Exception, _:
            pass



class Till(Signal):
    """
    MANAGE THE TIMEOUT LOGIC
    """
    def __init__(self, till=None, timeout=None, seconds=None):
        Signal.__init__(self)

        timers = []

        def go():
            self.go()
            for t in timers:
                t.cancel()

        if isinstance(till, Date):
            t = threading.Timer((till - Date.now()).seconds, go)
            t.start()
            timers.append(t)
        if timeout:
            t = threading.Timer(timeout.seconds, go)
            t.start()
            timers.append(t)
        if seconds:
            t = threading.Timer(seconds, go)
            t.start()
            timers.append(t)
        if isinstance(till, Signal):
            till.on_go(go)
