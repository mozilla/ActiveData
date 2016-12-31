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

import sys
import thread
import types
from collections import deque
from copy import copy
from datetime import datetime, timedelta
from time import sleep

from pyLibrary import strings
from pyLibrary.debugs.exceptions import Except, suppress_exception
from pyLibrary.debugs.profiles import CProfiler
from pyDots import coalesce, Data, unwraplist, Null
from pyLibrary.thread.lock import Lock
from pyLibrary.thread.signal import Signal
from pyLibrary.thread.till import Till
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import SECOND

_convert = None
_Except = None
_Log = None
DEBUG = False

MAX_DATETIME = datetime(2286, 11, 20, 17, 46, 39)
DEFAULT_WAIT_TIME = timedelta(minutes=10)

datetime.strptime('2012-01-01', '%Y-%m-%d')  # http://bugs.python.org/issue7980


def _late_import():
    global _convert
    global _Except
    global _Log

    from pyLibrary.debugs.exceptions import Except as _Except
    from pyLibrary.debugs.logs import Log as _Log

    _ = _convert
    _ = _Except
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
                if value is not None and value is not Thread.STOP:
                    yield value
            except Exception, e:
                _Log.warning("Tell me about what happened here", e)

        _Log.note("queue iterator is done")

    def add(self, value, timeout=None):
        if not self.keep_running and not self.allow_add_after_close:
            _Log.error("Do not add to closed queue")

        with self.lock:
            if value is Thread.STOP:
                # INSIDE THE lock SO THAT EXITING WILL RELEASE wait()
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
                        if v is Thread.STOP:
                            self.keep_running = False
                            continue
                        if v not in self.queue:
                            self.queue.append(v)
                else:
                    for v in values:
                        if v is Thread.STOP:
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

        while self.keep_running and len(self.queue) > self.max:
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
            return any(r != Thread.STOP for r in self.queue)

    def pop(self, till=None):
        """
        WAIT FOR NEXT ITEM ON THE QUEUE
        RETURN Thread.STOP IF QUEUE IS CLOSED
        RETURN None IF till IS REACHED AND QUEUE IS STILL EMPTY

        :param till:  A `Signal` to stop waiting and return None
        :return:  A value, or a Thread.STOP or None
        """
        if till is not None and not isinstance(till, Signal):
            _Log.error("expecting a signal")

        with self.lock:
            while self.keep_running:
                if self.queue:
                    value = self.queue.popleft()
                    return value
                self.lock.wait(till=till)
        if DEBUG or not self.silent:
            _Log.note(self.name + " queue stopped")
        return Thread.STOP

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
            return output + [Thread.STOP]

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
        self.timers = None

    def add_child(self, child):
        self.children.append(child)

    def remove_child(self, child):
        with suppress_exception:
            self.children.remove(child)

    def stop(self):
        """
        BLOCKS UNTIL ALL THREADS HAVE STOPPED
        """
        join_errors = []

        children = copy(self.children)
        for c in reversed(children):
            if DEBUG and c.name:
                _Log.note("Stopping thread {{name|quote}}", name=c.name)
            try:
                c.stop()
            except Exception, e:
                join_errors.append(e)

        for c in children:
            if DEBUG and c.name:
                _Log.note("Joining on thread {{name|quote}}", name=c.name)
            try:
                c.join()
            except Exception, e:
                join_errors.append(e)

            if DEBUG and c.name:
                _Log.note("Done join on thread {{name|quote}}", name=c.name)

        if join_errors:
            _Log.error("Problem while stopping {{name|quote}}", name=self.name, cause=unwraplist(join_errors))

        self.timers.stop()
        self.timers.join()

        if DEBUG:
            _Log.note("Thread {{name|quote}} now stopped", name=self.name)



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
        self.kwargs["please_stop"] = self.kwargs.get("please_stop", Signal("please_stop for "+self.name))
        self.please_stop = self.kwargs["please_stop"]

        self.thread = None
        self.stopped = Signal("stopped signal for "+self.name)
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
            if DEBUG and c.name:
                _Log.note("Stopping thread {{name|quote}}", name=c.name)
            c.stop()
        self.please_stop.go()

        if DEBUG:
            _Log.note("Thread {{name|quote}} got request to stop", name=self.name)

    def add_child(self, child):
        self.children.append(child)

    def remove_child(self, child):
        try:
            self.children.remove(child)
        except Exception, e:
            # happens when multiple joins on same thread
            pass

    def _run(self):
        with CProfiler():

            self.id = thread.get_ident()
            with ALL_LOCK:
                ALL[self.id] = self

            try:
                if self.target is not None:
                    a, k, self.args, self.kwargs = self.args, self.kwargs, None, None
                    response = self.target(*a, **k)
                    with self.synch_lock:
                        self.end_of_thread = Data(response=response)
                else:
                    with self.synch_lock:
                        self.end_of_thread = Null
            except Exception, e:
                with self.synch_lock:
                    self.end_of_thread = Data(exception=_Except.wrap(e))
                if self not in self.parent.children:
                    # THREAD FAILURES ARE A PROBLEM ONLY IF NO ONE WILL BE JOINING WITH IT
                    try:
                        _Log.fatal("Problem in thread {{name|quote}}", name=self.name, cause=e)
                    except Exception:
                        sys.stderr.write(b"ERROR in thread: " + str(self.name) + b" " + str(e) + b"\n")
            finally:
                try:
                    children = copy(self.children)
                    for c in children:
                        try:
                            c.stop()
                        except Exception, e:
                            _Log.warning("Problem stopping thread {{thread}}", thread=c.name, cause=e)

                    for c in children:
                        try:
                            c.join()
                        except Exception, e:
                            _Log.warning("Problem joining thread {{thread}}", thread=c.name, cause=e)

                    self.stopped.go()
                    if DEBUG:
                        _Log.note("thread {{name|quote}} stopping", name=self.name)
                    del self.target, self.args, self.kwargs
                    with ALL_LOCK:
                        del ALL[self.id]

                except Exception, e:
                    if DEBUG:
                        _Log.warning("problem with thread {{name|quote}}", cause=e, name=self.name)
                finally:
                    self.stopped.go()
                    if DEBUG:
                        _Log.note("thread {{name|quote}} is done", name=self.name)

    def is_alive(self):
        return not self.stopped

    def join(self, till=None):
        """
        RETURN THE RESULT {"response":r, "exception":e} OF THE THREAD EXECUTION (INCLUDING EXCEPTION, IF EXISTS)
        """
        children = copy(self.children)
        for c in children:
            c.join(till=till)

        if DEBUG:
            _Log.note("{{parent|quote}} waiting on thread {{child|quote}}", parent=Thread.current().name, child=self.name)
        (self.stopped | till).wait()
        if self.stopped:
            self.parent.remove_child(self)
            if not self.end_of_thread.exception:
                return self.end_of_thread.response
            else:
                _Log.error("Thread {{name|quote}} did not end well", name=self.name, cause=self.end_of_thread.exception)
        else:
            from pyLibrary.debugs.exceptions import Except

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
    def wait_for_shutdown_signal(
        please_stop=False,  # ASSIGN SIGNAL TO STOP EARLY
        allow_exit=False  # ALLOW "exit" COMMAND ON CONSOLE TO ALSO STOP THE APP
    ):
        """
        FOR USE BY PROCESSES NOT EXPECTED TO EVER COMPLETE UNTIL EXTERNAL
        SHUTDOWN IS REQUESTED

        SLEEP UNTIL keyboard interrupt, OR please_stop, OR "exit"

        :param please_stop:
        :param allow_exit:
        :return:
        """
        if not isinstance(please_stop, Signal):
            please_stop = Signal()

        please_stop.on_go(lambda: thread.start_new_thread(_stop_main_thread, ()))

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
            _Log.alert("SIGINT Detected!  Stopping...")
        finally:
            please_stop.go()

    @staticmethod
    def current():
        id = thread.get_ident()
        with ALL_LOCK:
            try:
                return ALL[id]
            except KeyError:
                return MAIN_THREAD


def _stop_main_thread():
    try:
        MAIN_THREAD.stop()
    except Exception, e:
        e = Except.wrap(e)
        _Log.warning("Problem with threads", cause=e)
    sys.exit(0)



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
        period = coalesce(period, SECOND)
        bit_more_time = 5 * SECOND

        Queue.__init__(self, name=name, max=max_size, silent=silent)

        def worker_bee(please_stop):
            def stopper():
                self.add(Thread.STOP)

            please_stop.on_go(stopper)

            _buffer = []
            _post_push_functions = []
            next_push = Date.now() + period  # THE TIME WE SHOULD DO A PUSH

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
                        items = [item] + self.pop_all()  # PLEASE REMOVE
                        now = Date.now()
                        next_push = now + period
                    else:
                        item = self.pop(till=Till(till=next_push))
                        items = [item]+self.pop_all()  # PLEASE REMOVE
                        now = Date.now()

                    for item in items:  # PLEASE REMOVE
                        if item is Thread.STOP:
                            push_to_queue()
                            please_stop.go()
                            break
                        elif isinstance(item, types.FunctionType):
                            _post_push_functions.append(item)
                        elif item is not None:
                            _buffer.append(item)

                except Exception, e:
                    e = Except.wrap(e)
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
                    if len(_buffer) >= batch_size or now > next_push:
                        next_push = now + period
                        if _buffer:
                            push_to_queue()
                            # A LITTLE MORE TIME TO FILL THE NEXT BUFFER
                            now = Date.now()
                            next_push = max(next_push, now + bit_more_time)

                except Exception, e:
                    e = Except.wrap(e)
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

    please_stop.on_go(_interrupt_main_safely)

    while not please_stop:
        # if DEBUG:
        #     Log.note("inside wait-for-shutdown loop")
        if cr_count > 30:
            (Till(seconds=3)|please_stop).wait()
        try:
            line = sys.stdin.readline()
        except Exception, e:
            Except.wrap(e)
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
    if DEBUG:
        _Log.note("inside wait-for-shutdown loop")
    while not please_stop:
        with suppress_exception:
            sleep(1)


def _interrupt_main_safely():
    try:
        thread.interrupt_main()
    except KeyboardInterrupt:
        # WE COULD BE INTERRUPTING SELF
        pass


MAIN_THREAD = MainThread()

ALL_LOCK = Lock("threads ALL_LOCK")
ALL = dict()
ALL[thread.get_ident()] = MAIN_THREAD

MAIN_THREAD.timers = Thread.run("timers", Till.daemon)
MAIN_THREAD.children.remove(MAIN_THREAD.timers)
