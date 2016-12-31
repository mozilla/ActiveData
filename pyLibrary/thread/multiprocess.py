# encoding: utf-8
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

import subprocess

from pyLibrary import convert
from pyLibrary.debugs.exceptions import Except
from pyLibrary.debugs.logs import Log
from pyLibrary.thread.threads import Queue, Thread, Signal, Lock

DEBUG = True


class Process(object):
    def __init__(self, name, params, cwd=None, env=None, debug=False):
        self.name = name
        self.service_stopped = Signal("stopped signal for " + convert.string2quote(name))
        self.stdin = Queue("stdin for process " + convert.string2quote(name), silent=True)
        self.stdout = Queue("stdout for process " + convert.string2quote(name), silent=True)
        self.stderr = Queue("stderr for process " + convert.string2quote(name), silent=True)

        try:
            self.debug=debug or DEBUG
            self.service = service = subprocess.Popen(
                params,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=-1,
                cwd=cwd,
                env=env
            )

            self.stopper = Signal()
            self.stopper.on_go(self._kill)
            self.thread_locker = Lock()
            self.children = [
                Thread.run(self.name + " waiter", self._monitor, parent_thread=self),
                Thread.run(self.name + " stdin", self._writer, service.stdin, self.stdin, please_stop=self.stopper, parent_thread=self),
                Thread.run(self.name + " stdout", self._reader, service.stdout, self.stdout, please_stop=self.stopper, parent_thread=self),
                # Thread.run(self.name + " stderr", self._reader, service.stderr, self.stderr, please_stop=self.stopper, parent_thread=self),
            ]
        except Exception, e:
            Log.error("Can not call", e)

    def stop(self):
        self.stdin.add("exit")  # ONE MORE SEND
        self.stopper.go()
        self.stdin.add(Thread.STOP)
        self.stdout.add(Thread.STOP)
        self.stderr.add(Thread.STOP)

    def join(self):
        self.service_stopped.wait()
        with self.thread_locker:
            child_threads, self.children = self.children, []
        for c in child_threads:
            c.join()

    def remove_child(self, child):
        with self.thread_locker:
            self.children.remove(child)

    @property
    def pid(self):
        return self.service.pid

    def _monitor(self, please_stop):
        self.service.wait()
        if self.debug:
            Log.alert("{{name}} stopped with returncode={{returncode}}", name=self.name, returncode=self.service.returncode)
        self.stdin.add(Thread.STOP)
        self.service_stopped.go()

    def _reader(self, pipe, recieve, please_stop):
        try:
            while not please_stop:
                line = pipe.readline()
                if self.service.returncode is not None:
                    # GRAB A FEW MORE LINES
                    for i in range(100):
                        try:
                            line = pipe.readline()
                            if line:
                                recieve.add(line)
                                if self.debug:
                                    Log.note("FROM {{process}}: {{line}}", process=self.name, line=line.rstrip())
                        except Exception:
                            break
                    return

                recieve.add(line)
                if self.debug:
                    Log.note("FROM {{process}}: {{line}}", process=self.name, line=line.rstrip())
        finally:
            pipe.close()

    def _writer(self, pipe, send, please_stop):
        while not please_stop:
            line = send.pop()
            if line == Thread.STOP:
                please_stop.go()
                break

            if line:
                pipe.write(line + "\n")
        pipe.close()

    def _kill(self):
        try:
            self.service.kill()
        except Exception, e:
            ee = Except.wrap(e)
            if 'The operation completed successfully' in ee:
                return
            if 'No such process' in ee:
                return

            Log.warning("Failure to kill process {{process|quote}}", process=self.name, cause=ee)
