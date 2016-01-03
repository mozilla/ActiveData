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
from pyLibrary.debugs.logs import Log
from pyLibrary.thread.threads import Queue, Thread, Signal, Lock

DEBUG = True


class Process(object):
    def __init__(self, name, params, cwd=None, env=None):
        self.name = name
        self.service_stopped = Signal()
        self.stdin = Queue("stdin for process " + convert.string2quote(name), silent=True)
        self.stdout = Queue("stdout for process " + convert.string2quote(name), silent=True)
        self.stderr = Queue("stderr for process " + convert.string2quote(name), silent=True)

        try:
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
            self.stopper.on_go(lambda: service.kill())
            self.thread_locker = Lock()
            self.child_threads = [
                Thread.run(self.name + " waiter", self._monitor, parent_thread=self),
                Thread.run(self.name + " stdin", self._writer, service.stdin, self.stdin, please_stop=self.stopper, parent_thread=self),
                Thread.run(self.name + " stdout", self._reader, service.stdout, self.stdout, please_stop=self.stopper, parent_thread=self),
                Thread.run(self.name + " stderr", self._reader, service.stderr, self.stderr, please_stop=self.stopper, parent_thread=self),
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
        self.service_stopped.wait_for_go()
        with self.thread_locker:
            child_threads, self.child_threads = self.child_threads, []
        for c in child_threads:
            c.join()

    def remove_child(self, child):
        with self.thread_locker:
            self.child_threads.remove(child)

    def _monitor(self, please_stop):
        self.service.wait()
        if DEBUG:
            Log.alert("{{name}} stopped with returncode={{returncode}}", name=self.name, returncode=self.service.returncode)
        self.stdin.add(Thread.STOP)
        self.service_stopped.go()

    def _reader(self, pipe, recieve, please_stop):
        try:
            while not please_stop:
                line = pipe.readline()
                if self.service.returncode is not None:
                    return

                recieve.add(line)
                if DEBUG:
                    Log.note("FROM {{process}}: {{line}}", process=self.name, line=line.rstrip())
        finally:
            pipe.close()

    def _writer(self, pipe, send, please_stop):
        while not please_stop:
            line = send.pop()
            if line:
                pipe.write(line + "\n")
        pipe.close()


