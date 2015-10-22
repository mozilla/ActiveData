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

from pyLibrary.debugs.logs import Log
from pyLibrary.thread.threads import Queue, Thread, Signal


DEBUG = True


class Process(object):
    def __init__(self, name, params, cwd=None, env=None):
        self.name = name
        self.service_stopped = Signal()
        self.stdin = Queue("stdin")
        self.stdout = Queue("stdout")
        self.stderr = Queue("stderr")

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
            Thread.run(self.name + " waiter", self.waiter, self)
            Thread.run(self.name + " stdin", self.writer, service.stdin, self.stdin, please_stop=self.stopper)
            Thread.run(self.name + " stdout", self.reader, service.stdout, self.stdout, please_stop=self.stopper)
            Thread.run(self.name + " stderr", self.reader, service.stderr, self.stderr, please_stop=self.stopper)
        except Exception, e:
            Log.error("Can not call", e)

    def stop(self):
        self.stopper.go()
        self.stdin.add("exit")  # ONE MORE SEND

    def join(self):
        self.service_stopped.wait_for_go()

    def waiter(self, please_stop):
        self.service.wait()
        if DEBUG:
            Log.alert("{{name}} stopped with returncode={{returncode}}", name=self.name, returncode=self.service.returncode)
        self.service_stopped.go()

    def reader(self, pipe, recieve, please_stop):
        while not please_stop:
            line = pipe.readline()
            if self.service.returncode is not None:
                return

            recieve.add(line)
            Log.note("FROM {{process}}: {{line}}", process=self.name, line=line.rstrip())
        pipe.close()

    def writer(self, pipe, send, please_stop):
        while not please_stop:
            line = send.pop()
            if line:
                pipe.write(line + "\n")
        pipe.close()


