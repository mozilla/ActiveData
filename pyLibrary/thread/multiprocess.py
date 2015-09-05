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


DEBUG=True

class Process(object):

    def __init__(self, name, params, cwd=None):
        self.name = name
        self.service_stopped = Signal()
        self.send = Queue("send")
        self.recieve = Queue("recieve")

        try:
            self.service = service = subprocess.Popen(
                params,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=-1,
                cwd=cwd
            )

            self.stopper = Signal()
            self.stopper.on_go(lambda: service.kill())
            Thread.run(self.name+" waiter", waiter, self)
            Thread.run(self.name+" stdout", reader, service.stdout, self.recieve, please_stop=self.stopper)
            Thread.run(self.name+" stderr", reader, service.stderr, self.recieve, please_stop=self.stopper)
            Thread.run(self.name+" stdin", writer, service.stdin, self.recieve, please_stop=self.stopper)
        except Exception, e:
            Log.error("Can not call", e)

    def stop(self):
        self.stopper.go()
        self.send.add("exit")

    def join(self):
        self.service_stopped.wait_for_go()


def waiter(this, please_stop):
    this.service.wait()
    if DEBUG:
        Log.alert("{{name}} stopped", name=this.name)
    this.service_stopped.go()

def reader(stdout, recieve, please_stop):
    while not please_stop:
        line = stdout.readline()
        if line:
            recieve.add(line)
            Log.note("FROM PROCESS: {{line}}", line=line.rstrip())
        else:
            Thread.sleep(1)
    stdout.close()


def writer(stdin, send, please_stop):
    while not please_stop:
        line = send.pop()
        if line:
            stdin.write(line+"\n")
    stdin.close()


