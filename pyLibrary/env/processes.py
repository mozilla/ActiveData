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


import subprocess

try:
    from _subprocess import CREATE_NEW_PROCESS_GROUP
    from signal import CTRL_C_EVENT
    is_windows = True

    flags = CREATE_NEW_PROCESS_GROUP
except Exception, e:
    from signal import SIGINT
    is_windows = False
    flags = None


class Process(object):
    """
    OS INDEPENDENT PROCESS CREATION
    WILL FORCE stdin stdout AND stderr TO BE LINE BASED (
    OPTIMIZED FOR LINES OF JSON
    """

    def __init__(self, args, please_stop=None):
        if please_stop!=None:
            def stopper():
                self.stop()
                self.join()

            please_stop.on_go(stopper)

        if is_windows:
            self.proc = subprocess.Popen(
                args,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=-1,
                creationflags=flags
            )
        else:
            self.proc = subprocess.Popen(
                args,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=-1
            )

    def readline(self):
        return self.proc.stdout.readline()

    def writeline(self, line):
        return self.proc.stdin.writelines([line])

    def stop(self):
        if is_windows:
            self.proc.send_signal(CTRL_C_EVENT)
        else:
            self.proc.send_signal(SIGINT)

    def join(self):
        self.proc.wait()
