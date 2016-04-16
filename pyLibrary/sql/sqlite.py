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

import sqlite3

from pyLibrary.debugs import exceptions
from pyLibrary.debugs.exceptions import Except, extract_stack, ERROR
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict
from pyLibrary.thread.threads import Queue, Signal, Thread
from pyLibrary.times.timer import Timer

DEBUG = True

class Sqlite(object):

    canonical = None

    def __init__(self, db=None):
        """
        :param db:  Optional, wrap a sqlite db in a thread
        :return: Multithread save database
        """
        self.db = None
        self.queue = Queue("sql commands")   # HOLD (command, result, signal) PAIRS
        self.worker = Thread.run("sqlite db thread", self._worker)
        self.get_trace = DEBUG

    def execute(self, command):
        """
        COMMANDS WILL BE EXECUTED IN THE ORDER THEY ARE GIVEN
        BUT CAN INTERLEAVE WITH OTHER TREAD COMMANDS
        :param command: COMMAND FOR SQLITE
        :return: None
        """
        if self.get_trace:
            trace = extract_stack(1)
        else:
            trace = None
        self.queue.add((command, None, None, trace))

    def query(self, command):
        """
        WILL STALL CALLING THREAD UNTIL THE command IS COMPLETED
        :param command: COMMAND FOR SQLITE
        :return: list OF RESULTS
        """
        signal = Signal()
        result = Dict()
        self.queue.add((command, result, signal, None))
        signal.wait_for_go()
        if result.exception:
            Log.error("Problem with Sqlite call", cause=result.exception)
        return result

    def _worker(self, please_stop):
        if Sqlite.canonical:
            self.db = Sqlite.canonical
        else:
            self.db = sqlite3.connect(':memory:')

        try:
            while not please_stop:
                if DEBUG:
                    Log.note("begin pop")
                command, result, signal, trace = self.queue.pop()
                if DEBUG:
                    Log.note("done pop")

                if DEBUG:
                    Log.note("Running command\n{{command|indent}}", command=command)
                with Timer("Run command", debug=DEBUG):
                    if signal is not None:
                        try:
                            curr = self.db.execute(command)
                            result.meta.format = "table"
                            result.data = curr.fetchall()
                        except Exception, e:
                            e=Except.wrap(e)
                            result.exception = Except(ERROR, "Problem with\n{{command|indent}}", command=command, cause=e)
                        finally:
                            signal.go()
                    else:
                        try:
                            self.db.execute(command)
                        except Exception, e:
                            e = Except.wrap(e)
                            e.cause = Except(
                                type=ERROR,
                                template="Bad call to Sqlite",
                                trace=trace
                            )
                            Log.warning("Failure to execute", cause=e)

        except Exception, e:
            Log.error("Problem with sql thread", e)
        finally:
            self.db.close()
