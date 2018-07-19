# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from mo_logs import Log
from mo_threads import Till, Thread, Lock
from mo_times import Date, SECOND

METRIC_DECAY_RATE = 0.9  # PER-SECOND DECAY RATE FOR REPORTING REQUEST RATE
METRIC_REPORT_PERIOD = 10 * SECOND


class RateLogger(object):

    def __init__(self, name):
        self.name = name
        self.lock = Lock("rate locker")
        self.request_rate = 0.0
        self.last_request = Date.now()

        Thread.run("rate logger", self._daemon)

    def add(self, timestamp):
        with self.lock:
            decay = METRIC_DECAY_RATE ** (timestamp - self.last_request).seconds
            self.request_rate = decay*self.request_rate + 1
            self.last_request = timestamp

    def _daemon(self, please_stop):
        while not please_stop:
            timestamp = Date.now()
            with self.lock:
                decay = METRIC_DECAY_RATE ** (timestamp - self.last_request).seconds
                request_rate = self.request_rate = decay * self.request_rate
                self.last_request = timestamp

            Log.note("{{name}} request rate: {{rate|round(places=2)}} requests per second", name=self.name, rate=request_rate)
            (please_stop | Till(seconds=METRIC_REPORT_PERIOD.seconds)).wait()

