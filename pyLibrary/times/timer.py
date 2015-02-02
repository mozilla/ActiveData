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

from datetime import timedelta
from time import clock

from pyLibrary.dot import nvl, Dict
from pyLibrary.dot import wrap
from pyLibrary.debugs.logs import Log


class Timer(object):
    """
    USAGE:
    with Timer("doing hard time"):
        something_that_takes_long()
    OUTPUT:
        doing hard time took 45.468 sec

    param - USED WHEN LOGGING
    debug - SET TO False TO DISABLE THIS TIMER
    """

    def __init__(self, description, param=None, debug=True):
        self.template = description
        self.param = nvl(wrap(param), Dict())
        self.debug = debug
        self.interval = -1

    def __enter__(self):
        if self.debug:
            Log.note("Timer start: " + self.template, self.param, stack_depth=1)
            self.start = clock()

        return self

    def __exit__(self, type, value, traceback):
        if self.debug:
            self.end = clock()
            self.interval = self.end - self.start
            param = wrap(self.param)
            param.duration = timedelta(seconds=self.interval)
            Log.note("Timer end  : " + self.template + " (took {{duration}})", self.param, stack_depth=1)

    @property
    def duration(self):
        return timedelta(seconds=self.interval)
