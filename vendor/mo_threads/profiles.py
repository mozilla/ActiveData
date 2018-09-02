# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import cProfile
import pstats
from datetime import datetime

from mo_future import iteritems
from mo_logs import Log
from mo_threads import Queue

ENABLED = False
FILENAME = "profile.tab"

cprofiler_stats = Queue("cprofiler stats")


class CProfiler(object):
    """
    cProfiler CONTEXT MANAGER WRAPPER
    """

    def __init__(self):
        self.cprofiler = None

    def __enter__(self):
        if ENABLED:
            Log.note("starting cprofile")
            self.cprofiler = cProfile.Profile()
            self.cprofiler.enable()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cprofiler is not None:
            self.cprofiler.disable()
            cprofiler_stats.add(pstats.Stats(self.cprofiler))
            del self.cprofiler
            Log.note("done cprofile")

    def enable(self):
        if self.cprofiler is not None:
            return self.cprofiler.enable()

    def disable(self):
        if self.cprofiler is not None:
            return self.cprofiler.disable()


def write_profiles(main_thread_profile):
    from pyLibrary import convert
    from mo_files import File

    cprofiler_stats.add(main_thread_profile)
    stats = cprofiler_stats.pop_all()

    Log.note("aggregating {{num}} profile stats", num=len(stats))
    acc = stats[0]
    for s in stats[1:]:
        acc.add(s)

    stats = [
        {
            "num_calls": d[1],
            "self_time": d[2],
            "total_time": d[3],
            "self_time_per_call": d[2] / d[1],
            "total_time_per_call": d[3] / d[1],
            "file": (f[0] if f[0] != "~" else "").replace("\\", "/"),
            "line": f[1],
            "method": f[2].lstrip("<").rstrip(">")
        }
        for f, d, in iteritems(acc.stats)
    ]
    stats_file = File(FILENAME, suffix=convert.datetime2string(datetime.now(), "_%Y%m%d_%H%M%S"))
    stats_file.write(convert.list2tab(stats))




