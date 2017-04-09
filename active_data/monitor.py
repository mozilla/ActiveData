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

from mo_dots import coalesce
from mo_logs import constants, startup
from mo_logs import Log
from mo_files import File
from pyLibrary.meta import use_settings
from pyLibrary.thread.multiprocess import Process
from mo_threads import Signal, Thread
from mo_threads import Till
from mo_times.dates import Date
from mo_times.durations import DAY


class Scheduler(object):

    @override
    def __init__(self, please_stop, kwargs=None):
        self.please_stop = please_stop
        self.jobs = kwargs.jobs

        for j in kwargs.jobs:
            j.next_run_time = next_run(j)
            j.logger = Log.start_process(j.name)



    def trigger_job(self):
        while self.please_stop:
            now = Date.now()
            next = now+DAY

            for j in self.jobs:
                if j.next_run_time < now:
                    j.next_run_time = next_run(j)
                    self.run_job(j)

                next = Date.min(next, j.next_run_time)

            (Till(till=next) | self.please_stop).wait()

    def run_job(self, job):
        process = Process(
            name=job.name,
            params=job.command,
            cwd=job.directory,
            env=job.environment
        )

        # DIRECT OUTPUT TO FILES
        self.add_file(process.stdout, coalesce(job.stdout, File.newInstance(self.settings.log.directory, job.name)))


def next_run(job):
    if job.settings.start_next:
        formula = next_run(job.settings.start_next)
    elif job.settings.start_interval:
        formula = "now|"+job.settings.start_interval+"+"+job.settings.start_interval
    else:
        Log.error("Expecting `start_next` or `start_interval` for job {{job}}", job=job.name)

    now = Date.now()
    next = Date(formula)
    if next < now:
        Log.error("{{formula|quote}} does not calculate a future date")
    return next


def main():
    try:
        config = startup.read_settings()
        constants.set(config.constants)
        Log.start(config.debug)
        please_stop = Signal("main stop signal")
        Thread.wait_for_shutdown_signal(please_stop)
    except Exception, e:
        Log.error("Problem with etl", cause=e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()

