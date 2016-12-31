# encoding: utf-8
#
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

import logging

from pyLibrary.debugs.exceptions import suppress_exception
from pyLibrary.debugs.log_usingThreadedStream import TextLog_usingThreadedStream, time_delta_pusher
from pyLibrary.debugs.logs import Log
from pyLibrary.debugs.text_logs import TextLog
from pyDots import unwrap
from pyLibrary.thread import threads
from pyLibrary.thread.threads import Thread


# WRAP PYTHON CLASSIC logger OBJECTS
class TextLog_usingLogger(TextLog):
    def __init__(self, settings):
        self.logger = logging.Logger("unique name", level=logging.INFO)
        self.logger.addHandler(make_log_from_settings(settings))

        # TURNS OUT LOGGERS ARE REALLY SLOW TOO
        self.queue = threads.Queue("queue for classic logger", max=10000, silent=True)
        self.thread = Thread(
            "pushing to classic logger",
            time_delta_pusher,
            appender=self.logger.info,
            queue=self.queue,
            interval=0.3
        )
        self.thread.parent.remove_child(self.thread)  # LOGGING WILL BE RESPONSIBLE FOR THREAD stop()
        self.thread.start()

    def write(self, template, params):
        # http://docs.python.org/2/library/logging.html# logging.LogRecord
        self.queue.add({"template": template, "params": params})

    def stop(self):
        with suppress_exception:
            self.queue.add(Thread.STOP)  # BE PATIENT, LET REST OF MESSAGE BE SENT
            self.thread.join()

        with suppress_exception:
            self.queue.close()


def make_log_from_settings(settings):
    assert settings["class"]

    # IMPORT MODULE FOR HANDLER
    path = settings["class"].split(".")
    class_name = path[-1]
    path = ".".join(path[:-1])
    constructor = None
    try:
        temp = __import__(path, globals(), locals(), [class_name], -1)
        constructor = object.__getattribute__(temp, class_name)
    except Exception, e:
        if settings.stream and not constructor:
            # PROVIDE A DEFAULT STREAM HANLDER
            constructor = TextLog_usingThreadedStream
        else:
            Log.error("Can not find class {{class}}",  {"class": path}, cause=e)

    # IF WE NEED A FILE, MAKE SURE DIRECTORY EXISTS
    if settings.filename:
        from pyLibrary.env.files import File

        f = File(settings.filename)
        if not f.parent.exists:
            f.parent.create()

    settings['class'] = None
    params = unwrap(settings)
    log_instance = constructor(**params)
    return log_instance

