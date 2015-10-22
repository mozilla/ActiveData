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

from datetime import datetime, timedelta
import sys
from pyLibrary.debugs.text_logs import TextLog, DEBUG_LOGGING
from pyLibrary.debugs.logs import Log
from pyLibrary.strings import expand_template
from pyLibrary.thread.threads import Thread



class TextLog_usingThreadedStream(TextLog):
    # stream CAN BE AN OBJCET WITH write() METHOD, OR A STRING
    # WHICH WILL eval() TO ONE
    def __init__(self, stream):
        assert stream

        use_UTF8 = False

        if isinstance(stream, basestring):
            if stream.startswith("sys."):
                use_UTF8 = True  # sys.* ARE OLD AND CAN NOT HANDLE unicode
            self.stream = eval(stream)
            name = stream
        else:
            self.stream = stream
            name = "stream"

        # WRITE TO STREAMS CAN BE *REALLY* SLOW, WE WILL USE A THREAD
        from pyLibrary.thread.threads import Queue

        if use_UTF8:
            def utf8_appender(value):
                if isinstance(value, unicode):
                    value = value.encode('utf8')
                self.stream.write(value)

            appender = utf8_appender
        else:
            appender = self.stream.write

        self.queue = Queue("log to stream", max=10000, silent=True)
        self.thread = Thread("log to " + name, time_delta_pusher, appender=appender, queue=self.queue, interval=timedelta(seconds=0.3))
        self.thread.parent.remove_child(self.thread)  # LOGGING WILL BE RESPONSIBLE FOR THREAD stop()
        self.thread.start()

    def write(self, template, params):
        try:
            self.queue.add({"template": template, "params": params})
            return self
        except Exception, e:
            raise e  # OH NO!

    def stop(self):
        try:
            if DEBUG_LOGGING:
                sys.stdout.write("TextLog_usingThreadedStream sees stop, adding stop to queue\n")
            self.queue.add(Thread.STOP)  # BE PATIENT, LET REST OF MESSAGE BE SENT
            self.thread.join()
            if DEBUG_LOGGING:
                sys.stdout.write("TextLog_usingThreadedStream done\n")
        except Exception, e:
            if DEBUG_LOGGING:
                raise e

        try:
            self.queue.close()
        except Exception, f:
            if DEBUG_LOGGING:
                raise f



def time_delta_pusher(please_stop, appender, queue, interval):
    """
    appender - THE FUNCTION THAT ACCEPTS A STRING
    queue - FILLED WITH LOG ENTRIES {"template":template, "params":params} TO WRITE
    interval - timedelta
    USE IN A THREAD TO BATCH LOGS BY TIME INTERVAL
    """

    if not isinstance(interval, timedelta):
        Log.error("Expecting interval to be a timedelta")

    next_run = datetime.utcnow() + interval

    while not please_stop:
        Thread.sleep(till=next_run)
        next_run = datetime.utcnow() + interval
        logs = queue.pop_all()
        if logs:
            lines = []
            for log in logs:
                try:
                    if log is Thread.STOP:
                        please_stop.go()
                        next_run = datetime.utcnow()
                    else:
                        expanded = expand_template(log.get("template"), log.get("params"))
                        lines.append(expanded)
                except Exception, e:
                    Log.warning("Trouble formatting logs", e)
                    # SWALLOW ERROR, GOT TO KEEP RUNNING
            try:
                if DEBUG_LOGGING and please_stop:
                    sys.stdout.write("Call to appender with " + str(len(lines)) + " lines\n")
                appender(u"\n".join(lines) + u"\n")
                if DEBUG_LOGGING and please_stop:
                    sys.stdout.write("Done call to appender with " + str(len(lines)) + " lines\n")
            except Exception, e:
                sys.stderr.write("Trouble with appender: " + str(e.message) + "\n")
                # SWALLOW ERROR, GOT TO KEEP RUNNNIG

