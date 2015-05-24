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

from boto import sqs, boto
from boto.sqs.message import Message
import requests

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap, unwrap
from pyLibrary.maths import Math
from pyLibrary.meta import use_settings
from pyLibrary.thread.threads import Thread
from pyLibrary.times.durations import Duration, SECOND


class Queue(object):
    @use_settings
    def __init__(
        self,
        name,
        region,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        debug=False,
        settings=None
    ):
        self.settings = settings
        self.pending = []

        if settings.region not in [r.name for r in sqs.regions()]:
            Log.error("Can not find region {{region}} in {{regions}}",  region= settings.region,  regions= [r.name for r in sqs.regions()])

        conn = sqs.connect_to_region(
            region_name=unwrap(settings.region),
            aws_access_key_id=unwrap(settings.aws_access_key_id),
            aws_secret_access_key=unwrap(settings.aws_secret_access_key),
        )
        self.queue = conn.get_queue(settings.name)
        if self.queue == None:
            Log.error("Can not find queue with name {{queue}} in region {{region}}",  queue= settings.name,  region= settings.region)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __len__(self):
        attrib = self.queue.get_attributes("ApproximateNumberOfMessages")
        return int(attrib['ApproximateNumberOfMessages'])

    def add(self, message):
        message = wrap(message)
        m = Message()
        m.set_body(convert.value2json(message))
        self.queue.write(m)

    def extend(self, messages):
        for m in messages:
            self.add(m)

    def pop(self, wait=SECOND, till=None):
        m = self.queue.read(wait_time_seconds=Math.floor(wait.seconds))
        if not m:
            return None

        self.pending.append(m)
        return convert.json2value(m.get_body())

    def commit(self):
        pending = self.pending
        self.pending = []
        for p in pending:
            self.queue.delete_message(p)

    def rollback(self):
        if self.pending:
            pending = self.pending
            self.pending = []

            for p in pending:
                m = Message()
                m.set_body(p.get_body())
                self.queue.write(m)

            for p in pending:
                self.queue.delete_message(p)

            if self.settings.debug:
                Log.alert("{{num}} messages returned to queue",  num= len(pending))

    def close(self):
        self.commit()


def capture_termination_signal(please_stop):
    """
    WILL SIGNAL please_stop WHEN THIS AWS INSTANCE IS DUE FOR SHUTDOWN
    """

    def worker(please_stop):
        while not please_stop:
            try:
                response = requests.get("http://169.254.169.254/latest/meta-data/spot/termination-time")
                if response.status_code != 400:
                    please_stop.go()
                    return
            except Exception, e:
                pass  # BE QUIET
                Thread.sleep(seconds=61)
            Thread.sleep(seconds=11)

    Thread.run("listen for termination", worker)


def get_instance_metadata():
    return wrap(dict(boto.utils.get_instance_metadata()))


from . import s3
