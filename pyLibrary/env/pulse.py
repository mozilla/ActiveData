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

from mozillapulse.consumers import GenericConsumer

from pyLibrary.debugs.logs import Log
from pyLibrary.dot import unwrap, wrap, nvl
from pyLibrary.meta import use_settings
from pyLibrary.thread.threads import Thread


class Pulse(Thread):
    @use_settings
    def __init__(
        self,
        target,  # WILL BE CALLED WITH PULSE PAYLOADS AND ack() IF COMPLETE$ED WITHOUT EXCEPTION
        target_queue,  # (aka self.queue) WILL BE FILLED WITH PULSE PAYLOADS
        exchange,  # name of the Pulse exchange
        topic,  # message name pattern to subscribe to  ('#' is wildcard)
        host='pulse.mozilla.org',  # url to connect,
        port=5671,  # tcp port
        user=None,
        password=None,
        vhost="/",
        start=0,  # USED AS STARTING POINT FOR ASSIGNING THE _meta.count ATTRIBUTE
        ssl=True,
        applabel=None,
        heartbeat=False,  # True to also get the Pulse heartbeat message
        durable=False,  # True to keep queue after shutdown
        serializer='json',
        broker_timezone='GMT',
        settings=None
    ):
        self.target_queue = target_queue
        self.pulse_target = target
        if (target_queue == None and target == None) or (target_queue != None and target != None):
            Log.error("Expecting a queue (for fast digesters) or a target (for slow digesters)")

        Thread.__init__(self, name="Pulse consumer for " + settings.exchange, target=self._worker)
        self.settings = settings
        settings.callback = self._got_result
        settings.user = nvl(settings.user, settings.username)
        settings.applabel = nvl(settings.applable, settings.queue, settings.queue_name)

        self.pulse = GenericConsumer(settings, connect=True, **unwrap(settings))
        self.count = nvl(start, 0)
        self.start()


    def _got_result(self, data, message):
        data = wrap(data)
        data._meta.count = self.count
        self.count += 1

        if self.settings.debug:
            Log.note("{{data}}", {"data": data})
        if self.target_queue != None:
            try:
                self.target_queue.add(data)
                message.ack()
            except Exception, e:
                if not self.target_queue.closed:  # EXPECTED TO HAPPEN, THIS THREAD MAY HAVE BEEN AWAY FOR A WHILE
                    raise e
        else:
            try:
                self.pulse_target(data)
                message.ack()
            except Exception, e:
                Log.error("Problem processing Pulse payload\n{{data|indent}}", {"data": data}, e)

    def _worker(self, please_stop):
        while not please_stop:
            try:
                self.pulse.listen()
            except Exception, e:
                if not please_stop:
                    Log.warning("pulse had problem", e)

    def __exit__(self, exc_type, exc_val, exc_tb):
        Log.note("clean pulse exit")
        self.please_stop.go()
        try:
            self.target_queue.add(Thread.STOP)
            Log.note("stop put into queue")
        except:
            pass

        try:
            self.pulse.disconnect()
        except Exception, e:
            Log.warning("Can not disconnect during pulse exit, ignoring", e)
        Thread.__exit__(self, exc_type, exc_val, exc_tb)
