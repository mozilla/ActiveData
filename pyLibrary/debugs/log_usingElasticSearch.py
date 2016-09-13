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

from pyLibrary import convert, strings
from pyLibrary.debugs.exceptions import suppress_exception
from pyLibrary.debugs.logs import Log
from pyLibrary.debugs.text_logs import TextLog
from pyLibrary.dot import wrap, unwrap, coalesce, set_default
from pyLibrary.env.elasticsearch import Cluster
from pyLibrary.meta import use_settings
from pyLibrary.queries import jx
from pyLibrary.thread.threads import Thread, Queue
from pyLibrary.times.durations import MINUTE, Duration


class TextLog_usingElasticSearch(TextLog):
    @use_settings
    def __init__(self, host, index, type="log", max_size=1000, batch_size=100, settings=None):
        """
        settings ARE FOR THE ELASTICSEARCH INDEX
        """
        self.es = Cluster(settings).get_or_create_index(
            schema=convert.json2value(convert.value2json(SCHEMA), leaves=True),
            limit_replicas=True,
            tjson=True,
            settings=settings
        )
        self.batch_size = batch_size
        self.es.add_alias(coalesce(settings.alias, settings.index))
        self.queue = Queue("debug logs to es", max=max_size, silent=True)
        self.es.settings.retry.times = coalesce(self.es.settings.retry.times, 3)
        self.es.settings.retry.sleep = Duration(coalesce(self.es.settings.retry.sleep, MINUTE))
        Thread.run("add debug logs to es", self._insert_loop)

    def write(self, template, params):
        if params.get("template"):
            # DETECTED INNER TEMPLATE, ASSUME TRACE IS ON, SO DO NOT NEED THE OUTER TEMPLATE
            self.queue.add({"value": params})
        else:
            template = strings.limit(template, 2000)
            self.queue.add({"value": {"template": template, "params": params}}, timeout=3 * MINUTE)
        return self

    def _insert_loop(self, please_stop=None):
        bad_count = 0
        while not please_stop:
            try:
                Thread.sleep(seconds=1)
                messages = wrap(self.queue.pop_all())
                if messages:
                    # for m in messages:
                    #     m.value.params = leafer(m.value.params)
                    #     m.value.error = leafer(m.value.error)
                    for g, mm in jx.groupby(messages, size=self.batch_size):
                        self.es.extend(mm)
                    bad_count = 0
            except Exception, e:
                Log.warning("Problem inserting logs into ES", cause=e)
                bad_count += 1
                if bad_count > 5:
                    break
        Log.warning("Given up trying to write debug logs to ES index {{index}}", index=self.es.settings.index)

        # CONTINUE TO DRAIN THIS QUEUE
        while not please_stop:
            try:
                Thread.sleep(seconds=1)
                self.queue.pop_all()
            except Exception, e:
                Log.warning("Should not happen", cause=e)

    def stop(self):
        with suppress_exception:
            self.queue.add(Thread.STOP)  # BE PATIENT, LET REST OF MESSAGE BE SENT

        with suppress_exception:
            self.queue.close()



def leafer(param):
    temp = unwrap(param.leaves())
    if temp:
        return dict(temp)
    else:
        return None


SCHEMA = {
    "settings": {"index.number_of_shards": 2, "index.number_of_replicas": 2},
    "mappings": {"_default_": {
        "dynamic_templates": [
            {"everything_else": {
                "match": "*",
                "mapping": {"index": "no"}
            }}
        ],
        "_all": {"enabled": False},
        "_source": {"compress": True, "enabled": True},
        "properties": {
            "params": {"type": "object", "dynamic": False, "index": "no"},
            "template": {"type": "object", "dynamic": False, "index": "no"},
            "context": {
                "type": "object",
                "dynamic": False,
                "properties": {
                    "$value": {"type": "string"}
                }
            },
            "$object": {"type": "string"},
            "machine": {
                "dynamic": True,
                "properties": {
                    "python": {
                        "properties": {"$value": {"index": "not_analyzed", "type": "string", "doc_values": True}}},
                    "$object": {"type": "string"},
                    "os": {"properties": {"$value": {"index": "not_analyzed", "type": "string", "doc_values": True}}},
                    "name": {"properties": {"$value": {"index": "not_analyzed", "type": "string", "doc_values": True}}}
                }
            },
            "location": {
                "dynamic": True,
                "properties": {
                    "$object": {"type": "string"},
                    "file": {"properties": {"$value": {"index": "not_analyzed", "type": "string", "doc_values": True}}},
                    "method": {
                        "properties": {"$value": {"index": "not_analyzed", "type": "string", "doc_values": True}}},
                    "line": {"properties": {"$value": {"index": "not_analyzed", "type": "long", "doc_values": True}}}
                }
            },
            "thread": {
                "dynamic": True,
                "properties": {
                    "$object": {"type": "string"},
                    "name": {"properties": {"$value": {"index": "not_analyzed", "type": "string", "doc_values": True}}},
                    "id": {"properties": {"$value": {"index": "not_analyzed", "type": "string", "doc_values": True}}}
                }
            },
            "timestamp": {"properties": {"$value": {"index": "not_analyzed", "type": "string"}}}
        }
    }}
}
