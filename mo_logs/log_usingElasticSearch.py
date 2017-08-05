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

from collections import Mapping

import mo_json
from mo_dots import wrap, coalesce
from mo_json import value2json
from mo_kwargs import override
from mo_threads import Thread, Queue, Till, THREAD_STOP
from mo_times import MINUTE, Duration
from pyLibrary.env.elasticsearch import Cluster
from jx_python import jx

from mo_logs import Log, strings
from mo_logs.exceptions import suppress_exception
from mo_logs.log_usingNothing import StructuredLogger

MAX_BAD_COUNT = 5
LOG_STRING_LENGTH = 2000


class StructuredLogger_usingElasticSearch(StructuredLogger):
    @override
    def __init__(self, host, index, port=9200, type="log", max_size=1000, batch_size=100, kwargs=None):
        """
        settings ARE FOR THE ELASTICSEARCH INDEX
        """
        self.es = Cluster(kwargs).get_or_create_index(
            schema=mo_json.json2value(value2json(SCHEMA), leaves=True),
            limit_replicas=True,
            tjson=True,
            kwargs=kwargs
        )
        self.batch_size = batch_size
        self.es.add_alias(coalesce(kwargs.alias, kwargs.index))
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
                Till(seconds=1).wait()
                messages = wrap(self.queue.pop_all())
                if not messages:
                    continue

                for g, mm in jx.groupby(messages, size=self.batch_size):
                    scrubbed = []
                    try:
                        for i, message in enumerate(mm):
                            if message is THREAD_STOP:
                                please_stop.go()
                                return
                            scrubbed.append(_deep_json_to_string(message, depth=3))
                    finally:
                        self.es.extend(scrubbed)
                    bad_count = 0
            except Exception as e:
                Log.warning("Problem inserting logs into ES", cause=e)
                bad_count += 1
                if bad_count > MAX_BAD_COUNT:
                    Log.warning("Given up trying to write debug logs to ES index {{index}}", index=self.es.settings.index)
                Till(seconds=30).wait()

        # CONTINUE TO DRAIN THIS QUEUE
        while not please_stop:
            try:
                Till(seconds=1).wait()
                self.queue.pop_all()
            except Exception as e:
                Log.warning("Should not happen", cause=e)

    def stop(self):
        with suppress_exception:
            self.queue.add(THREAD_STOP)  # BE PATIENT, LET REST OF MESSAGE BE SENT

        with suppress_exception:
            self.queue.close()


def _deep_json_to_string(value, depth):
    """
    :param value: SOME STRUCTURE
    :param depth: THE MAX DEPTH OF PROPERTIES, DEEPER WILL BE STRING-IFIED
    :return: FLATTER STRUCTURE
    """
    if isinstance(value, Mapping):
        if depth == 0:
            return strings.limit(value2json(value), LOG_STRING_LENGTH)

        return {k: _deep_json_to_string(v, depth - 1) for k, v in value.items()}
    elif isinstance(value, list):
        return strings.limit(value2json(value), LOG_STRING_LENGTH)
    elif isinstance(value, (float, int, long)):
        return value
    elif isinstance(value, basestring):
        return strings.limit(value, LOG_STRING_LENGTH)
    else:
        return strings.limit(value2json(value), LOG_STRING_LENGTH)


SCHEMA = {
    "settings": {"index.number_of_shards": 2, "index.number_of_replicas": 2},
    "mappings": {"_default_": {
        "dynamic_templates": [
            {"everything_else": {
                "match": "*",
                "mapping": {"index": False}
            }}
        ],
        "_all": {"enabled": False},
        "_source": {"compress": True, "enabled": True},
        "properties": {
            "params": {"type": "object", "dynamic": False, "index": False},
            "template": {"type": "object", "dynamic": False, "index": False},
            "context": {
                "type": "object",
                "dynamic": False,
                "properties": {
                    "$string": {"type": "string"}
                }
            },
            "$exists": {"type": "string"},
            "machine": {
                "dynamic": True,
                "properties": {
                    "python": {
                        "properties": {"$string": {"index": "not_analyzed", "type": "string", "doc_values": True}}},
                    "$exists": {"type": "string"},
                    "os": {"properties": {"$string": {"index": "not_analyzed", "type": "string", "doc_values": True}}},
                    "name": {"properties": {"$string": {"index": "not_analyzed", "type": "string", "doc_values": True}}}
                }
            },
            "location": {
                "dynamic": True,
                "properties": {
                    "$exists": {"type": "string"},
                    "file": {"properties": {"$string": {"index": "not_analyzed", "type": "string", "doc_values": True}}},
                    "method": {
                        "properties": {"$string": {"index": "not_analyzed", "type": "string", "doc_values": True}}},
                    "line": {"properties": {"$integer": {"index": "not_analyzed", "type": "long", "doc_values": True}}}
                }
            },
            "thread": {
                "dynamic": True,
                "properties": {
                    "$exists": {"type": "string"},
                    "name": {"properties": {"$string": {"index": "not_analyzed", "type": "string", "doc_values": True}}},
                    "id": {"properties": {"$string": {"index": "not_analyzed", "type": "string", "doc_values": True}}}
                }
            },
            "timestamp": {"properties": {"$string": {"index": "not_analyzed", "type": "string"}}}
        }
    }}
}
