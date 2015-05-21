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

from pyLibrary import convert
from pyLibrary.env.elasticsearch import Cluster
from pyLibrary.meta import use_settings
from pyLibrary.thread.threads import Thread
from .logs import BaseLog


class Log_usingElasticSearch(BaseLog):

    @use_settings
    def __init__(self, host, index, type="log", max_size=1000, batch_size=100, settings=None):
        """
        settings ARE FOR THE ELASTICSEARCH INDEX
        """
        self.es = Cluster(settings).get_or_create_index(
            schema=convert.json2value(convert.value2json(SCHEMA), paths=True),
            limit_replicas=True,
            settings=settings
        )
        self.queue = self.es.threaded_queue(max_size=max_size, batch_size=batch_size)

    def write(self, template, params):
        try:
            if params.get("template"):
                # DETECTED INNER TEMPLATE, ASSUME TRACE IS ON, SO DO NOT NEED THE OUTER TEMPLATE
                self.queue.add({"value": params})
            else:
                if len(template) > 2000:
                    template = template[:1997] + "..."
                self.queue.add({"value": {"template": template, "params": params}})
            return self
        except Exception, e:
            raise e  # OH NO!

    def stop(self):
        try:
            self.queue.add(Thread.STOP)  # BE PATIENT, LET REST OF MESSAGE BE SENT
        except Exception, e:
            pass

        try:
            self.queue.close()
        except Exception, f:
            pass



SCHEMA = {
    "settings": {
        "index.number_of_shards": 3,
        "index.number_of_replicas": 2,
        "index.store.throttle.type": "merge",
        "index.store.throttle.max_bytes_per_sec": "2mb",
        "index.cache.filter.expire": "1m",
        "index.cache.field.type": "soft",
    },
    "mappings": {
        "_default_": {
            "dynamic_templates": [
                {
                    "values_strings": {
                        "match": "*",
                        "match_mapping_type" : "string",
                        "mapping": {
                            "type": "string",
                            "index": "not_analyzed"
                        }
                    }
                }
            ],
            "_all": {
                "enabled": False
            },
            "_source": {
                "compress": True,
                "enabled": True
            },
            "properties": {
                "timestamp": {
                    "type": "double",
                    "index": "not_analyzed",
                    "store": "yes"
                },
                "params": {
                    "type": "object",
                    "enabled": False,
                    "index": "no",
                    "store": "yes"
                }
            }
        }
    }
}
