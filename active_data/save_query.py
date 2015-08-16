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
import hashlib

from pyLibrary import convert
from pyLibrary.dot import wrap
from pyLibrary.env.elasticsearch import Cluster
from pyLibrary.meta import use_settings
from pyLibrary.queries.containers.cube import Cube
from pyLibrary.queries.qb_usingES import FromES
from pyLibrary.thread.threads import Thread
from pyLibrary.times.dates import Date


HASH_BLOCK_SIZE = 100


class SaveQueries(object):
    @use_settings
    def __init__(self, host, index, type="query", max_size=10, batch_size=10, settings=None):
        """
        settings ARE FOR THE ELASTICSEARCH INDEX
        """
        es = Cluster(settings).get_or_create_index(
            schema=convert.json2value(convert.value2json(SCHEMA), paths=True),
            limit_replicas=True,
            settings=settings
        )
        #ENSURE THE TYPE EXISTS FOR PROBING
        es.add({"id": "dummy", "value": {
            "hash": "dummy",
            "create_time": Date.now(),
            "last_used": Date.now(),
            "query": ""
        }})
        es.add_alias(es.settings.alias)
        self.queue = es.threaded_queue(max_size=max_size, batch_size=batch_size)
        self.es = FromES(es.settings)



    def find(self, hash):
        result = self.es.query({
            "from": {"type": "elasticsearch", "settings": self.es.settings},
            "where": {"prefix": {"hash": hash}}
        })

        try:
            query = wrap(result.data).query
            if len(query) != 1:
                return None
        except Exception, e:
            return None

        self.es.update({
            "update": {"type": "elasticsearch", "settings": self.es.settings},
            "set": {"last_used": Date.now()},
            "where": {"eq": {"hash": hash}}
        })

        return query[0]

    def save(self, query):
        query.meta = None
        json = convert.value2json(query)
        hash = convert.unicode2utf8(json)

        #TRY MANY HASHES AT ONCE
        hashes = [None] * HASH_BLOCK_SIZE
        for i in range(HASH_BLOCK_SIZE):
            hash = hashlib.sha1(hash).digest()
            hashes[i] = hash

        short_hashes = [convert.bytes2base64(h[0:6]).replace("/", "_") for h in hashes]
        available = {h: True for h in short_hashes}

        existing = self.es.query({
            "from": {"type": "elasticsearch", "settings": self.es.settings},
            "where": {"terms": {"hash": short_hashes}}
        })

        for e in Cube(select=existing.select, edges=existing.edges, data=existing.data).values():
            if e.query == json:
                return e.hash
            short_hashes[e.hash] = False

        # THIS WILL THROW AN ERROR IF THERE ARE NONE, HOW UNLUCKY!
        best = [h for h in short_hashes if available[h]][0]

        self.queue.add({
            "id": best,
            "value": {
                "hash": best,
                "create_time": Date.now(),
                "last_used": Date.now(),
                "query": json
            }
        })

        return best

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
        "index.cache.filter.expire": "1m",
        "index.cache.field.type": "soft",
    },
    "mappings": {
        "_default_": {
            "dynamic_templates": [
                {
                    "values_strings": {
                        "match": "*",
                        "match_mapping_type": "string",
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
                "enabled": True
            },
            "properties": {
                "create_time": {
                    "type": "double",
                    "index": "not_analyzed",
                    "store": "yes"
                },
                "last_used": {
                    "type": "double",
                    "index": "not_analyzed",
                    "store": "yes"
                },
                "hash": {
                    "type": "string",
                    "index": "not_analyzed",
                    "store": "yes"
                },
                "query": {
                    "type": "object",
                    "enabled": False,
                    "index": "no",
                    "store": "yes"
                }
            }
        }
    }
}
