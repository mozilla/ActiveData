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

from pyLibrary.debugs.logs import Log
from pyDots import wrap
from pyLibrary.env import http


def move_shards(settings):
    path = settings.elasticsearch.host+":"+unicode(settings.elasticsearch.port)

    command = [
        {"move":
            {
                "index": "unittest20150803_045709",
                "shard": 20,
                "from_node": "primary",
                "to_node": "tertiary"
            }
        }
    ]

    result = http.post_json(path + "/_cluster/reroute", data={"commands": command})
    Log.note("result {{result}}", result=result)


def main():
    try:
        settings = wrap({"elasticsearch":{
            "host": "http://activedata.allizom.org",
            "port": 9200,
            "debug": True
        }})

        Log.start(settings)
        move_shards(settings)
    except Exception, e:
        Log.error("Problem with assign of shards", e)
    finally:
        Log.stop()

if __name__=="__main__":
    main()
