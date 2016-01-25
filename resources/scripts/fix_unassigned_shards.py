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

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap
from pyLibrary.env import http
from pyLibrary.maths.randoms import Random
from pyLibrary.queries import qb


def assign_shards(settings):
    """
    ASSIGN THE UNASSIGNED SHARDS
    """
    path = settings.elasticsearch.host+":"+unicode(settings.elasticsearch.port)

    #GET LIST OF NODES
    # coordinator    26.2gb
    # secondary     383.7gb
    # spot_47727B30   934gb
    # spot_BB7A8053   934gb
    # primary       638.8gb
    # spot_A9DB0988     5tb
    Log.note("get nodes")
    nodes = wrap(list(convert_table_to_list(http.get(path + "/_cat/nodes?bytes=b&h=n,r,d").content, ["name", "role", "disk"])))
    for n in nodes:
        n.disk = float(n.disk)
        # ASSIGN SHARDS TO SPOT NODES ONLY
    #     if not n.name.startswith("spot_"):
    #         n.disk = 0.0
    Log.note("Nodes:\n{{nodes}}", nodes=nodes)


    #GET LIST OF SHARDS, WITH STATUS
    # debug20150915_172538                0  p STARTED        37319   9.6mb 172.31.0.196 primary
    # debug20150915_172538                0  r UNASSIGNED
    # debug20150915_172538                1  p STARTED        37624   9.6mb 172.31.0.39  secondary
    # debug20150915_172538                1  r UNASSIGNED
    shards = wrap(list(convert_table_to_list(http.get(path + "/_cat/shards").content, ["index", "i", "type", "status", "num", "size", "ip", "node"])))
    # Log.note("Shards:\n{{shards}}", shards=shards)
    for shard in qb.sort(shards, "index"):
        if shard.status=="UNASSIGNED" and shard.index=="saved_queries20150510_160318" and shard.i=='0':
            i = Random.weight(nodes.disk)
            command = wrap({"allocate":{
                "index": shard.index,
                "shard": shard.i,
                "node": "tertiary", # nodes[i].name,
                "allow_primary": True
            }})
            result = convert.json2value(convert.utf82unicode(http.post(path + "/_cluster/reroute", json={"commands": [command]}).content))
            if not result.acknowledged:
                Log.warning("Can not allocate: {{error}}", error=result.error)
            else:
                Log.note("index={{shard.index}}, shard={{shard.i}}, assign_to={{node}}, ok={{result.acknowledged}}", shard=shard, result=result, node=nodes[i].name)


def convert_table_to_list(table, column_names):
    lines = [l for l in table.split("\n") if l.strip()]

    # FIND THE COLUMNS WITH JUST SPACES
    columns = []
    for i, c in enumerate(zip(*lines)):
        if all(r == " " for r in c):
            columns.append(i)

    for i, row in enumerate(lines):
        yield wrap({c: r for c, r in zip(column_names, split_at(row, columns))})


def split_at(row, columns):
    output = []
    last = 0
    for c in columns:
        output.append(row[last:c].strip())
        last = c
    output.append(row[last:].strip())
    return output


def main():
    try:
        settings = wrap({"elasticsearch":{
            "host": "http://activedata.allizom.org",
            "port": 9200,
            "debug": True
        }})

        Log.start(settings)
        assign_shards(settings)
    except Exception, e:
        Log.error("Problem with assign of shards", e)
    finally:
        Log.stop()

if __name__=="__main__":
    main()
