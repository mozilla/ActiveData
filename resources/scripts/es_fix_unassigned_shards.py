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
from pyLibrary.dot import wrap, listwrap
from pyLibrary.env import http
from pyLibrary.maths import Math
from pyLibrary.maths.randoms import Random
from pyLibrary.queries import jx
from pyLibrary.queries.unique_index import UniqueIndex
from pyLibrary.thread.threads import Thread

CONCURRENT = 4


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
    nodes = UniqueIndex("name", list(convert_table_to_list(http.get(path + "/_cat/nodes?bytes=b&h=n,r,d,i").content, ["name", "role", "disk", "ip"])))
    for n in nodes:
        n.disk = float(n.disk)
        if n.name.startswith("spot_"):
            n.zone = "spot"
        else:
            n.zone = n.name
    # Log.note("Nodes:\n{{nodes}}", nodes=list(nodes))

    #GET LIST OF SHARDS, WITH STATUS
    # debug20150915_172538                0  p STARTED        37319   9.6mb 172.31.0.196 primary
    # debug20150915_172538                0  r UNASSIGNED
    # debug20150915_172538                1  p STARTED        37624   9.6mb 172.31.0.39  secondary
    # debug20150915_172538                1  r UNASSIGNED
    shards = wrap(list(convert_table_to_list(http.get(path + "/_cat/shards").content, ["index", "i", "type", "status", "num", "size", "ip", "node"])))
    for s in shards:
        s.i = int(s.i)
        s.size = text_to_bytes(s.size)
        s.zone = nodes[s.node].zone

    # LOOKING FOR SHARDS WITH ONLY ONE INSTANCE, IN THE spot ZONE
    high_risk_shards = []
    for g, replicas in jx.groupby(shards, ["index", "i"]):
        replicas = list(replicas)
        safe_zones = list(set([s.zone for s in replicas if s.status == "STARTED"]))
        if len(safe_zones) == 1 and safe_zones[0] == "spot":
            # MARK NODE AS RISKY
            for s in replicas:
                if s.status == "UNASSIGNED":
                    high_risk_shards.append(s)
                    break  # ONLY NEED ONE
    if high_risk_shards:
        Log.note("{{num}} high risk shards found", num=len(high_risk_shards))
        allocate(jx.sort(high_risk_shards, "size"), path, nodes, set(n.zone for n in nodes)-{"spot"})
    else:
        Log.note("No high risk shards found")

    # LOOK SHARDS WITH A QUORUM (USUALLY 2) IN A SINGLE ZONE (ES BUG https://github.com/elastic/elasticsearch/issues/13667)
    # buggy_shards = []
    # for g, replicas in jx.groupby(shards, ["index", "i"]):
    #     replicas = list(replicas)
    #
    #     num = len(replicas)
    #     for zone, parts in jx.group(replicas, "zone"):
    #         parts = len(parts)
    #         if len(parts) > float(num) / 2.0:
    #             # WE CAN ASSIGN ONE REPLICA TO ANTHER ZONE
    #             i = Random.int(len(parts))
    #             r = parts[i]
    #             buggy_shards.append(r)
    #
    # if buggy_shards:
    #     Log.note("{{num}} high risk shards found", num=len(buggy_shards))
    #     allocate(jx.sort(buggy_shards, "size")[:2:], path, nodes, set(n.zone for n in nodes)-{"spot"})
    # else:
    #     Log.note("No high risk shards found")

    # ARE WE BUSY MOVING TOO MUCH?
    relocating = [s for s in shards if s.status in ("RELOCATING", "INITIALIZING")]
    if len(relocating) >= CONCURRENT:
        Log.note("Delay work, cluster busy RELOCATING/INITIALIZING {{num}} shards", num=len(relocating))
        return

    # LOOK FOR UNALLOCATED SHARDS WE CAN PUT ON THE SPOT ZONE
    low_risk_shards = []
    for g, replicas in jx.groupby(shards, ["index", "i"]):
        replicas = wrap(list(replicas))
        size = Math.MAX(replicas.size)
        safe_zones = list(set([s.zone for s in replicas if s.status == "STARTED" and s.zone != "spot"]))
        if safe_zones:
            # WE CAN ASSIGN THIS REPLICA TO spot
            for s in replicas:
                if s.status == "UNASSIGNED":
                    s.size = size
                    low_risk_shards.append(s)
                    break  # ONLY NEED ONE

    if low_risk_shards:
        Log.note("{{num}} low risk shards found", num=len(low_risk_shards))
        num = CONCURRENT - len(relocating)
        allocate(jx.sort(low_risk_shards, "size")[:num:], path, nodes, {"spot"})
        return
    else:
        Log.note("No low risk shards found")

    # LOOK FOR SHARDS WE CAN MOVE TO SPOT
    too_safe_shards = []
    for g, replicas in jx.groupby(shards, ["index", "i"]):
        replicas = listwrap(list(replicas))
        safe_zones = list(set([nodes[s.node].zone for s in replicas if s.status == "STARTED"]) - {"spot"})
        if len(safe_zones) >= len(replicas):
            # WE CAN ASSIGN ONE REPLICA TO spot
            i = Random.int(len(replicas))
            r = replicas[i]
            too_safe_shards.append(r)

    if too_safe_shards:
        num = CONCURRENT - len(relocating)
        Log.note("{{num}} shards can be moved to spot", num=len(too_safe_shards))
        allocate(jx.sort(too_safe_shards, {"value": "size", "sort": -1})[0:num:], path, nodes, {"spot"})
    else:
        Log.note("No shards moved")



def allocate(shards, path, nodes, zones):
    for shard in shards:
        i = Random.weight([n.disk if n.zone in zones else 0 for n in nodes])
        destination_node = list(nodes)[i].name

        if shard.status == "UNASSIGNED":
            # destination_node = "secondary"
            command = wrap({"allocate": {
                "index": shard.index,
                "shard": shard.i,
                "node": destination_node,  # nodes[i].name,
                "allow_primary": True
            }})
        else:
            command = wrap({"move":
                {
                    "index": shard.index,
                    "shard": shard.i,
                    "from_node": shard.node,
                    "to_node": destination_node
                }
            })

        result = convert.json2value(
            convert.utf82unicode(http.post(path + "/_cluster/reroute", json={"commands": [command]}).content))
        if not result.acknowledged:
            Log.warning("Can not move/allocate: {{error}}", error=result.error)
        else:
            Log.note("index={{shard.index}}, shard={{shard.i}}, assign_to={{node}}, ok={{result.acknowledged}}",
                     shard=shard, result=result, node=destination_node)


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


def text_to_bytes(size):
    if size=="":
        return 0

    multiplier = {
        "kb":1000,
        "mb":1000000,
        "gb":1000000000
    }.get(size[-2:])
    if not multiplier:
        multiplier = 1
        size = size[:-1]
    else:
        size = size[:-2]
    try:
        return float(size)*float(multiplier)
    except Exception, e:
        Log.error("not expected", cause=e)



def main():
    try:
        settings = wrap({"elasticsearch":{
            "host": "http://activedata.allizom.org",
            "port": 9200,
            "debug": True
        }})

        Log.start(settings)
        path = settings.elasticsearch.host+":"+unicode(settings.elasticsearch.port)
        response = http.put(path+"/_cluster/settings", data='{"persistent": {"cluster.routing.allocation.enable": "none"}}')
        Log.note("DISABLE SHARD MOVEMENT: {{result}}", result=response.all_content)

        while True:
            assign_shards(settings)
            Thread.sleep(seconds=30)
    except Exception, e:
        Log.error("Problem with assign of shards", e)
    finally:
        Log.stop()

if __name__=="__main__":
    main()
