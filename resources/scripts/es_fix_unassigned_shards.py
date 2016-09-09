
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

from copy import copy

from pyLibrary import convert, strings
from pyLibrary.debugs import constants
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap, Dict, coalesce, DictList
from pyLibrary.env import http
from pyLibrary.maths import Math
from pyLibrary.maths.randoms import Random
from pyLibrary.queries import jx
from pyLibrary.queries.unique_index import UniqueIndex
from pyLibrary.thread.threads import Thread, Signal

CONCURRENT = 1
BIG_SHARD_SIZE = 5 * 1024 * 1024 * 1024  # SIZE WHEN WE SHOULD BE MOVING ONLY ONE SHARD AT A TIME

CURRENT_MOVING_SHARDS = DictList()  # BECAUSE ES WILL NOT TELL US WHERE THE SHARDS ARE MOVING TO

def assign_shards(settings):
    """
    ASSIGN THE UNASSIGNED SHARDS
    """
    path = settings.elasticsearch.host + ":" + unicode(settings.elasticsearch.port)

    # GET LIST OF NODES
    # coordinator    26.2gb
    # secondary     383.7gb
    # spot_47727B30   934gb
    # spot_BB7A8053   934gb
    # primary       638.8gb
    # spot_A9DB0988     5tb
    Log.note("get nodes")

    # stats = http.get_json(path+"/_stats")

    # TODO: PULL DATA ABOUT NODES TO INCLUDE THE USER DEFINED ZONES
    #

    nodes = UniqueIndex("name", list(convert_table_to_list(
        http.get(path + "/_cat/nodes?bytes=b&h=n,r,d,i,hm").content,
        ["name", "role", "disk", "ip", "memory"]
    )))
    if "primary" not in nodes or "secondary" not in nodes:
        Log.error("missing an important index\n{{nodes|json}}", nodes=nodes)

    zones = UniqueIndex("name")
    for z in settings.zones:
        zones.add(z)

    risky_zone_names = set(z.name for z in settings.zones if z.risky)

    for n in nodes:
        if n.role == 'd':
            n.disk = 0 if n.disk == "" else float(n.disk)
            n.memory = text_to_bytes(n.memory)
        else:
            n.disk = 0
            n.memory = 0

        if n.name.startswith("spot_") or n.name.startswith("coord"):
            n.zone = zones["spot"]
        else:
            n.zone = zones["primary"]

    for g, siblings in jx.groupby(nodes, "zone.name"):
        siblings = list(siblings)
        siblings = wrap(filter(lambda n: n.role == "d", siblings))
        for s in siblings:
            s.siblings = len(siblings)
            s.zone.memory = Math.sum(siblings.memory)

    Log.note("{{num}} nodes", num=len(nodes))

    # Log.note("Nodes:\n{{nodes}}", nodes=list(nodes))

    # GET LIST OF SHARDS, WITH STATUS
    # debug20150915_172538                0  p STARTED        37319   9.6mb 172.31.0.196 primary
    # debug20150915_172538                0  r UNASSIGNED
    # debug20150915_172538                1  p STARTED        37624   9.6mb 172.31.0.39  secondary
    # debug20150915_172538                1  r UNASSIGNED
    shards = wrap(list(convert_table_to_list(http.get(path + "/_cat/shards").content,
                                             ["index", "i", "type", "status", "num", "size", "ip", "node"])))
    for s in shards:
        s.i = int(s.i)
        s.size = text_to_bytes(s.size)
        if s.node.find(" -> ") != -1:
            m = s.node.split(" -> ")
            s.node = m[0]  # <from> " -> " <to> format
            CURRENT_MOVING_SHARDS.append({
                "index": s.index,
                "shard": s.i,
                "from_node": m[0],
                "to_node": m[1].split(" ")[-1]
            })
        s.node = nodes[s.node]

    # TODO: MAKE ZONE OBJECTS TO STORE THE NUMBER OF REPLICAS

    # ASSIGN SIZE TO ALL SHARDS
    for g, replicas in jx.groupby(shards, ["index", "i"]):
        replicas = wrap(list(replicas))
        size = Math.MAX(replicas.size)
        for r in replicas:
            r.size = size

    # AN "ALLOCATION" IS THE SET OF SHARDS FOR ONE INDEX ON ONE NODE
    # CALCULATE HOW MANY SHARDS SHOULD BE IN EACH ALLOCATION
    allocation = UniqueIndex(["index", "node.name"])

    for g, replicas in jx.groupby(shards, "index"):
        replicas = wrap(list(replicas))
        num_primaries = len(filter(lambda r: r.type == 'p', replicas))

        multiplier = Math.MAX(settings.zones.shards)
        num_replicas = len(settings.zones) * multiplier
        if len(replicas)/num_primaries < num_replicas:
            # DECREASE NUMBER OF REQUIRED REPLICAS
            response = http.put(path + "/" + g.index + "/_settings", json={"index.recovery.initial_shards": 1})
            Log.note("Number of shards required {{index}}\n{{result}}", index=g.index, result=convert.json2value(convert.utf82unicode(response.content)))

            # INCREASE NUMBER OF REPLICAS
            response = http.put(path + "/" + g.index + "/_settings", json={"index": {"number_of_replicas": num_replicas-1}})
            Log.note("Update replicas for {{index}}\n{{result}}", index=g.index, result=convert.json2value(convert.utf82unicode(response.content)))

        for n in nodes:
            if n.role == 'd':
                max_allowed = Math.ceiling((n.memory / n.zone.memory) * (n.zone.shards * num_primaries))
            else:
                max_allowed = 0

            allocation.add({
                "index": g.index,
                "node": n,
                "max_allowed": max_allowed,
                "shards": filter(lambda r: r.node.name == n.name and r.index == g.index, shards)
            })

        index_size = Math.sum(replicas.size)
        for r in replicas:
            r.index_size = index_size
            r.siblings = num_primaries

    relocating = wrap([s for s in shards if s.status in ("RELOCATING", "INITIALIZING")])
    Log.note("{{num}} shards in motion", num=len(relocating))
    # for r in relocating:
    #     cancel(path, r)

    for m in copy(CURRENT_MOVING_SHARDS):
        for s in shards:
            if s.index == m.index and s.i == m.shard and s.node.name == m.to_node and s.status == "STARTED":
                # FINISHED MOVE
                CURRENT_MOVING_SHARDS.remove(m)
                break
            elif s.index == m.index and s.i == m.shard and s.node.name == m.from_node and s.status == "RELOCATING":
                # STILL MOVING, ADD A VIRTUAL SHARD TO REPRESENT THE DESTINATION OF RELOCATION
                s = copy(s)
                s.node = nodes[m.to_node]
                s.status = "INITIALIZING"
                relocating.append(s)
                break
        else:
            # COULD NOT BE FOUND
            CURRENT_MOVING_SHARDS.remove(m)

    # LOOKING FOR SHARDS WITH ZERO INSTANCES, IN THE spot ZONE
    not_started = []
    for g, replicas in jx.groupby(shards, ["index", "i"]):
        replicas = list(replicas)
        started_replicas = list(set([s.zone.name for s in replicas if s.status in {"STARTED", "RELOCATING"}]))
        if len(started_replicas) == 0:
            # MARK NODE AS RISKY
            for s in replicas:
                if s.status == "UNASSIGNED":
                    not_started.append(s)
                    break  # ONLY NEED ONE
    if not_started:
        # TODO: CANCEL ANYTHING MOVING IN SPOT
        Log.note("{{num}} shards have not started", num=len(not_started))
        Log.warning("Shards not started!!\n{{shards|json|indent}}", shards=not_started)
        initailizing_indexes = set(relocating.index)
        busy = [n for n in not_started if n.index in initailizing_indexes]
        please_initialize = [n for n in not_started if n.index not in initailizing_indexes]
        if len(busy) > 1:
            # WE GET HERE WHEN AN IMPORTANT NODE IS WARMING UP ITS SHARDS
            # SINCE WE CAN NOT RECOGNIZE THE ASSIGNMENT THAT WE MAY HAVE REQUESTED LAST ITERATION
            Log.note("Delay work, cluster busy RELOCATING/INITIALIZING {{num}} shards", num=len(relocating))
        allocate(30, please_initialize, set(n.zone.name for n in nodes) - risky_zone_names, "not started")
    else:
        Log.note("All shards have started")

    # LOOKING FOR SHARDS WITH ONLY ONE INSTANCE, IN THE RISKY ZONES
    high_risk_shards = []
    for g, replicas in jx.groupby(shards, ["index", "i"]):
        # TODO: CANCEL ANYTHING MOVING IN SPOT
        replicas = list(replicas)
        realized_zone_names = set([s.node.zone.name for s in replicas if s.status in {"STARTED", "RELOCATING"}])
        if len(realized_zone_names-risky_zone_names) == 0:
            # MARK NODE AS RISKY
            for s in replicas:
                if s.status == "UNASSIGNED":
                    high_risk_shards.append(s)
                    break  # ONLY NEED ONE
    if high_risk_shards:
        Log.note("{{num}} high risk shards found", num=len(high_risk_shards))
        allocate(10, high_risk_shards, set(n.zone for n in nodes.zone) - risky_zone_names, "high risk shards")
    else:
        Log.note("No high risk shards found")

    # LOOK FOR SHARDS WE CAN MOVE TO SPOT
    # THIS HAPPENS WHEN THE ES SHARD LOGIC ASSIGNED TOO MANY REPLICAS TO A SINGLE ZONE
    over_allocated_shards = Dict()
    for g, replicas in jx.groupby(shards, ["index", "i"]):
        replicas = wrap(list(replicas))
        for z in zones:
            safe_replicas = filter(lambda r: r.status == "STARTED" and r.node.zone.name == z.name, replicas)
            if len(safe_replicas) > z.shards:
                # IS THERE A PLACE TO PUT IT?
                best_zone = None
                for possible_zone in zones:
                    if possible_zone == z:
                        continue
                    number_of_shards = len(filter(
                        lambda r: r.status in {"INITIALIZING", "STARTED", "RELOCATING"} and r.node.zone.name==possible_zone.name,
                        replicas
                    ))
                    if not best_zone or best_zone[1] > number_of_shards:
                        best_zone = possible_zone, number_of_shards
                    if zones[possible_zone].shards > number_of_shards:
                        # TODO: NEED BETTER CHOOSER; NODE WITH MOST SHARDS
                        i = Random.weight([r.siblings for r in safe_replicas])
                        shard = safe_replicas[i]
                        over_allocated_shards[possible_zone.name] += [shard]
                        break
                else:
                    i = Random.weight([r.siblings for r in safe_replicas])
                    shard = safe_replicas[i]
                    over_allocated_shards[best_zone[0].name] += [shard]


    if over_allocated_shards:
        for z, v in over_allocated_shards.items():
            Log.note("{{num}} shards can be moved to {{zone}}", num=len(over_allocated_shards), zone=z)
            allocate(CONCURRENT, v, {z}, "over allocated")
    else:
        Log.note("No over-allocated shard found")

    # MOVE SHARDS OUT OF FULL NODES (BIGGEST TO SMALLEST)


    # LOOK FOR DUPLICATION OPPORTUNITIES
    # IN THEORY THIS IS FASTER BECAUSE THEY ARE IN THE SAME ZONE (AND BETTER MACHINES)
    dup_shards = Dict()
    for g, replicas in jx.groupby(shards, ["index", "i"]):
        replicas = wrap(list(replicas))

        # WE CAN ASSIGN THIS REPLICA TO spot
        for s in replicas:
            if s.status != "UNASSIGNED":
                continue
            for z in settings.zones:
                started_count = len([r for r in replicas if r.status in {"STARTED", "RELOCATING"} and r.node.zone.name==z.name])
                active_count = len([r for r in replicas if r.status in {"INITIALIZING", "STARTED", "RELOCATING"} and r.node.zone.name==z.name])
                if started_count >= 1 and active_count < z.shards:
                    s.status = "INITIALIZING"
                    dup_shards[z.name] += [s]
            break  # ONLY ONE SHARD PER CYCLE

    if dup_shards:
        for zone_name, assign in dup_shards.items():
            Log.note("{{num}} shards can be duplicated in the {{zone}} zone", num=len(assign), zone=zone_name)
            allocate(CONCURRENT, assign, {zone_name}, "duplicate shards")
    else:
        Log.note("No duplicate shards left to assign")

    # LOOK FOR UNALLOCATED SHARDS
    low_risk_shards = Dict()
    for g, replicas in jx.groupby(shards, ["index", "i"]):
        replicas = wrap(list(replicas))

        # WE CAN ASSIGN THIS REPLICA TO spot
        for s in replicas:
            if s.status != "UNASSIGNED":
                continue
            for z in settings.zones:
                active_count = len([r for r in replicas if r.status in {"INITIALIZING", "STARTED", "RELOCATING"} and r.node.zone.name==z.name])
                if active_count < 1:
                    low_risk_shards[z.name] += [s]
            break  # ONLY ONE SHARD PER CYCLE

    if low_risk_shards:
        for zone_name, assign in low_risk_shards.items():
            Log.note("{{num}} low risk shards can be assigned to {{zone}} zone", num=len(assign), zone=zone_name)
            allocate(CONCURRENT, assign, {zone_name}, "low risk shards")
    else:
        Log.note("No low risk shards found")

    # LOOK FOR SHARD IMBALANCE
    not_balanced = Dict()
    for g, replicas in jx.groupby(filter(lambda r: r.status == "STARTED", shards), ["node.name", "index"]):
        replicas = list(replicas)
        if not g.node:
            continue
        _node = nodes[g.node.name]
        alloc = allocation[g]
        for i in range(alloc.max_allowed, len(replicas), 1):
            i = Random.int(len(replicas))
            shard = replicas[i]
            replicas.remove(shard)
            not_balanced[_node.zone.name] += [shard]

    if not_balanced:
        for z, b in not_balanced.items():
            Log.note("{{num}} shards can be moved to better location within {{zone|quote}} zone", zone=z, num=len(b))
            allocate(CONCURRENT, b, {z}, "not balanced")
    else:
        Log.note("No shards need to move")

    _allocate(relocating, path, nodes, shards, allocation)


def reset_node(node):
    # FIND THE IP
    # VERIFY IT IS UNRESPONSIVE
    # LOG IN, AND RESET

    pass


ALLOCATION_REQUESTS = []


def allocate(concurrent, proposed_shards, zones, reason):
    ALLOCATION_REQUESTS.extend([
        {
            "shard": s,
            "to_zone": zones,
            "concurrent": concurrent,
            "reason": reason
        }
        for s in proposed_shards
    ])


def net_shards_to_move(concurrent, shards, relocating):
    sorted_shards = jx.sort(shards, ["index_size", "size"])
    total_size = 0
    for s in sorted_shards:
        if total_size > BIG_SHARD_SIZE:
            break
        concurrent += 1
        total_size += s.size
    concurrent = max(concurrent, CONCURRENT)
    net = concurrent - len(relocating)
    return net, sorted_shards


def _allocate(relocating, path, nodes, all_shards, allocation):
    moves = jx.sort(ALLOCATION_REQUESTS, ["shard.index_size", "shard.size"])

    busy_nodes = Dict()
    for s in relocating:
        if s.status == "INITIALIZING":
            busy_nodes[s.node.name] += 1

    for move in moves:
        shard = move.shard
        zones = move.to_zone

        shards_for_this_index = wrap(jx.filter(all_shards, {
            "eq": {
                "index": shard.index
            }
        }))
        index_size = Math.sum(shards_for_this_index.size)
        existing_on_nodes = set(s.node.name for s in shards_for_this_index if s.status in {"INITIALIZING", "STARTED", "RELOCATING"} and s.i==shard.i)
        # FOR THE NODES WITH NO SHARDS, GIVE A DEFAULT VALUES
        node_weight = {
            n.name: coalesce(n.memory, 0)
            for n in nodes
        }
        for g, ss in jx.groupby(filter(lambda s: s.status == "STARTED" and s.node, shards_for_this_index), "node.name"):
            ss = wrap(list(ss))
            index_count = len(ss)
            node_weight[g.node.name] = nodes[g.node.name].memory * (1 - Math.sum(ss.size)/index_size)
            max_allowed = allocation[shard.index, g.node.name].max_allowed
            node_weight[g.node.name] *= 4 ** Math.MIN([0, max_allowed - index_count - 1])

        list_nodes = list(nodes)
        list_node_weight = [node_weight[n.name] for n in list_nodes]
        for i, n in enumerate(list_nodes):
            if n.zone.name not in zones:
                list_node_weight[i] = 0
            elif n.name in existing_on_nodes:
                list_node_weight[i] = 0
            elif busy_nodes[n.name] >= move.concurrent:
                list_node_weight[i] = 0

        if Math.sum(list_node_weight) == 0:
            continue  # NO SHARDS CAN ACCEPT THIS

        while True:
            i = Random.weight(list_node_weight)
            destination_node = list_nodes[i].name
            for s in all_shards:
                if s.index == shard.index and s.i == shard.i and s.node.name == destination_node:
                    Log.error(
                        "SHOULD NEVER HAPPEN Shard {{shard.index}}:{{shard.i}} already on node {{node}}",
                        shard=shard,
                        node=destination_node
                    )
                    break
            else:
                break

        if shard.status == "UNASSIGNED":
            # destination_node = "secondary"
            command = wrap({"allocate": {
                "index": shard.index,
                "shard": shard.i,
                "node": destination_node,  # nodes[i].name,
                "allow_primary": True
            }})
        else:
            _move = {
                "index": shard.index,
                "shard": shard.i,
                "from_node": shard.node.name,
                "to_node": destination_node
            }
            CURRENT_MOVING_SHARDS.append(_move)
            command = wrap({"move": _move})
        Log.note(
            "{{motivation}}: {{mode|upper}} index={{shard.index}}, shard={{shard.i}}, assign_to={{node}}",
            mode=list(command.keys())[0],
            motivation=move.reason,
            shard=shard,
            node=destination_node
        )

        response = http.post(path + "/_cluster/reroute", json={"commands": [command]})
        result = convert.json2value(convert.utf82unicode(response.content))
        if response.status_code not in [200, 201] or not result.acknowledged:
            main_reason = strings.between(result.error, "[NO", "]")

            if move.reason == "over allocated" and main_reason and main_reason.find("too many shards on nodes for attribute") != -1:
                pass  # THIS WILL HAPPEN WHEN THE ES SHARD BALANCER IS ACTIVATED, NOTHING WE CAN DO
            else:
                Log.warning(
                    "{{code}} Can not move/allocate:\n\treason={{reason}}\n\tdetails={{error|quote}}",
                    code=response.status_code,
                    reason=main_reason,
                    error=result.error
                )
        else:
            busy_nodes[destination_node] += 1
            Log.note(
                "ok={{result.acknowledged}}",
                result=result
            )
    Log.note("Done making moves")


def cancel(path, shard):
    json = {"commands": [{"cancel": {
        "index": shard.index,
        "shard": shard.i,
        "node": shard.node.name
    }}]}
    result = convert.json2value(
        convert.utf82unicode(http.post(path + "/_cluster/reroute", json=json).content)
    )
    if not result.acknowledged:
        main_reason = strings.between(result.error, "[NO", "]")
        Log.warning(
            "Can not cancel from {{node}}:\n\treason={{reason}}\n\tdetails={{error|quote}}",
            reason=main_reason,
            node=shard.node.name,
            error=result.error
        )
    else:
        Log.note(
            "index={{shard.index}}, shard={{shard.i}}, assign_to={{node}}, ok={{result.acknowledged}}",
            shard=shard,
            result=result,
            node=shard.node.name
        )

    Log.note("All moves made")


def balance_multiplier(shard_count, node_count):
    return 10 ** (Math.floor(shard_count / node_count + 0.9)-1)


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
    if size == "":
        return 0

    multiplier = {
        "kb": 1000,
        "mb": 1000000,
        "gb": 1000000000
    }.get(size[-2:])
    if not multiplier:
        multiplier = 1
        if size[-1]=="b":
            size = size[:-1]
    else:
        size = size[:-2]
    try:
        return float(size) * float(multiplier)
    except Exception, e:
        Log.error("not expected", cause=e)


def main():
    settings = startup.read_settings()
    Log.start(settings.debug)

    constants.set(settings.constants)
    path = settings.elasticsearch.host + ":" + unicode(settings.elasticsearch.port)

    try:
        response = http.put(
            path + "/_cluster/settings",
            data='{"persistent": {"index.recovery.initial_shards": 1, "action.write_consistency": 1}}'
        )
        Log.note("ONE SHARD IS ENOUGH: {{result}}", result=response.all_content)

        response = http.put(
            path + "/_cluster/settings",
            data='{"persistent": {"cluster.routing.allocation.enable": "none"}}'
        )
        Log.note("DISABLE SHARD MOVEMENT: {{result}}", result=response.all_content)

        response = http.put(
            path + "/_cluster/settings",
            data='{"transient": {"cluster.routing.allocation.disk.watermark.low": "95%"}}'
        )
        Log.note("ALLOW ALLOCATION: {{result}}", result=response.all_content)

        please_stop = Signal()
        def loop(please_stop):
            try:
                while not please_stop:
                    assign_shards(settings)
                    Thread.sleep(seconds=30, please_stop=please_stop)
            except Exception, e:
                Log.error("Not expected", cause=e)
            finally:
                please_stop.go()

        Thread.run("loop", loop, please_stop=please_stop)
        Thread.wait_for_shutdown_signal(please_stop=please_stop, allow_exit=True)
    except Exception, e:
        Log.error("Problem with assign of shards", e)
    finally:
        response = http.put(
            path + "/_cluster/settings",
            data='{"transient": {"cluster.routing.allocation.disk.watermark.low": "40%"}}'
        )
        Log.note("RESTRICT ALLOCATION: {{result}}", result=response.all_content)

        response = http.put(
            path + "/_cluster/settings",
            data='{"persistent": {"cluster.routing.allocation.enable": "all"}}'
        )
        Log.note("ENABLE SHARD MOVEMENT: {{result}}", result=response.all_content)

        Log.stop()


if __name__ == "__main__":
    main()
