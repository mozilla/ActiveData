
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

import sys

from pyLibrary import convert, strings
from pyLibrary.debugs import constants
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap, Dict, coalesce, DictList, listwrap, unwrap, set_default
from pyLibrary.env import http
from pyLibrary.maths import Math
from pyLibrary.maths.randoms import Random
from pyLibrary.queries import jx
from pyLibrary.queries.unique_index import UniqueIndex
from pyLibrary.thread.threads import Thread, Signal

DEBUG = True

CONCURRENT = 1  # NUMBER OF SHARDS TO MOVE CONCURRENTLY, PER NODE
BILLION = 1024 * 1024 * 1024
BIG_SHARD_SIZE = 2 * BILLION  # SIZE WHEN WE SHOULD BE MOVING ONLY ONE SHARD AT A TIME

current_moving_shards = DictList()  # BECAUSE ES WILL NOT TELL US WHERE THE SHARDS ARE MOVING TO

DEAD = "DEAD"
ALIVE = "ALIVE"
last_known_node_status = Dict()


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

    zones = UniqueIndex("name")
    for z in settings.zones:
        zones.add(z)

    stats = http.get_json(path+"/_nodes/stats?all=true")
    nodes = UniqueIndex("name", [
        {
            "name": n.name,
            "ip": n.host[3:].replace("-", "."),
            "role": "-" if n.attributes.data == 'false' else "d",
            "zone": zones[n.attributes.zone],
            "memory": n.jvm.mem.heap_max_in_bytes,
            "disk": n.fs.total.total_in_bytes,
            "disk_free": n.fs.total.available_in_bytes
        }
        for k, n in stats.nodes.items()
    ])
    # if "primary" not in nodes or "secondary" not in nodes:
    #     Log.error("missing an important index\n{{nodes|json}}", nodes=nodes)

    risky_zone_names = set(z.name for z in settings.zones if z.risky)

    # REVIEW NODE STATUS, AND ANY CHANGES
    first_run = not last_known_node_status
    for n in nodes:
        status, last_known_node_status[n.name] = last_known_node_status[n.name], ALIVE
        if status == DEAD:
            Log.warning("Node {{node}} came back to life!", node=n.name)
        elif status == None and not first_run:
            Log.alert("New node {{node}}!", node=n.name)

        if not n.zone:
            Log.error("Expecting all nodes to have a zone")
        if n.role == 'd':
            n.disk = 0 if n.disk == "" else float(n.disk)
        else:
            n.disk = 0
            n.memory = 0
    for n, status in last_known_node_status.copy().items():
        if not nodes[n] and status == ALIVE:
            Log.alert("Lost node {{node}}", node=n)
            last_known_node_status[n] = DEAD

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
    shards = wrap(list(convert_table_to_list(
        http.get(path + "/_cat/shards").content,
        ["index", "i", "type", "status", "num", "size", "ip", "node"]
    )))
    current_moving_shards.__clear__()
    for s in shards:
        s.i = int(s.i)
        s.size = text_to_bytes(s.size)
        if s.node.find(" -> ") != -1:
            m = s.node.split(" -> ")
            s.node = m[0]  # <from> " -> " <to> format
            destination = m[1].split(" ")[-1]
            if nodes[destination]:
                destination = nodes[destination]
            else:
                for n in nodes:
                    if n.ip == destination:
                        destination = n
                        break

            current_moving_shards.append({
                "index": s.index,
                "shard": s.i,
                "from_node": m[0],
                "to_node": destination.name
            })
            # shards.append(set_default({"node": destination}, s))
        s.node = nodes[s.node]

    # Log.note("{{shards}}", shards=shards)
    Log.note("{{num}} shards moving", num=len(current_moving_shards))

    # TODO: MAKE ZONE OBJECTS TO STORE THE NUMBER OF REPLICAS

    # ASSIGN SIZE TO ALL SHARDS
    for g, replicas in jx.groupby(shards, ["index", "i"]):
        replicas = wrap(list(replicas))
        size = Math.MAX(replicas.size)
        for r in replicas:
            r.size = size

    relocating = wrap([s for s in shards if s.status in ("RELOCATING", "INITIALIZING")])
    Log.note("{{num}} shards in motion", num=len(relocating))

    for m in copy(current_moving_shards):
        for s in shards:
            if s.index == m.index and s.i == m.shard and s.node.name == m.to_node and s.status == "STARTED":
                # FINISHED MOVE
                current_moving_shards.remove(m)
                break
            elif s.index == m.index and s.i == m.shard and s.node.name == m.from_node and s.status == "RELOCATING":
                # STILL MOVING, ADD A VIRTUAL SHARD TO REPRESENT THE DESTINATION OF RELOCATION
                s = copy(s)
                s.type = 'r'
                s.node = nodes[m.to_node]
                s.status = "INITIALIZING"
                if s.node:  # HAPPENS WHEN SENDING SHARD TO UNKNOWN
                    relocating.append(s)
                    shards.append(s)  # SORRY, BUT MOVING SHARDS TAKE TWO SPOTS
                break
        else:
            # COULD NOT BE FOUND
            current_moving_shards.remove(m)

    # AN "ALLOCATION" IS THE SET OF SHARDS FOR ONE INDEX ON ONE NODE
    # CALCULATE HOW MANY SHARDS SHOULD BE IN EACH ALLOCATION
    allocation = UniqueIndex(["index", "node.name"])

    for g, replicas in jx.groupby(shards, "index"):
        Log.note("review replicas of {{index}}", index=g.index)
        replicas = wrap(list(replicas))
        num_primaries = len(filter(lambda r: r.type == 'p', replicas))

        multiplier = Math.MAX(settings.zones.shards)
        num_replicas = len(settings.zones) * multiplier
        if float(len(replicas)) / float(num_primaries) < num_replicas:
            # DECREASE NUMBER OF REQUIRED REPLICAS
            response = http.put(path + "/" + g.index + "/_settings", json={"index.recovery.initial_shards": 1})
            Log.note("Number of shards required {{index}}\n{{result}}", index=g.index, result=convert.json2value(convert.utf82unicode(response.content)))

            # INCREASE NUMBER OF REPLICAS
            response = http.put(path + "/" + g.index + "/_settings", json={"index": {"number_of_replicas": num_replicas-1}})
            Log.note("Update replicas for {{index}}\n{{result}}", index=g.index, result=convert.json2value(convert.utf82unicode(response.content)))

        for n in nodes:
            if n.role == 'd':
                pro = (float(n.memory) / float(n.zone.memory)) * (n.zone.shards * num_primaries)
                min_allowed = Math.floor(pro)
                max_allowed = Math.ceiling(pro)
            else:
                min_allowed = 0
                max_allowed = 0

            allocation.add({
                "index": g.index,
                "node": n,
                "min_allowed": min_allowed,
                "max_allowed": max_allowed,
                "shards": list(filter(lambda r: r.node.name == n.name, replicas))
            })

        index_size = Math.sum(replicas.size)
        for r in replicas:
            r.index_size = index_size
            r.siblings = num_primaries

    del ALLOCATION_REQUESTS[:]

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
        Log.warning("{{num}} shards have not started", num=len(not_started))
        # Log.warning("Shards not started!!\n{{shards|json|indent}}", shards=not_started)
        initailizing_indexes = set(relocating.index)
        busy = [n for n in not_started if n.index in initailizing_indexes]
        please_initialize = [n for n in not_started if n.index not in initailizing_indexes]
        if len(busy) > 1:
            # WE GET HERE WHEN AN IMPORTANT NODE IS WARMING UP ITS SHARDS
            # SINCE WE CAN NOT RECOGNIZE THE ASSIGNMENT THAT WE MAY HAVE REQUESTED LAST ITERATION
            Log.note("Delay work, cluster busy RELOCATING/INITIALIZING {{num}} shards", num=len(relocating))
        allocate(30, please_initialize, set(n.zone.name for n in nodes) - risky_zone_names, "not started", 1, settings)
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
        allocate(10, high_risk_shards, set(n.zone.name for n in nodes) - risky_zone_names, "high risk shards", 2, settings)
    else:
        Log.note("No high risk shards found")

    # THIS HAPPENS WHEN THE ES SHARD LOGIC ASSIGNED TOO MANY REPLICAS TO A SINGLE ZONE
    overloaded_zone_index_pairs = set()
    over_allocated_shards = Dict()
    for g, replicas in jx.groupby(shards, ["index", "i"]):
        replicas = wrap(list(replicas))
        for z in zones:
            safe_replicas = filter(lambda r: r.status == "STARTED" and r.node.zone.name == z.name, replicas)
            if len(safe_replicas) > z.shards:
                overloaded_zone_index_pairs.add((z.name, g.index))
                # IS THERE A PLACE TO PUT IT?
                best_zone = None
                for possible_zone in zones:
                    number_of_shards = len(filter(
                        lambda r: r.status in {"INITIALIZING", "STARTED", "RELOCATING"} and r.node.zone.name == possible_zone.name,
                        replicas
                    ))
                    if not best_zone or (not best_zone[0].risky and z.risky) or (best_zone[1] > number_of_shards and best_zone[0].risky == r.risky):
                        best_zone = possible_zone, number_of_shards
                    if zones[possible_zone].shards > number_of_shards:
                        # TODO: NEED BETTER CHOOSER; NODE WITH MOST SHARDS
                        i = Random.weight([r.siblings for r in safe_replicas])
                        shard = safe_replicas[i]
                        over_allocated_shards[possible_zone.name] += [shard]
                        break
                else:
                    if z == best_zone[0]:
                        continue
                    i = Random.weight([r.siblings for r in safe_replicas])
                    shard = safe_replicas[i]
                    # alloc = allocation[g.index, shard.node.name]
                    potential_peers =filter(
                        lambda r: r.status in {"INITIALIZING", "STARTED", "RELOCATING"} and r.index ==shard.index and r.i==shard.i and r.node.zone==shard.node.zone,
                        shards
                    )
                    if len(potential_peers) >= best_zone[0].shards:
                        continue
                    over_allocated_shards[best_zone[0].name] += [shard]

    if over_allocated_shards:
        for z, v in over_allocated_shards.items():
            Log.note("{{num}} shards can be moved to {{zone}}", num=len(over_allocated_shards), zone=z)
            allocate(CONCURRENT, v, {z}, "over allocated", 3, settings)
    else:
        Log.note("No over-allocated shard found")

    # MOVE SHARDS OUT OF FULL NODES (BIGGEST TO SMALLEST)


    # LOOK FOR DUPLICATION OPPORTUNITIES
    # ONLY DUPLICATE PRIMARY SHARDS AT THIS TIME
    # IN THEORY THIS IS FASTER BECAUSE THEY ARE IN THE SAME ZONE (AND BETTER MACHINES)
    dup_shards = Dict()
    for g, replicas in jx.groupby(shards, ["index", "i"]):
        replicas = wrap(list(replicas))
        # WE CAN ASSIGN THIS REPLICA WITHIN THE SAME ZONE
        for s in replicas:
            if s.status != "UNASSIGNED" or s.type != "p":
                continue
            for z in settings.zones:
                started_count = len([r for r in replicas if r.status in {"STARTED"} and r.node.zone.name==z.name])
                active_count = len([r for r in replicas if r.status in {"INITIALIZING", "STARTED", "RELOCATING"} and r.node.zone.name==z.name])
                if started_count >= 1 and active_count < z.shards:
                    dup_shards[z.name] += [s]
            break  # ONLY ONE SHARD PER CYCLE

    if dup_shards:
        for zone_name, assign in dup_shards.items():
            # Log.note("{{dups}}", dups=assign)
            Log.note("{{num}} shards can be duplicated in the {{zone}} zone", num=len(assign), zone=zone_name)
            allocate(CONCURRENT, assign, {zone_name}, "duplicate shards", 5, settings)
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
            allocate(CONCURRENT, assign, {zone_name}, "low risk shards", 4, settings)
    else:
        Log.note("No low risk shards found")

    # LOOK FOR SHARD IMBALANCE
    rebalance_candidates = Dict()
    for g, replicas in jx.groupby(filter(lambda r: r.status == "STARTED", shards), ["node.name", "index"]):
        replicas = list(replicas)
        if not g.node:
            continue
        _node = nodes[g.node.name]
        alloc = allocation[g]
        if (_node.zone.name, g.index) in overloaded_zone_index_pairs:
            continue
        for i in range(alloc.max_allowed, len(replicas), 1):
            i = Random.int(len(replicas))
            shard = replicas[i]
            replicas.remove(shard)
            rebalance_candidates[_node.zone.name] += [shard]

    if rebalance_candidates:
        for z, b in rebalance_candidates.items():
            Log.note("{{num}} shards can be moved to better location within {{zone|quote}} zone", zone=z, num=len(b))
            allocate(CONCURRENT, b, {z}, "not balanced", 6, settings)
    else:
        Log.note("No shards need to be balanced")

    # LOOK FOR OTHER, SLOWER, DUPLICATION OPPORTUNITIES
    dup_shards = Dict()
    for g, replicas in jx.groupby(shards, ["index", "i"]):
        replicas = wrap(list(replicas))
        # WE CAN ASSIGN THIS REPLICA WITHIN THE SAME ZONE
        for s in replicas:
            if s.status != "UNASSIGNED":
                continue
            for z in settings.zones:
                started_count = len([r for r in replicas if r.status in {"STARTED"} and r.node.zone.name==z.name])
                active_count = len([r for r in replicas if r.status in {"INITIALIZING", "STARTED", "RELOCATING"} and r.node.zone.name==z.name])
                if started_count >= 1 and active_count < z.shards:
                    dup_shards[z.name] += [s]
            break  # ONLY ONE SHARD PER CYCLE

    if dup_shards:
        for zone_name, assign in dup_shards.items():
            # Log.note("{{dups}}", dups=assign)
            Log.note("{{num}} shards can be duplicated between zones", num=len(assign))
            allocate(CONCURRENT, assign, {zone_name}, "inter-zone duplicate shards ", 7, settings)
    else:
        Log.note("No duplicate shards left to assign")

    # ENSURE ALL NODES HAVE THE MINIMUM NUMBER OF SHARDS
    total_moves = 0
    for index_name in set(shards.index):
        for z in set([n.zone.name for n in nodes]):
            rebalance_candidate = None  # MOVE ONLY ONE SHARD, PER INDEX, PER ZONE, AT A TIME
            most_shards = 0  # WE WANT TO OFFLOAD THE NODE WITH THE MOST SHARDS
            destination_zone_name = None

            for n in nodes:
                if n.zone.name != z:
                    continue

                alloc = allocation[index_name, n.name]
                if (n.name, index_name) in overloaded_zone_index_pairs:
                    continue
                if not alloc.shards or len(alloc.shards) < alloc.min_allowed:
                    destination_zone_name = z
                    continue
                started_shards = [r for r in alloc.shards if r.status in {"STARTED"}]
                if most_shards >= len(started_shards):
                    continue

                if Math.max(1, alloc.min_allowed) < len(started_shards):
                    shard = started_shards[0]
                    rebalance_candidate = shard
                    most_shards = len(started_shards)

            if destination_zone_name and rebalance_candidate:
                total_moves += 1
                allocate(CONCURRENT, [rebalance_candidate], {destination_zone_name}, "not balanced", 8, settings)
    if total_moves:
        Log.note(
            "{{num}} shards can be moved to better location within their own zone",
            num=total_moves,
        )

    _allocate(relocating, path, nodes, shards, allocation)


def reset_node(node):
    # FIND THE IP
    # VERIFY IT IS UNRESPONSIVE
    # LOG IN, AND RESET

    pass


ALLOCATION_REQUESTS = []


def allocate(concurrent, proposed_shards, zones, reason, mode_priority, settings):
    if DEBUG:
        assert all(isinstance(z, unicode) for z in zones)
    for s in proposed_shards:
        move = {
            "shard": s,
            "to_zone": zones,
            "concurrent": concurrent,
            "reason": reason,
            "mode_priority": mode_priority,
            "replication_priority": replication_priority(s, settings)
        }
        ALLOCATION_REQUESTS.append(move)


def replication_priority(shard, settings):
    for i, prefix in enumerate(settings.replication_priority):
        if prefix.endswith("*") and shard.index.startswith(prefix[:-1]):
            return i
        elif shard.index == prefix:
            return i
    return len(settings.replication_priority)


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
    moves = jx.sort(ALLOCATION_REQUESTS, ["mode_priority", "replication_priority", "shard.index_size", "shard.i"])

    busy_nodes = Dict()
    for s in relocating:
        if s.status == "INITIALIZING":
            busy_nodes[s.node.name] += s.size

    done = set()  # (index, i) pair

    for move in moves:
        shard = move.shard
        if (shard.index, shard.i) in done:
            continue
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
            node_weight[g.node.name] = nodes[g.node.name].memory * (1 - float(Math.sum(ss.size))/float(index_size))
            min_allowed = allocation[shard.index, g.node.name].min_allowed
            node_weight[g.node.name] *= 4 ** Math.MIN([-1, min_allowed - index_count - 1])

        list_nodes = list(nodes)
        list_node_weight = [node_weight[n.name] for n in list_nodes]
        for i, n in enumerate(list_nodes):
            alloc = allocation[shard.index, n.name]

            if n.zone.name not in zones:
                list_node_weight[i] = 0
            elif n.name in existing_on_nodes:
                list_node_weight[i] = 0
            elif busy_nodes[n.name] >= move.concurrent*BIG_SHARD_SIZE:
                list_node_weight[i] = 0
            elif n.disk and float(n.disk_free - shard.size)/float(n.disk) < 0.10:
                list_node_weight[i] = 0
            elif len(alloc.shards) >= alloc.max_allowed:
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

        existing = filter(
            lambda r: r.index == shard.index and r.i == shard.i and r.node.name == destination_node and r.status in {"INITIALIZING", "STARTED", "RELOCATING"},
            all_shards
        )
        if len(existing) >= nodes[destination_node].zone.shards:
            Log.error("should nt happen")

        if shard.status == "UNASSIGNED":
            # destination_node = "secondary"
            command = wrap({"allocate": {
                "index": shard.index,
                "shard": shard.i,
                "node": destination_node,  # nodes[i].name,
                "allow_primary": True
            }})
        elif shard.status == "STARTED":
            _move = {
                "index": shard.index,
                "shard": shard.i,
                "from_node": shard.node.name,
                "to_node": destination_node
            }
            current_moving_shards.append(_move)
            command = wrap({"move": _move})
        else:
            Log.error("do not know how to handle")

        Log.note(
            "{{motivation}}: {{mode|upper}} index={{shard.index}}, shard={{shard.i}}, from={{from_node}}, assign_to={{node}}",
            mode=list(command.keys())[0],
            motivation=move.reason,
            shard=shard,
            from_node=shard.node.name,
            node=destination_node
        )

        response = http.post(path + "/_cluster/reroute", json={"commands": [command]})
        result = convert.json2value(convert.utf82unicode(response.content))
        if response.status_code not in [200, 201] or not result.acknowledged:
            main_reason = strings.between(result.error, "[NO", "]")

            if main_reason and main_reason.find("too many shards on nodes for attribute") != -1:
                pass  # THIS WILL HAPPEN WHEN THE ES SHARD BALANCER IS ACTIVATED, NOTHING WE CAN DO
                Log.note("failed: zone full")
            elif main_reason and main_reason.find("after allocation more than allowed") != -1:
                pass
                Log.note("failed: out of space")
            elif "failed to resolve [" in result.error:
                # LOST A NODE WHILE SENDING UPDATES
                lost_node_name = strings.between(result.error, "failed to resolve [", "]").strip()
                Log.warning("Lost node {{node}}", node=lost_node_name)
                nodes[lost_node_name].zone = None
            else:
                Log.warning(
                    "{{code}} Can not move/allocate:\n\treason={{reason}}\n\tdetails={{error|quote}}",
                    code=response.status_code,
                    reason=main_reason,
                    error=result.error
                )
        else:
            if shard.status == "STARTED":
                shard.status = "RELOCATING"
            done.add((shard.index, shard.i))
            busy_nodes[destination_node] += shard.size
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
    return 10 ** (Math.floor(float(shard_count) / float(node_count) + 0.9)-1)


def convert_table_to_list(table, column_names):
    lines = [l for l in table.split("\n") if l.strip()]

    # FIND THE COLUMNS WITH JUST SPACES
    columns = []
    for i, c in enumerate(zip(*lines)):
        if all(r == " " for r in c):
            columns.append(i)

    columns = columns[0:len(column_names)-1]
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
        Log.note("ONE SHARD IS ENOUGH TO ALLOW WRITES: {{result}}", result=response.all_content)

        response = http.put(
            path + "/_cluster/settings",
            data='{"persistent": {"cluster.routing.allocation.enable": "none"}}'
        )
        Log.note("DISABLE SHARD MOVEMENT: {{result}}", result=response.all_content)

        response = http.put(
            path + "/_cluster/settings",
            data='{"transient": {"cluster.routing.allocation.disk.threshold_enabled" : false}}'
        )
        Log.note("ALLOW ALLOCATION: {{result}}", result=response.all_content)

        please_stop = Signal()

        def loop(please_stop):
            while not please_stop:
                try:
                    assign_shards(settings)
                except Exception, e:
                    Log.warning("Not expected", cause=e)
                Thread.sleep(seconds=30, please_stop=please_stop)

        Thread.run("loop", loop, please_stop=please_stop)
        Thread.wait_for_shutdown_signal(please_stop=please_stop, allow_exit=True)
    except Exception, e:
        Log.error("Problem with assign of shards", e)
    finally:
        for p, command in settings["finally"].items():
            for c in listwrap(command):
                response = http.put(
                    path + p,
                    data=convert.value2json(c)
                )
                Log.note("Finally {{command}}\n{{result}}", command=c, result=response.all_content)

        Log.stop()


if __name__ == "__main__":
    main()
