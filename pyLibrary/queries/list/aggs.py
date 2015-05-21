# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
import itertools

from pyLibrary.collections.matrix import Matrix
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import listwrap, unwrap, set_default
from pyLibrary.queries import windows
from pyLibrary.queries.cube import Cube
from pyLibrary.queries.domains import SimpleSetDomain, DefaultDomain
# from pyLibrary.queries.py.util import util_filter
from pyLibrary.queries.expressions import qb_expression_to_function
from pyLibrary.queries.query import qb


def is_aggs(query):
    if query.edges or query.groupby or any(a != None and a != "none" for a in listwrap(query.select).aggregate):
        return True
    return False


def list_aggs(frum, query):
    select = listwrap(query.select)

    is_join = False  # True IF MANY TO MANY JOIN WITH AN EDGE
    for e in query.edges:
        if isinstance(e.domain, DefaultDomain):
            e.domain = SimpleSetDomain(partitions=list(sorted(set(frum.select(e.value)))))

    for s in listwrap(query.select):
        s["exec"] = qb_expression_to_function(s.value)

    result = {
        s.name: Matrix(
            dims=[len(e.domain.partitions) + (1 if e.allowNulls else 0) for e in query.edges],
            zeros=s.aggregate == "count"
        )
        for s in select
    }
    where = qb_expression_to_function(query.where)
    for d in filter(where, frum):
        d = d.copy()
        coord = []  # LIST OF MATCHING COORDINATE FAMILIES, USUALLY ONLY ONE PER FAMILY BUT JOINS WITH EDGES CAN CAUSE MORE
        for e in query.edges:
            coord.append(get_matches(e, d))

        for s in select:
            mat = result[s.name]
            agg = s.aggregate
            var = s.value
            if agg == "count":
                for c in itertools.product(*coord):
                    if var == "." or var == None:
                        mat[c] += 1
                        continue

                    for e, cc in zip(query.edges, c):
                        d[e.name] = cc
                    val = s["exec"](d, c, frum)
                    if val != None:
                        mat[c] += 1
            else:
                for c in itertools.product(*coord):
                    acc = mat[c]
                    if acc == None:
                        acc = windows.name2accumulator.get(agg)
                        if acc == None:
                            Log.error("select aggregate {{agg}} is not recognized",  agg= agg)
                        acc = acc(**s)
                        mat[c] = acc
                    for e, cc in zip(query.edges, c):  # BECAUSE WE DO NOT KNOW IF s.exec NEEDS THESE EDGES, SO WE PASS THEM ANYWAY
                        d[e.name] = e.domain.partitions[cc]
                    val = s["exec"](d, c, frum)
                    acc.add(val)

    for s in select:
        if s.aggregate == "count":
            continue
        m = result[s.name]
        for c, var in m.items():
            if var != None:
                m[c] = var.end()

    output = Cube(select, query.edges, result)
    return output



def get_matches(e, d):
    if e.value:
        if e.allowNulls:
            return [e.domain.getIndexByKey(d[e.value])]
        else:
            c = e.domain.getIndexByKey(d[e.value])
            if c == len(e.domain.partitions):
                return []
            else:
                return [c]
    elif e.range and e.range.mode == "inclusive":
        for p in e.domain.partitions:
            if p["max"] == None or p["min"] == None:
                Log.error("Inclusive expects domain parts to have `min` and `max` properties")

        output = []
        mi, ma = d[e.range.min], d[e.range.max]
        for p in e.domain.partitions:
            if mi <= p["max"] and p["min"] < ma:
                output.append(p.dataIndex)
        if e.allowNulls and not output:
            output.append(len(e.domain.partitions))  # ENSURE THIS IS NULL
        return output

    elif e.range:
        output = []
        mi, ma = d[e.range.min], d[e.range.max]
        var = e.domain.key
        for p in e.domain.partitions:
            if mi <= p[var] < ma:
                output.append(p.dataIndex)
        if e.allowNulls and not output:
            output.append(len(e.domain.partitions))  # ENSURE THIS IS NULL
        return output
