# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import itertools

from pyLibrary.collections import UNION
from pyLibrary.collections.matrix import Matrix
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import listwrap, wrap
from pyLibrary.queries import windows
from pyLibrary.queries.containers.cube import Cube
from pyLibrary.queries.domains import SimpleSetDomain, DefaultDomain
from pyLibrary.queries.expression_compiler import compile_expression
from pyLibrary.queries.expressions import jx_expression_to_function, jx_expression


def is_aggs(query):
    if query.edges or query.groupby or any(a != None and a != "none" for a in listwrap(query.select).aggregate):
        return True
    return False


def list_aggs(frum, query):
    frum = wrap(frum)
    select = listwrap(query.select)

    for e in query.edges:
        if isinstance(e.domain, DefaultDomain):
            accessor = jx_expression_to_function(e.value)
            unique_values = set(map(accessor, frum))
            if None in unique_values:
                e.allowNulls = True
                unique_values -= {None}
            e.domain = SimpleSetDomain(partitions=list(sorted(unique_values)))

    s_expressions = [jx_expression(s.value) for s in select]
    s_accessors = [compile_expression(e.to_python()) for e in s_expressions]

    result = {
        s.name: Matrix(
            dims=[len(e.domain.partitions) + (1 if e.allowNulls else 0) for e in query.edges],
            zeros=lambda: windows.name2accumulator.get(s.aggregate)(**s)
        )
        for s in select
    }
    where = jx_expression_to_function(query.where)
    coord = [None]*len(query.edges)

    net_new_edge_names = set(wrap(query.edges).name) - UNION(jx_expression(e.value).vars() for e in query.edges)
    if net_new_edge_names & UNION(e.vars() for e in s_expressions):
        # s_accessor NEEDS THESE EDGES, SO WE PASS THEM ANYWAY
        for d in filter(where, frum):
            d = d.copy()
            for c, e in enumerate(query.edges):
                coord[c] = get_matches(e, d)

            for s_accessor, s in zip(s_accessors, select):
                mat = result[s.name]
                for c in itertools.product(*coord):
                    acc = mat[c]
                    for e, cc in zip(query.edges, c):
                        d[e.name] = e.domain.partitions[cc]
                    val = s_accessor(d, c, frum)
                    acc.add(val)
    else:
        # FASTER
        for d in filter(where, frum):
            for c, e in enumerate(query.edges):
                coord[c] = get_matches(e, d)

            for s_accessor, s in zip(s_accessors, select):
                mat = result[s.name]
                for c in itertools.product(*coord):
                    acc = mat[c]
                    val = s_accessor(d, c, frum)
                    acc.add(val)

    for s in select:
        # if s.aggregate == "count":
        #     continue
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
