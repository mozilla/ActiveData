# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from jx_base.expressions import Expression
from jx_elasticsearch.meta import Table
from mo_dots import listwrap, set_default
from mo_future import is_text
from mo_logs import Log
from mo_times import Date

DEBUG = True

COMMON = {}


class QueryStats(object):
    def __new__(cls, cluster):
        existing = COMMON.get(id(cluster))
        if not existing:
            existing = COMMON[id(cluster)] = object.__new__(cls)
        return existing

    def __init__(self, cluster):
        if hasattr(self, "index"):
            return

        self.index = cluster.get_or_create_index(
            index="meta.stats", typed=False, schema=SCHEMA
        )
        self.todo = self.index.threaded_queue(max_size=100, period=60)

    def record(self, query):
        try:
            vars_record = get_stats(query)
            self.todo.extend({"value": v} for v in vars_record)
        except Exception as e:
            Log.warning("problem processing query stats", cause=e)


def get_stats(query):
    frum = query.frum
    if isinstance(frum, Table):
        vars_record = {"table": frum.name}
    elif is_text(frum):
        vars_record = {"table": frum}
    else:
        vars_record = get_stats(frum)
    now = Date.now()
    vars_record['timestamp']=now

    output = []
    for clause in ["select", "edges", "groupby", "window", "sort"]:
        vars_record["mode"] = clause
        for expr in listwrap(getattr(query, clause)):
            if isinstance(expr.value, Expression):
                for v in expr.value.vars():
                    output.append(set_default({"column": v.var}, vars_record))
    for v in query.where.vars():
        output.append(set_default({"column": v.var, "mode": "where"}, vars_record))
    return output


SCHEMA = {
    "settings": {"index.number_of_shards": 1, "index.number_of_replicas": 2},
    "mappings": {"stats": {"properties": {}}},
}


