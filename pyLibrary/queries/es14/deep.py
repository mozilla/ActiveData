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
from pyLibrary import queries
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import split_field, DictList, listwrap, literal_field, wrap, coalesce, Dict, set_default, unwrap
from pyLibrary.queries import es09, es14
from pyLibrary.queries.domains import is_keyword
from pyLibrary.queries.es14.setop import format_dispatch
from pyLibrary.queries.es14.util import qb_sort_to_es_sort

from pyLibrary.queries.expressions import query_get_all_vars, qb_expression_to_ruby, expression_map, qb_expression_to_esfilter, split_expression_by_depth, simplify_esfilter
from pyLibrary.queries.unique_index import UniqueIndex
from pyLibrary.thread.threads import Thread
from pyLibrary.times.timer import Timer


def is_deepop(es, query):
    if query.edges or query.groupby:
        return False
    if all(s.aggregate not in (None, "count", "none") for s in listwrap(query.select)):
        return False

    vars = query_get_all_vars(query)
    columns = query.frum.get_columns()
    if len(split_field(query.frum.name)) > 1:
        return True
    if any(c for c in columns if c.nested_path and c.name in vars):
        return True
    return False


def es_deepop(es, query):
    columns = query.frum.get_columns()
    query_path = query.frum.query_path
    columns = UniqueIndex(keys=["name"], data=sorted(columns, lambda a, b: cmp(len(listwrap(b.nested_path)), len(listwrap(a.nested_path)))), fail_on_dup=False)
    _map = {c.name: c.abs_name for c in columns}
    where = expression_map(_map, query.where)

    es_query, es_filters = es14.util.es_query_template(query.frum.name)

    # SPLIT WHERE CLAUSE BY DEPTH
    wheres = split_expression_by_depth(where, query.frum)
    for i, f in enumerate(es_filters):
        # PROBLEM IS {"match_all": {}} DOES NOT SURVIVE set_default()
        for k, v in unwrap(simplify_esfilter(qb_expression_to_esfilter(wheres[i]))).items():
            f[k] = v


    if not wheres[1]:
        more_filter = {
            "and": [
                qb_expression_to_esfilter({"and": wheres[0]}),
                {"not": {
                    "nested": {
                        "path": query_path,
                        "filter": {
                            "match_all": {}
                        }
                    }
                }}
            ]
        }
    else:
        more_filter = None

    es_query.size = coalesce(query.limit, queries.query.DEFAULT_LIMIT)
    es_query.sort = qb_sort_to_es_sort(query.sort)

    is_list = isinstance(query.select, list)
    new_select = DictList()

    def get_pull(column):
        if column.nested_path:
            return "_inner" + column.abs_name[len(listwrap(column.nested_path)[0]):]
        else:
            return "fields." + literal_field(column.abs_name)

    i = 0
    for s in listwrap(query.select):
        if s.value == "*":
            # IF THERE IS A *, THEN INSERT THE EXTRA COLUMNS
            for c in columns:
                if c.relative and c.type not in ["nested", "object"]:
                    if not c.nested_path:
                        es_query.fields += [c.abs_name]
                    new_select.append({
                        "name": c.name,
                        "pull": get_pull(c),
                        "nested_path": listwrap(c.nested_path)[0],
                        "put": {"name": c.name, "index": i, "child": "."}
                    })
                    i += 1

            # REMOVE DOTS IN PREFIX IF NAME NOT AMBIGUOUS
            col_names = [c.name for c in columns if c.relative]
            for n in new_select:
                if n.name.startswith("..") and n.name.lstrip(".") not in col_names:
                    n.name = n.put.name = n.name.lstrip(".")
        elif s.value == ".":
            for c in columns:
                if c.relative and c.type not in ["nested", "object"]:
                    if not c.nested_path:
                        es_query.fields += [c.abs_name]
                    new_select.append({
                        "name": c.name,
                        "pull": get_pull(c),
                        "nested_path": listwrap(c.nested_path)[0],
                        "put": {"name": ".", "index": i, "child": c.abs_name}
                    })
            i += 1
        elif isinstance(s.value, basestring) and s.value.endswith(".*") and is_keyword(s.value[:-2]):
            parent = s.value[:-1]
            prefix = len(parent)
            for c in columns:
                if c.name.startswith(parent):
                    pull = get_pull(c)
                    Log.error("what's this?!!")
                    if len(listwrap(c.nested_path)) < 0:
                        es_query.fields [c.abs_name]
                    new_select.append({
                        "name": s.name + "." + c.name[prefix:],
                        "pull": pull,
                        "nested_path": listwrap(c.nested_path)[0],
                        "put": {"name": s.name + "." + c[prefix:], "index": i, "child": "."}
                    })
        elif isinstance(s.value, basestring) and is_keyword(s.value):
            parent = s.value + "."
            prefix = len(parent)
            net_columns = [c for c in columns if c.name.startswith(parent)]
            if not net_columns:
                c = columns[(s.value,)]
                pull = get_pull(c)
                if not c.nested_path:
                    es_query.fields += [s.value]
                new_select.append({
                    "name": s.name if is_list else ".",
                    "pull": pull,
                    "nested_path": listwrap(c.nested_path)[0],
                    "put": {"name": s.name, "index": i, "child": "."}
                })
            else:
                for n in net_columns:
                    pull = get_pull(n)
                    if not n.nested_path:
                        es_query.fields += [n.abs_name]
                    new_select.append({
                        "name": s.name if is_list else ".",
                        "pull": pull,
                        "nested_path": listwrap(n.nested_path)[0],
                        "put": {"name": s.name, "index": i, "child": n[prefix:]}
                    })
            i += 1
        elif isinstance(s.value, list):
            Log.error("need an example")
            es_query.fields += [v for v in s.value]
        else:
            Log.error("need an example")
            es_query.script_fields[literal_field(s.name)] = {"script": qb_expression_to_ruby(s.value)}
            new_select.append({
                "name": s.name if is_list else ".",
                "value": s.name,
                "put": {"name": s.name, "index": i, "child": "."}
            })
            i += 1

    # <COMPLICATED> ES needs two calls to get all documents
    more = []
    def get_more(please_stop):
        more.append(es09.util.post(
            es,
            Dict(
                filter=more_filter,
                fields=es_query.fields
            ),
            query.limit
        ))
    if more_filter:
        need_more = Thread.run("get more", target=get_more)

    with Timer("call to ES") as call_timer:
        data = es09.util.post(es, es_query, query.limit)

    # EACH A HIT IS RETURNED MULTIPLE TIMES FOR EACH INNER HIT, WITH INNER HIT INCLUDED
    def inners():
        for t in data.hits.hits:
            for i in t.inner_hits[literal_field(query_path)].hits.hits:
                t._inner = i._source
                yield t
        if more_filter:
            Thread.join(need_more)
            for t in more[0].hits.hits:
                yield t
    #</COMPLICATED>

    try:
        formatter, groupby_formatter, mime_type = format_dispatch[query.format]

        output = formatter(inners(), new_select, query)
        output.meta.es_response_time = call_timer.duration
        output.meta.content_type = mime_type
        output.meta.es_query = es_query
        return output
    except Exception, e:
        Log.error("problem formatting", e)
