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

from jx_base import Column, container
from jx_base.container import Container
from jx_base.expressions import jx_expression
from jx_base.language import is_op
from jx_base.expressions import QueryOp
from jx_elasticsearch import elasticsearch
from jx_elasticsearch.es52.agg_bulk import is_bulk_agg, es_bulkaggsop
from jx_elasticsearch.es52.agg_op import es_aggsop, is_aggsop
from jx_elasticsearch.es52.expressions import ES52 as ES52Lang
from jx_elasticsearch.es52.painless import Painless
from jx_elasticsearch.es52.set_bulk import is_bulk_set, es_bulksetop
from jx_elasticsearch.es52.set_op import es_setop, is_setop
from jx_elasticsearch.es52.stats import QueryStats
from jx_elasticsearch.es52.util import aggregates, temper_limit
from jx_elasticsearch.meta import ElasticsearchMetadata, Table
from jx_python import jx
from mo_dots import (
    Data,
    coalesce,
    listwrap,
    split_field,
    startswith_field,
    unwrap,
    to_data,
)
from mo_future import sort_using_key
from mo_http import http
from mo_json import OBJECT, value2json, NESTED
from mo_json.typed_encoder import EXISTS_TYPE
from mo_kwargs import override
from mo_logs import Except, Log
from mo_times import Date


class ES52(Container):
    """
    SEND jx QUERIES TO ElasticSearch
    """

    def __new__(cls, *args, **kwargs):
        if (
            len(args) == 1 and args[0].get("index") == "meta"
        ) or kwargs.get("index") == "meta":
            output = ElasticsearchMetadata.__new__(
                ElasticsearchMetadata, *args, **kwargs
            )
            output.__init__(*args, **kwargs)
            return output
        else:
            return Container.__new__(cls)

    @override
    def __init__(
        self,
        host,
        index,  # THE NAME OF THE SNOWFLAKE (IF WRITING)
        alias=None,  # THE NAME OF THE SNOWFLAKE (FOR READING)
        type=None,
        name=None,  # THE FULL NAME OF THE TABLE (THE NESTED PATH INTO THE SNOWFLAKE)
        port=9200,
        read_only=True,
        timeout=None,  # NUMBER OF SECONDS TO WAIT FOR RESPONSE, OR SECONDS TO WAIT FOR DOWNLOAD (PASSED TO requests)
        wait_for_active_shards=1,  # ES WRITE CONSISTENCY (https://www.elastic.co/guide/en/elasticsearch/reference/1.7/docs-index_.html#index-consistency)
        typed=None,
        kwargs=None,
    ):
        Container.__init__(self)
        if not container.config.default:
            container.config.default = {
                "type": "elasticsearch",
                "settings": unwrap(kwargs),
            }
        self.edges = Data()  # SET EARLY, SO OTHER PROCESSES CAN REQUEST IT
        self.worker = None
        self.settings = kwargs
        self._namespace = ElasticsearchMetadata(kwargs=kwargs)
        self.name = name = self._namespace._find_alias(coalesce(alias, index, name))
        if read_only:
            self.es = elasticsearch.Alias(alias=name, index=None, kwargs=kwargs)
        else:
            self.es = (
                elasticsearch
                .Cluster(kwargs=kwargs)
                .get_index(read_only=read_only, kwargs=kwargs)
            )

        self._ensure_max_result_window_set(name)
        self.settings.type = self.es.settings.type
        self.stats = QueryStats(self.es.cluster)

        columns = self.snowflake.columns  # ABSOLUTE COLUMNS
        is_typed = any(c.es_column == EXISTS_TYPE for c in columns)

        if typed == None:
            # SWITCH ON TYPED MODE
            self.typed = is_typed
        else:
            if is_typed != typed:
                Log.error(
                    "Expecting given typed {{typed}} to match {{is_typed}}",
                    typed=typed,
                    is_typed=is_typed,
                )
            self.typed = typed

        if not typed:
            # ADD EXISTENCE COLUMNS
            all_paths = {".": None}  # MAP FROM path TO parent TO MAKE A TREE

            def nested_path_of(v):
                if v == ".":
                    return (".",)
                return (v,) + nested_path_of(all_paths[v])

            query_paths = sort_using_key(
                set(step for path in self.snowflake.query_paths for step in path),
                key=lambda p: len(split_field(p)),
            )
            for step in query_paths:
                if step in all_paths:
                    continue
                else:
                    best = "."
                    for candidate in all_paths.keys():
                        if startswith_field(step, candidate):
                            if startswith_field(candidate, best):
                                best = candidate
                    all_paths[step] = best
            for p in all_paths.keys():
                if p == ".":
                    nested_path = (".",)
                else:
                    nested_path = nested_path_of(p)[1:]

                jx_type = OBJECT if p == "." else NESTED
                self.namespace.meta.columns.add(Column(
                    name=p,
                    es_column=p,
                    es_index=self.name,
                    es_type=jx_type,
                    jx_type=jx_type,
                    cardinality=1,
                    nested_path=nested_path,
                    multi=1001 if jx_type is NESTED else 1,
                    last_updated=Date.now(),
                ))

    @property
    def snowflake(self):
        return self._namespace.get_snowflake(self.es.settings.alias)

    @property
    def namespace(self):
        return self._namespace

    def get_table(self, full_name):
        return Table(full_name, self)

    def get_schema(self, query_path):
        return self._namespace.get_schema(query_path)

    def __data__(self):
        settings = self.settings.copy()
        settings.settings = None
        return settings

    @property
    def url(self):
        return self.es.url

    def _ensure_max_result_window_set(self, name):
        # TODO : CHECK IF THIS IS ALREADY SET, IT TAKES TOO LONG
        for i, s in self.es.cluster.get_metadata().indices.items():
            if name == i or name in s.aliases:
                if (
                    s.settings.index.max_result_window != "100000"
                    or s.settings.index.max_inner_result_window != "100000"
                ):
                    Log.note("setting max_result_window")
                    self.es.cluster.put(
                        "/" + name + "/_settings",
                        data={"index": {
                            "max_inner_result_window": 100000,
                            "max_result_window": 100000,
                        }},
                    )
                    break

    def query(self, _query):
        try:
            query = QueryOp.wrap(_query, container=self, namespace=self.namespace)

            self.stats.record(query)

            for s in listwrap(query.select):
                if s.aggregate != None and not aggregates.get(s.aggregate):
                    Log.error(
                        "ES can not aggregate {{name}} because {{aggregate|quote}} is"
                        " not a recognized aggregate",
                        name=s.name,
                        aggregate=s.aggregate,
                    )

            frum = query["from"]
            if is_op(frum, QueryOp):
                result = self.query(frum)
                q2 = query.copy()
                q2.frum = result
                return jx.run(q2)

            if is_bulk_agg(self.es, query):
                return es_bulkaggsop(self, frum, query)
            if is_bulk_set(self.es, query):
                return es_bulksetop(self, frum, query)

            query.limit = temper_limit(query.limit, query)

            if is_aggsop(self.es, query):
                return es_aggsop(self.es, frum, query)
            if is_setop(self.es, query):
                return es_setop(self.es, query)
            Log.error("Can not handle")
        except Exception as cause:
            cause = Except.wrap(cause)
            if "Data too large, data for" in cause:
                http.post(self.es.cluster.url / "_cache/clear")
                Log.error("Problem (Tried to clear Elasticsearch cache)", cause)
            Log.error("problem", cause=cause)

    def update(self, command):
        """
        EXPECTING command == {"set":term, "where":where}
        THE set CLAUSE IS A DICT MAPPING NAMES TO VALUES
        THE where CLAUSE IS AN ES FILTER
        """
        command = to_data(command)
        table = self.get_table(command["update"])

        es_index = self.es.cluster.get_index(
            read_only=False, alias=None, kwargs=self.es.settings
        )

        schema = table.schema

        # GET IDS OF DOCUMENTS
        query = {
            "from": command["update"],
            "select": [{"value": "_id"}]
            + [{"name": k, "value": v} for k, v in command.set.items()],
            "where": command.where,
            "format": "list",
            "limit": 10000,
        }

        results = self.query(query)

        if results.data:
            content = "".join(
                t
                for r in results.data
                for _id, row in [(r._id, r)]
                for _ in [row.__setitem__("_id", None)]  # WARNING! DESTRUCTIVE TO row
                for update in map(value2json, ({"update": {"_id": _id}}, {"doc": row}))
                for t in (update, "\n")
            )
            response = self.es.cluster.post(
                es_index.path + "/" + "_bulk",
                data=content,
                timeout=self.settings.timeout,
                params={"wait_for_active_shards": self.settings.wait_for_active_shards},
            )
            if response.errors:
                Log.error(
                    "could not update: {{error}}",
                    error=[
                        e.error
                        for i in response["items"]
                        for e in i.values()
                        if e.status not in (200, 201)
                    ],
                )

        # DELETE BY QUERY, IF NEEDED
        if "." in listwrap(command["clear"]):
            es_filter = (
                ES52Lang[jx_expression(command.where)].partial_eval().to_es(schema)
            )
            self.es.delete_record(es_filter)
            return

        es_index.refresh()
