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

from jx_base import container
from jx_base.container import Container
from jx_base.dimensions import Dimension
from jx_base.expressions import jx_expression
from jx_base.query import QueryOp
from jx_elasticsearch.es52.aggs import es_aggsop, is_aggsop
from jx_elasticsearch.es52.deep import is_deepop, es_deepop
from jx_elasticsearch.es52.setop import is_setop, es_setop
from jx_elasticsearch.es52.util import aggregates
from jx_elasticsearch.meta import ElasticsearchMetadata, Table
from jx_python import jx
from mo_dots import Data, unwrap, coalesce, split_field, join_field, wrap, listwrap
from mo_json import value2json
from mo_json.typed_encoder import EXISTS_TYPE
from mo_kwargs import override
from mo_logs import Log, Except
from pyLibrary.env import elasticsearch, http


class ES52(Container):
    """
    SEND jx QUERIES TO ElasticSearch
    """

    def __new__(cls, *args, **kwargs):
        if (len(args) == 1 and args[0].get("index") == "meta") or kwargs.get("index") == "meta":
            output = ElasticsearchMetadata.__new__(ElasticsearchMetadata, *args, **kwargs)
            output.__init__(*args, **kwargs)
            return output
        else:
            return Container.__new__(cls)

    @override
    def __init__(
        self,
        host,
        index,
        type=None,
        name=None,
        port=9200,
        read_only=True,
        timeout=None,  # NUMBER OF SECONDS TO WAIT FOR RESPONSE, OR SECONDS TO WAIT FOR DOWNLOAD (PASSED TO requests)
        wait_for_active_shards=1,  # ES WRITE CONSISTENCY (https://www.elastic.co/guide/en/elasticsearch/reference/1.7/docs-index_.html#index-consistency)
        typed=None,
        kwargs=None
    ):
        Container.__init__(self)
        if not container.config.default:
            container.config.default = {
                "type": "elasticsearch",
                "settings": unwrap(kwargs)
            }
        self.settings = kwargs
        self.name = name = coalesce(name, index)
        if read_only:
            self.es = elasticsearch.Alias(alias=index, kwargs=kwargs)
        else:
            self.es = elasticsearch.Cluster(kwargs=kwargs).get_index(read_only=read_only, kwargs=kwargs)

        self._namespace = ElasticsearchMetadata(kwargs=kwargs)
        self.settings.type = self.es.settings.type
        self.edges = Data()
        self.worker = None

        columns = self.snowflake.columns  # ABSOLUTE COLUMNS
        is_typed = any(c.es_column == EXISTS_TYPE for c in columns)

        if typed == None:
            # SWITCH ON TYPED MODE
            self.typed = is_typed
        else:
            if is_typed != typed:
                Log.error("Expecting given typed {{typed}} to match {{is_typed}}", typed=typed, is_typed=is_typed)
            self.typed = typed

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

    def __enter__(self):
        Log.error("No longer used")
        return self

    def __exit__(self, type, value, traceback):
        if not self.worker:
            return

        if isinstance(value, Exception):
            self.worker.stop()
            self.worker.join()
        else:
            self.worker.join()

    @property
    def query_path(self):
        return join_field(split_field(self.name)[1:])

    @property
    def url(self):
        return self.es.url

    def query(self, _query):
        try:
            query = QueryOp.wrap(_query, container=self, namespace=self.namespace)

            for s in listwrap(query.select):
                if s.aggregate != None and not aggregates.get(s.aggregate):
                    Log.error(
                        "ES can not aggregate {{name}} because {{aggregate|quote}} is not a recognized aggregate",
                        name=s.name,
                        aggregate=s.aggregate
                    )

            frum = query["from"]
            if isinstance(frum, QueryOp):
                result = self.query(frum)
                q2 = query.copy()
                q2.frum = result
                return jx.run(q2)

            if is_deepop(self.es, query):
                return es_deepop(self.es, query)
            if is_aggsop(self.es, query):
                return es_aggsop(self.es, frum, query)
            if is_setop(self.es, query):
                return es_setop(self.es, query)
            Log.error("Can not handle")
        except Exception as e:
            e = Except.wrap(e)
            if "Data too large, data for" in e:
                http.post(self.es.cluster.url / "_cache/clear")
                Log.error("Problem (Tried to clear Elasticsearch cache)", e)
            Log.error("problem", e)

    def addDimension(self, dim):
        if isinstance(dim, list):
            Log.error("Expecting dimension to be a object, not a list:\n{{dim}}",  dim= dim)
        self._addDimension(dim, [])

    def _addDimension(self, dim, path):
        dim.full_name = dim.name
        for e in dim.edges:
            d = Dimension(e, dim, self)
            self.edges[d.full_name] = d

    def __getitem__(self, item):
        c = self.get_columns(table_name=self.name, column_name=item)
        if c:
            if len(c) > 1:
                Log.error("Do not know how to handle multipole matches")
            return c[0]

        e = self.edges[item]
        if not c:
            Log.warning("Column with name {{column|quote}} can not be found in {{table}}", column=item, table=self.name)
        return e

    def __getattr__(self, item):
        return self.edges[item]

    def update(self, command):
        """
        EXPECTING command == {"set":term, "where":where}
        THE set CLAUSE IS A DICT MAPPING NAMES TO VALUES
        THE where CLAUSE IS AN ES FILTER
        """
        command = wrap(command)
        table = self.get_table(command['update'])

        es_index = self.es.cluster.get_index(read_only=False, alias=None, kwargs=self.es.settings)

        schema = table.schema
        es_filter = jx_expression(command.where).to_esfilter(schema)

        # GET IDS OF DOCUMENTS
        query = {
            "from": command['update'],
            "select": ["_id"] + [
                {"name": k, "value": v}
                for k, v in command.set.items()
            ],
            "where": command.where,
            "format": "list",
            "limit": 10000
        }

        results = self.query(query)

        if results.data:
            content = "".join(
                t
                for r in results.data
                for _id, row in [(r._id, r)]
                for _ in [row.__setitem__('_id', None)]  # WARNING! DESTRUCTIVE TO row
                for update in map(value2json, ({"update": {"_id": _id}}, {"doc": row}))
                for t in (update, "\n")
            )
            response = self.es.cluster.post(
                es_index.path + "/" + "_bulk",
                data=content,
                headers={"Content-Type": "application/json"},
                timeout=self.settings.timeout,
                params={"wait_for_active_shards": self.settings.wait_for_active_shards}
            )
            if response.errors:
                Log.error("could not update: {{error}}", error=[e.error for i in response["items"] for e in i.values() if e.status not in (200, 201)])

        # DELETE BY QUERY, IF NEEDED
        if '.' in listwrap(command.clear):
            self.es.delete_record(es_filter)
            return

        es_index.flush()


