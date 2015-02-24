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

from pyLibrary import convert
from pyLibrary.env import elasticsearch
from pyLibrary.meta import use_settings
from pyLibrary.queries import MVEL, qb
from pyLibrary.queries.container import Container
from pyLibrary.queries.es_query_aggop import is_aggop, es_aggop
from pyLibrary.queries.es_query_aggs import es_aggsop, is_aggsop
from pyLibrary.queries.es_query_setop import is_fieldop, is_setop, is_deep, es_setop, es_deepop, es_fieldop
from pyLibrary.queries.es_query_terms import es_terms, is_terms
from pyLibrary.queries.es_query_terms_stats import es_terms_stats, is_terms_stats
from pyLibrary.queries.es_query_util import aggregates, loadColumns
from pyLibrary.queries.dimensions import Dimension
from pyLibrary.queries.query import Query, _normalize_where
from pyLibrary.debugs.logs import Log
from pyLibrary.queries.MVEL import _MVEL
from pyLibrary.dot.dicts import Dict
from pyLibrary.dot import nvl, unwrap
from pyLibrary.dot.lists import DictList
from pyLibrary.dot import wrap, listwrap


class ESQuery(Container):
    """
    SEND GENERAL qb QUERIES TO ElasticSearch
    """

    @use_settings
    def __init__(self, host, index, alias=None, name=None, type=None, port=9200, settings=None):
        self.settings = settings
        self.name = nvl(name, alias, index)
        self._es = elasticsearch.Index(settings=settings)
        self.edges = Dict()
        self.worker = None
        self.ready = False

    def as_dict(self):
        settings = self.settings.copy()
        settings.settings = None
        return settings

    def __json__(self):
        return convert.value2json(self.as_dict())


    def __enter__(self):
        self.ready = True
        return self

    def __exit__(self, type, value, traceback):
        self.ready = False
        if not self.worker:
            return

        if isinstance(value, Exception):
            self.worker.stop()
            self.worker.join()
        else:
            self.worker.join()

    @property
    def url(self):
        return self._es.settings.host + "/" + self._es.settings.alias + "/" + self._es.settings.type


    def query(self, _query):
        if not self.ready:
            Log.error("Must use with clause for any instance of ESQuery")

        query = Query(_query, schema=self)

        for s in listwrap(query.select):
            if not aggregates[s.aggregate]:
                Log.error("ES can not aggregate " + self.select[0].name + " because '" + self.select[0].aggregate + "' is not a recognized aggregate")

        frum = query["from"]
        if isinstance(frum, Query):
            result = self.query(frum)
            q2 = query.copy()
            q2.frum = result
            return qb.run(q2)

        try:
            frum = loadColumns(self._es, query["from"])
            mvel = _MVEL(frum)
        except Exception, e:
            mvel = None
            Log.warning("TODO: Fix this", e)

        if is_aggsop(self._es, query):
            return es_aggsop(self._es, mvel, query)
        if is_fieldop(query):
            return es_fieldop(self._es, query)
        elif is_deep(query):
            return es_deepop(self._es, mvel, query)
        elif is_setop(query):
            return es_setop(self._es, mvel, query)
        elif is_aggop(query):
            return es_aggop(self._es, mvel, query)
        elif is_terms(query):
            return es_terms(self._es, mvel, query)
        elif is_terms_stats(query):
            return es_terms_stats(self, mvel, query)

        Log.error("Can not handle")


    def get_column_names(self):
        # GET METADATA FOR INDEX
        # LIST ALL COLUMNS
        frum = loadColumns(self._es, self)
        return frum.columns.name

    def addDimension(self, dim):
        if isinstance(dim, list):
            Log.error("Expecting dimension to be a object, not a list:\n{{dim}}", {"dim": dim})
        self._addDimension(dim, [])

    def _addDimension(self, dim, path):
        dim.full_name = dim.name
        for e in dim.edges:
            d = Dimension(e, dim, self)
            self.edges[d.full_name] = d

    def __getitem__(self, item):
        e = self.edges[item]
        return e

    def __getattr__(self, item):
        return self.edges[item]

    def normalize_edges(self, edges):
        output = DictList()
        for e in listwrap(edges):
            output.extend(self._normalize_edge(e))
        return output

    def _normalize_edge(self, edge):
        """
        RETURN A EDGE DEFINITION INTO A SIMPLE ARRAY OF PATH-LEAF
        DEFINITIONS [ {"name":<pathA>, "value":<pathB>}, ... ]

        USEFUL FOR DECLARING HIGH-LEVEL DIMENSIONS, AND RELIEVING LOW LEVEL PATH PAIRS
        """
        if isinstance(edge, basestring):
            e = self[edge]
            if e:
                domain = e.getDomain()
                fields = domain.dimension.fields

                if isinstance(fields, list):
                    if len(fields) == 1:
                        return [{"value": fields[0]}]
                    else:
                        return [{"name": (edge + "[" + str(i) + "]"), "value": v} for i, v in enumerate(fields)]
                elif isinstance(fields, dict):
                    return [{"name": (edge + "." + k), "value": v} for k, v in fields.items()]
                else:
                    Log.error("do not know how to handle")

            return [{
                        "name": edge,
                        "value": edge
                    }]
        else:
            return [{
                        "name": nvl(edge.name, edge.value),
                        "value": edge.value
                    }]


    def update(self, command):
        """
        EXPECTING command == {"set":term, "where":where}
        THE set CLAUSE IS A DICT MAPPING NAMES TO VALUES
        THE where CLAUSE IS AN ES FILTER
        """
        command = wrap(command)

        # GET IDS OF DOCUMENTS
        results = self._es.search({
            "fields": [],
            "query": {"filtered": {
                "query": {"match_all": {}},
                "filter": _normalize_where(command.where, self)
            }},
            "size": 200000
        })

        # SCRIPT IS SAME FOR ALL (CAN ONLY HANDLE ASSIGNMENT TO CONSTANT)
        scripts = DictList()
        for k, v in command.set.items():
            if not MVEL.isKeyword(k):
                Log.error("Only support simple paths for now")

            scripts.append("ctx._source." + k + " = " + MVEL.value2MVEL(v) + ";\n")
        script = "".join(scripts)

        if results.hits.hits:
            command = []
            for id in results.hits.hits._id:
                command.append({"update": {"_id": id}})
                command.append({"script": script})
            content = ("\n".join(convert.value2json(c) for c in command) + "\n").encode('utf-8')
            self._es.cluster._post(
                self._es.path + "/_bulk",
                data=content,
                headers={"Content-Type": "application/json"}
            )
