# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from collections import Mapping
from copy import deepcopy
from datetime import datetime
import re
import time

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.env import http
from pyLibrary.maths.randoms import Random
from pyLibrary.maths import Math
from pyLibrary.meta import use_settings
from pyLibrary.queries import qb
from pyLibrary.strings import utf82unicode
from pyLibrary.dot import coalesce, Null, Dict
from pyLibrary.dot.lists import DictList
from pyLibrary.dot import wrap, unwrap
from pyLibrary.thread.threads import ThreadedQueue, Thread


class Index(object):
    """
    AN ElasticSearch INDEX LIFETIME MANAGEMENT TOOL

    ElasticSearch'S REST INTERFACE WORKS WELL WITH PYTHON AND JAVASCRIPT
    SO HARDLY ANY LIBRARY IS REQUIRED.  IT IS SIMPLER TO MAKE HTTP CALLS
    DIRECTLY TO ES USING YOUR FAVORITE HTTP LIBRARY.  I HAVE SOME
    CONVENIENCE FUNCTIONS HERE, BUT IT'S BETTER TO MAKE YOUR OWN.

    THIS CLASS IS TO HELP DURING ETL, CREATING INDEXES, MANAGING ALIASES
    AND REMOVING INDEXES WHEN THEY HAVE BEEN REPLACED.  IT USES A STANDARD
    SUFFIX (YYYYMMDD-HHMMSS) TO TRACK AGE AND RELATIONSHIP TO THE ALIAS,
    IF ANY YET.

    """
    @use_settings
    def __init__(
        self,
        index,  # NAME OF THE INDEX, EITHER ALIAS NAME OR FULL VERSION NAME
        type,  # SCHEMA NAME
        alias=None,
        explore_metadata=True,  # PROBING THE CLUSTER FOR METADATA IS ALLOWED
        timeout=None,  # NUMBER OF SECONDS TO WAIT FOR RESPONSE, OR SECONDS TO WAIT FOR DOWNLOAD (PASSED TO requests)
        debug=False,  # DO NOT SHOW THE DEBUG STATEMENTS
        settings=None
    ):
        if index==None or type==None:
            Log.error("not allowed")
        if index == alias:
            Log.error("must have a unique index name")

        self.cluster_state = None
        self.cluster_metadata = None
        self.debug = debug
        if self.debug:
            Log.alert("elasticsearch debugging for index {{index}} is on",  index= settings.index)

        self.settings = settings
        self.cluster = Cluster(settings)

        try:
            index = self.get_index(index)
            if index and alias==None:
                settings.alias = settings.index
                settings.index = index
            if index==None:
                Log.error("not allowed")
        except Exception, e:
            # EXPLORING (get_metadata()) IS NOT ALLOWED ON THE PUBLIC CLUSTER
            pass

        self.path = "/" + index + "/" + type

    @property
    def url(self):
        return self.cluster.path + "/" + self.path

    def get_schema(self, retry=True):
        if self.settings.explore_metadata:
            indices = self.cluster.get_metadata().indices
            index = indices[self.settings.index]

            if index == None and retry:
                #TRY AGAIN, JUST IN CASE
                self.cluster.cluster_state = None
                return self.get_schema(retry=False)

            if not index.mappings[self.settings.type]:
                Log.error("ElasticSearch index ({{index}}) does not have type ({{type}})", self.settings)
            return index.mappings[self.settings.type]
        else:
            mapping = self.cluster.get(self.path + "/_mapping")
            if not mapping[self.settings.type]:
                Log.error("{{index}} does not have type {{type}}", self.settings)
            return wrap({"mappings": mapping[self.settings.type]})

    def delete_all_but_self(self):
        """
        DELETE ALL INDEXES WITH GIVEN PREFIX, EXCEPT name
        """
        prefix = self.settings.alias
        name = self.settings.index

        if prefix == name:
            Log.note("{{index_name}} will not be deleted",  index_name= prefix)
        for a in self.cluster.get_aliases():
            # MATCH <prefix>YYMMDD_HHMMSS FORMAT
            if re.match(re.escape(prefix) + "\\d{8}_\\d{6}", a.index) and a.index != name:
                self.cluster.delete_index(a.index)

    def add_alias(self, alias=None):
        if alias:
            self.cluster_state = None
            self.cluster._post(
                "/_aliases",
                data=convert.unicode2utf8(convert.value2json({
                    "actions": [
                        {"add": {"index": self.settings.index, "alias": alias}}
                    ]
                })),
                timeout=coalesce(self.settings.timeout, 30)
            )
        else:
            # SET ALIAS ACCORDING TO LIFECYCLE RULES
            self.cluster_state = None
            self.cluster._post(
                "/_aliases",
                data=convert.unicode2utf8(convert.value2json({
                    "actions": [
                        {"add": {"index": self.settings.index, "alias": self.settings.alias}}
                    ]
                })),
                timeout=coalesce(self.settings.timeout, 30)
            )

    def get_proto(self, alias):
        """
        RETURN ALL INDEXES THAT ARE INTENDED TO BE GIVEN alias, BUT HAVE NO
        ALIAS YET BECAUSE INCOMPLETE
        """
        output = sort([
            a.index
            for a in self.cluster.get_aliases()
            if re.match(re.escape(alias) + "\\d{8}_\\d{6}", a.index) and not a.alias
        ])
        return output

    def get_index(self, alias):
        """
        RETURN THE INDEX USED BY THIS alias
        """
        output = sort([
            a.index
            for a in self.cluster.get_aliases()
            if a.alias == alias or
                a.index == alias or
               (re.match(re.escape(alias) + "\\d{8}_\\d{6}", a.index) and a.index != alias)
        ])

        if len(output) > 1:
            Log.error("only one index with given alias==\"{{alias}}\" expected",  alias= alias)

        if not output:
            return Null

        return output.last()

    def is_proto(self, index):
        """
        RETURN True IF THIS INDEX HAS NOT BEEN ASSIGNED ITS ALIAS
        """
        for a in self.cluster.get_aliases():
            if a.index == index and a.alias:
                return False
        return True

    def flush(self):
        self.cluster._post("/" + self.settings.index + "/_refresh")

    def delete_record(self, filter):
        self.cluster.get_metadata()

        if self.cluster.cluster_state.version.number.startswith("0.90"):
            query = {"filtered": {
                "query": {"match_all": {}},
                "filter": filter
            }}
        elif self.cluster.cluster_state.version.number.startswith("1.0"):
            query = {"query": {"filtered": {
                "query": {"match_all": {}},
                "filter": filter
            }}}
        else:
            raise NotImplementedError

        if self.debug:
            Log.note("Delete bugs:\n{{query}}",  query= query)

        result = self.cluster.delete(
            self.path + "/_query",
            data=convert.value2json(query),
            timeout=60
        )

        for name, status in result._indices.items():
            if status._shards.failed > 0:
                Log.error("Failure to delete from {{index}}",  index= name)


    def extend(self, records):
        """
        records - MUST HAVE FORM OF
            [{"value":value}, ... {"value":value}] OR
            [{"json":json}, ... {"json":json}]
            OPTIONAL "id" PROPERTY IS ALSO ACCEPTED
        """
        lines = []
        try:
            for r in records:
                id = r.get("id")

                if id == None:
                    id = Random.hex(40)

                if "json" in r:
                    # if id != coalesce(wrap(convert.json2value(r["json"])).value._id, id):
                    #     Log.error("expecting _id to match")
                    json = r["json"]
                elif "value" in r:
                    # if id != coalesce(wrap(r).value._id, id):
                    #     Log.error("expecting _id to match")
                    json = convert.value2json(r["value"])
                else:
                    json = None
                    Log.error("Expecting every record given to have \"value\" or \"json\" property")

                lines.append('{"index":{"_id": ' + convert.value2json(id) + '}}')
                lines.append(json)
            del records

            if not lines:
                return

            try:
                data_bytes = "\n".join(lines) + "\n"
                data_bytes = data_bytes.encode("utf8")
            except Exception, e:
                Log.error("can not make request body from\n{{lines|indent}}", lines=lines, cause=e)


            response = self.cluster._post(
                self.path + "/_bulk",
                data=data_bytes,
                headers={"Content-Type": "text"},
                timeout=self.settings.timeout
            )
            items = response["items"]

            for i, item in enumerate(items):
                if self.cluster.version.startswith("0.90."):
                    if not item.index.ok:
                        Log.error(
                            "{{error}} while loading line:\n{{line}}",
                            error=item.index.error,
                            line=lines[i * 2 + 1]
                        )
                elif any(map(self.cluster.version.startswith, ["1.4.", "1.5."])):
                    if item.index.status not in [200, 201]:
                        Log.error(
                            "{{num}} {{error}} while loading line into {{index}}:\n{{line}}",
                            num=item.index.status,
                            error=item.index.error,
                            line=lines[i * 2 + 1],
                            index=self.settings.index
                        )
                else:
                    Log.error("version not supported {{version}}",  version=self.cluster.version)

            if self.debug:
                Log.note("{{num}} documents added", num=len(items))
        except Exception, e:
            if e.message.startswith("sequence item "):
                Log.error("problem with {{data}}", data=repr(lines[int(e.message[14:16].strip())]), cause=e)
            Log.error("problem sending to ES", e)


    # RECORDS MUST HAVE id AND json AS A STRING OR
    # HAVE id AND value AS AN OBJECT
    def add(self, record):
        if isinstance(record, list):
            Log.error("add() has changed to only accept one record, no lists")
        self.extend([record])

    # -1 FOR NO REFRESH
    def set_refresh_interval(self, seconds):
        if seconds <= 0:
            interval = -1
        else:
            interval = unicode(seconds) + "s"

        if self.cluster.version.startswith("0.90."):
            response = self.cluster.put(
                "/" + self.settings.index + "/_settings",
                data='{"index":{"refresh_interval":' + convert.value2json(interval) + '}}'
            )

            result = convert.json2value(utf82unicode(response.all_content))
            if not result.ok:
                Log.error("Can not set refresh interval ({{error}})", {
                    "error": utf82unicode(response.all_content)
                })
        elif any(map(self.cluster.version.startswith, ["1.4.", "1.5."])):
            response = self.cluster.put(
                "/" + self.settings.index + "/_settings",
                data=convert.unicode2utf8('{"index":{"refresh_interval":' + convert.value2json(interval) + '}}')
            )

            result = convert.json2value(utf82unicode(response.all_content))
            if not result.acknowledged:
                Log.error("Can not set refresh interval ({{error}})", {
                    "error": utf82unicode(response.all_content)
                })
        else:
            Log.error("Do not know how to handle ES version {{version}}",  version=self.cluster.version)

    def search(self, query, timeout=None):
        query = wrap(query)
        try:
            if self.debug:
                if len(query.facets.keys()) > 20:
                    show_query = query.copy()
                    show_query.facets = {k: "..." for k in query.facets.keys()}
                else:
                    show_query = query
                Log.note("Query:\n{{query|indent}}", query=show_query)
            return self.cluster._post(
                self.path + "/_search",
                data=convert.value2json(query).encode("utf8"),
                timeout=coalesce(timeout, self.settings.timeout)
            )
        except Exception, e:
            Log.error(
                "Problem with search (path={{path}}):\n{{query|indent}}",
                path=self.path + "/_search",
                query=query,
                cause=e
            )

    def threaded_queue(self, batch_size=None, max_size=None, period=None, silent=False):
        return ThreadedQueue("push to elasticsearch: " + self.settings.index, self, batch_size=batch_size, max_size=max_size, period=period, silent=silent)

    def delete(self):
        self.cluster.delete_index(index=self.settings.index)


class Cluster(object):
    @use_settings
    def __init__(self, host, port=9200, settings=None):
        """
        settings.explore_metadata == True - IF PROBING THE CLUSTER FOR METADATA IS ALLOWED
        settings.timeout == NUMBER OF SECONDS TO WAIT FOR RESPONSE, OR SECONDS TO WAIT FOR DOWNLOAD (PASSED TO requests)
        """

        settings.setdefault("explore_metadata", True)

        self.cluster_state = None
        self.cluster_metadata = None

        self.debug = settings.debug
        self.settings = settings
        self.version = None
        self.path = settings.host + ":" + unicode(settings.port)

    @use_settings
    def get_or_create_index(
        self,
        index,
        alias=None,
        schema=None,
        limit_replicas=None,
        settings=None
    ):
        from pyLibrary.queries import qb

        settings = deepcopy(settings)
        aliases = self.get_aliases()

        indexes = qb.sort([
            a
            for a in aliases
            if (a.alias == settings.index and settings.alias == None) or
               (re.match(re.escape(settings.index) + "\\d{8}_\\d{6}", a.index) and settings.alias == None) or
            (a.index == settings.index and (a.alias == None or a.alias == settings.alias ))
        ], "index")
        if not indexes:
            output = self.create_index(settings=settings, schema=schema, limit_replicas=limit_replicas)
            return output
        elif indexes.last().alias != None:
            settings.alias = indexes.last().alias
            settings.index = indexes.last().index
        elif settings.alias == None:
            settings.alias = settings.index
            settings.index = indexes.last().index
        return Index(settings)


    def get_index(self, index, alias=None, settings=None):
        """
        TESTS THAT THE INDEX EXISTS BEFORE RETURNING A HANDLE
        """
        aliases = self.get_aliases()
        if settings.index in aliases.index:
            return Index(settings)
        if settings.index in aliases.alias:
            match = [a for a in aliases if a.alias == settings.index][0]
            settings.alias = match.alias
            settings.index = match.index
            return Index(settings)
        Log.error("Can not find index {{index_name}}",  index_name= settings.index)

    def get_alias(self, alias):
        """
        RETURN REFERENCE TO ALIAS (MANY INDEXES)
        USER MUST BE SURE NOT TO SEND UPDATES
        """
        aliases = self.get_aliases()
        if alias in aliases.alias:
            settings = self.settings.copy()
            settings.alias = alias
            settings.index = alias
            return Index(settings)
        Log.error("Can not find any index with alias {{alias_name}}",  alias_name= alias)

    @use_settings
    def create_index(
        self,
        index,
        alias=None,
        schema=None,
        limit_replicas=None,
        settings=None
    ):
        if not settings.alias:
            settings.alias = settings.index
            settings.index = proto_name(settings.alias)

        if settings.alias == settings.index:
            Log.error("Expecting index name to conform to pattern")

        if settings.schema_file:
            Log.error('schema_file attribute not supported.  Use {"$ref":<filename>} instead')

        if schema == None:
            Log.error("Expecting a schema")
        elif isinstance(schema, basestring):
            schema = convert.json2value(schema, paths=True)
        else:
            schema = convert.json2value(convert.value2json(schema), paths=True)

        if limit_replicas:
            # DO NOT ASK FOR TOO MANY REPLICAS
            health = self.get("/_cluster/health")
            if schema.settings.index.number_of_replicas >= health.number_of_nodes:
                Log.warning("Reduced number of replicas: {{from}} requested, {{to}} realized",
                    {"from": schema.settings.index.number_of_replicas},
                    to= health.number_of_nodes - 1
                )
                schema.settings.index.number_of_replicas = health.number_of_nodes - 1

        self._post(
            "/" + settings.index,
            data=convert.value2json(schema).encode("utf8"),
            headers={"Content-Type": "application/json"}
        )
        while True:
            time.sleep(1)
            try:
                self.head("/" + settings.index)
                break
            except Exception, _:
                Log.note("{{index}} does not exist yet",  index= settings.index)


        es = Index(settings)
        return es

    def delete_index(self, index=None):
        self.delete("/" + index)

    def get_aliases(self):
        """
        RETURN LIST OF {"alias":a, "index":i} PAIRS
        ALL INDEXES INCLUDED, EVEN IF NO ALIAS {"alias":Null}
        """
        data = self.get_metadata().indices
        output = []
        for index, desc in data.items():
            if not desc["aliases"]:
                output.append({"index": index, "alias": None})
            else:
                for a in desc["aliases"]:
                    output.append({"index": index, "alias": a})
        return wrap(output)

    def get_metadata(self):
        if self.settings.explore_metadata:
            if not self.cluster_metadata:
                response = self.get("/_cluster/state")
                self.cluster_metadata = wrap(response.metadata)
                self.cluster_state = wrap(self.get("/"))
                self.version = self.cluster_state.version.number
        else:
            Log.error("Metadata exploration has been disabled")
        return self.cluster_metadata


    def _post(self, path, **kwargs):
        url = self.settings.host + ":" + unicode(self.settings.port) + path

        try:
            wrap(kwargs).headers["Accept-Encoding"] = "gzip,deflate"

            if "data" in kwargs and not isinstance(kwargs["data"], str):
                Log.error("data must be utf8 encoded string")

            if self.debug:
                sample = kwargs.get("data", "")[:300]
                Log.note("{{url}}:\n{{data|indent}}",  url= url,  data= sample)

            response = http.post(url, **kwargs)
            if response.status_code not in [200, 201]:
                Log.error(response.reason + ": " + response.all_content)
            if self.debug:
                Log.note("response: {{response}}", response=utf82unicode(response.all_content)[:130])
            details = convert.json2value(utf82unicode(response.all_content))
            if details.error:
                Log.error(convert.quote2string(details.error))
            if details._shards.failed > 0:
                Log.error("Shard failures {{failures|indent}}",
                    failures= "---\n".join(r.replace(";", ";\n") for r in details._shards.failures.reason)
                )
            return details
        except Exception, e:
            if url[0:4] != "http":
                suggestion = " (did you forget \"http://\" prefix on the host name?)"
            else:
                suggestion = ""

            if kwargs.get("data"):
                Log.error("Problem with call to {{url}}" + suggestion + "\n{{body|left(10000)}}",
                    url= url,
                    body= kwargs["data"][0:10000] if self.debug else kwargs["data"][0:100], cause=e)
            else:
                Log.error("Problem with call to {{url}}" + suggestion, {"url": url}, e)



    def get(self, path, **kwargs):
        url = self.settings.host + ":" + unicode(self.settings.port) + path
        try:
            response = http.get(url, **kwargs)
            if response.status_code not in [200]:
                Log.error(response.reason+": "+response.all_content)
            if self.debug:
                Log.note("response: {{response}}",  response= utf82unicode(response.all_content)[:130])
            details = wrap(convert.json2value(utf82unicode(response.all_content)))
            if details.error:
                Log.error(details.error)
            return details
        except Exception, e:
            Log.error("Problem with call to {{url}}",  url= url, cause=e)

    def head(self, path, **kwargs):
        url = self.settings.host + ":" + unicode(self.settings.port) + path
        try:
            response = http.head(url, **kwargs)
            if response.status_code not in [200]:
                Log.error(response.reason+": "+response.all_content)
            if self.debug:
                Log.note("response: {{response}}",  response= utf82unicode(response.all_content)[:130])
            if response.all_content:
                details = wrap(convert.json2value(utf82unicode(response.all_content)))
                if details.error:
                    Log.error(details.error)
                return details
            else:
                return None  # WE DO NOT EXPECT content WITH HEAD REQUEST
        except Exception, e:
            Log.error("Problem with call to {{url}}",  url= url, cause=e)

    def put(self, path, **kwargs):
        url = self.settings.host + ":" + unicode(self.settings.port) + path

        if self.debug:
            sample = kwargs["data"][:300]
            Log.note("PUT {{url}}:\n{{data|indent}}",  url= url,  data= sample)
        try:
            response = http.put(url, **kwargs)
            if response.status_code not in [200]:
                Log.error(response.reason+": "+response.all_content)
            if self.debug:
                Log.note("response: {{response}}",  response= utf82unicode(response.all_content)[0:300:])
            return response
        except Exception, e:
            Log.error("Problem with call to {{url}}",  url= url, cause=e)

    def delete(self, path, **kwargs):
        url = self.settings.host + ":" + unicode(self.settings.port) + path
        try:
            response = convert.json2value(utf82unicode(http.delete(url, **kwargs).content))
            if self.debug:
                Log.note("delete response {{response}}",  response= response)
            return response
        except Exception, e:
            Log.error("Problem with call to {{url}}",  url= url, cause=e)


def proto_name(prefix, timestamp=None):
    if not timestamp:
        timestamp = datetime.utcnow()
    return prefix + convert.datetime2string(timestamp, "%Y%m%d_%H%M%S")


def sort(values):
    return wrap(sorted(values))


def scrub(r):
    """
    REMOVE KEYS OF DEGENERATE VALUES (EMPTY STRINGS, EMPTY LISTS, AND NULLS)
    CONVERT STRINGS OF NUMBERS TO NUMBERS
    RETURNS **COPY**, DOES NOT CHANGE ORIGINAL
    """
    return wrap(_scrub(r))


def _scrub(r):
    try:
        if r == None:
            return None
        elif isinstance(r, basestring):
            if r == "":
                return None
            return r
        elif Math.is_number(r):
            return convert.value2number(r)
        elif isinstance(r, Mapping):
            if isinstance(r, Dict):
                r = object.__getattribute__(r, "_dict")
            output = {}
            for k, v in r.items():
                v = _scrub(v)
                if v != None:
                    output[k.lower()] = v
            if len(output) == 0:
                return None
            return output
        elif hasattr(r, '__iter__'):
            if isinstance(r, DictList):
                r = r.list
            output = []
            for v in r:
                v = _scrub(v)
                if v != None:
                    output.append(v)
            if not output:
                return None
            try:
                return sort(output)
            except Exception:
                return output
        else:
            return r
    except Exception, e:
        Log.warning("Can not scrub: {{json}}",  json= r)



class Alias(object):
    @use_settings
    def __init__(
        self,
        alias,  # NAME OF THE ALIAS
        type=None,  # SCHEMA NAME, WILL HUNT FOR ONE IF None
        explore_metadata=True,  # IF PROBING THE CLUSTER FOR METADATA IS ALLOWED
        debug=False,
        timeout=None,  # NUMBER OF SECONDS TO WAIT FOR RESPONSE, OR SECONDS TO WAIT FOR DOWNLOAD (PASSED TO requests)
        settings=None
    ):
        self.debug = debug
        if self.debug:
            Log.alert("Elasticsearch debugging on {{index|quote}} is on",  index= settings.index)

        self.settings = settings
        self.cluster = Cluster(settings)

        if type == None:
            if not explore_metadata:
                Log.error("Alias() was given no `type` (aka schema) and not allowed to explore metadata.  Do not know what to do now.")

            indices = self.cluster.get_metadata().indices
            if not self.settings.alias or self.settings.alias==self.settings.index:
                candidates = [(name, i) for name, i in indices.items() if self.settings.index in i.aliases]
                index = qb.sort(candidates, 0).last()[1]
            else:
                index = indices[self.settings.index]

            # FIND MAPPING WITH MOST PROPERTIES (AND ASSUME THAT IS THE CANONICAL TYPE)
            max_prop = -1
            for _type, mapping in index.mappings.items():
                num_prop = len(mapping.properties.keys())
                if max_prop < num_prop:
                    max_prop = num_prop
                    self.settings.type = _type
                    type = _type

            if type == None:
                Log.error("Can not find schema type for index {{index}}",  index= coalesce(self.settings.alias, self.settings.index))

        self.path = "/" + alias + "/" + type

    @property
    def url(self):
        return self.cluster.path + "/" + self.path

    def get_schema(self, retry=True):
        if self.settings.explore_metadata:
            indices = self.cluster.get_metadata().indices
            if not self.settings.alias or self.settings.alias==self.settings.index:
                #PARTIALLY DEFINED settings
                candidates = [(name, i) for name, i in indices.items() if self.settings.index in i.aliases]
                # TODO: MERGE THE mappings OF ALL candidates, DO NOT JUST PICK THE LAST ONE
                index = "dummy value"
                schema = wrap({"properties": {}})
                for _, ind in qb.sort(candidates, {"value": 0, "sort": -1}):
                    schema.properties = _merge_mapping(schema.properties, ind.mappings[self.settings.type].properties)
            else:
                #FULLY DEFINED settings
                index = indices[self.settings.index]
                schema = index.mappings[self.settings.type]

            if index == None and retry:
                #TRY AGAIN, JUST IN CASE
                self.cluster.cluster_state = None
                return self.get_schema(retry=False)

            #TODO: REMOVE THIS BUG CORRECTION
            if not schema and self.settings.type == "test_result":
                schema = index.mappings["test_results"]
            # DONE BUG CORRECTION

            if not schema:
                Log.error(
                    "ElasticSearch index ({{index}}) does not have type ({{type}})",
                    index=self.settings.index,
                    type=self.settings.type
                )
            return schema
        else:
            mapping = self.cluster.get(self.path + "/_mapping")
            if not mapping[self.settings.type]:
                Log.error("{{index}} does not have type {{type}}", self.settings)
            return wrap({"mappings": mapping[self.settings.type]})

    def delete(self, filter):
        self.cluster.get_metadata()

        if self.cluster.cluster_state.version.number.startswith("0.90"):
            query = {"filtered": {
                "query": {"match_all": {}},
                "filter": filter
            }}
        elif self.cluster.cluster_state.version.number.startswith("1."):
            query = {"query": {"filtered": {
                "query": {"match_all": {}},
                "filter": filter
            }}}
        else:
            raise NotImplementedError

        if self.debug:
            Log.note("Delete bugs:\n{{query}}",  query= query)

        keep_trying = True
        while keep_trying:
            result = self.cluster.delete(
                self.path + "/_query",
                data=convert.value2json(query),
                timeout=60
            )
            keep_trying = False
            for name, status in result._indices.items():
                if status._shards.failed > 0:
                    if status._shards.failures[0].reason.find("rejected execution (queue capacity ") >= 0:
                        keep_trying = True
                        Thread.sleep(seconds=5)
                        break

            if not keep_trying:
                for name, status in result._indices.items():
                    if status._shards.failed > 0:
                        Log.error(
                            "ES shard(s) report Failure to delete from {{index}}: {{message}}.  Query was {{query}}",
                            index=name,
                            query=query,
                            message=status._shards.failures[0].reason
                        )


    def search(self, query, timeout=None):
        query = wrap(query)
        try:
            if self.debug:
                if len(query.facets.keys()) > 20:
                    show_query = query.copy()
                    show_query.facets = {k: "..." for k in query.facets.keys()}
                else:
                    show_query = query
                Log.note("Query:\n{{query|indent}}",  query= show_query)
            return self.cluster._post(
                self.path + "/_search",
                data=convert.value2json(query).encode("utf8"),
                timeout=coalesce(timeout, self.settings.timeout)
            )
        except Exception, e:
            Log.error(
                "Problem with search (path={{path}}):\n{{query|indent}}",
                path=self.path + "/_search",
                query=query,
                cause=e
            )


def _merge_mapping(a, b):
    """
    MERGE TWO MAPPINGS, a TAKES PRECEDENCE
    """
    for name, b_details in b.items():
        a_details = a[name]
        if a_details.properties and not a_details.type:
            a_details.type = "object"
        if b_details.properties and not b_details.type:
            b_details.type = "object"

        if a_details:
            a_details.type = _merge_type[a_details.type][b_details.type]

            if b_details.type in ["object", "nested"]:
                _merge_mapping(a_details.properties, b_details.properties)
        else:
            a[name] = deepcopy(b_details)

    return a

_merge_type = {
    "boolean": {
        "boolean": "boolean",
        "integer": "integer",
        "long": "long",
        "float": "float",
        "double": "double",
        "string": "string",
        "object": None,
        "nested": None
    },
    "integer": {
        "boolean": "integer",
        "integer": "integer",
        "long": "long",
        "float": "float",
        "double": "double",
        "string": "string",
        "object": None,
        "nested": None
    },
    "long": {
        "boolean": "long",
        "integer": "long",
        "long": "long",
        "float": "double",
        "double": "double",
        "string": "string",
        "object": None,
        "nested": None
    },
    "float": {
        "boolean": "float",
        "integer": "float",
        "long": "double",
        "float": "float",
        "double": "double",
        "string": "string",
        "object": None,
        "nested": None
    },
    "double": {
        "boolean": "double",
        "integer": "double",
        "long": "double",
        "float": "double",
        "double": "double",
        "string": "string",
        "object": None,
        "nested": None
    },
    "string": {
        "boolean": "string",
        "integer": "string",
        "long": "string",
        "float": "string",
        "double": "string",
        "string": "string",
        "object": None,
        "nested": None
    },
    "object": {
        "boolean": None,
        "integer": None,
        "long": None,
        "float": None,
        "double": None,
        "string": None,
        "object": "object",
        "nested": "nested"
    },
    "nested": {
        "boolean": None,
        "integer": None,
        "long": None,
        "float": None,
        "double": None,
        "string": None,
        "object": "nested",
        "nested": "nested"
    }
}



