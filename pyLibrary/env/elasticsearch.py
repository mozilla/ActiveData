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

from pyLibrary import convert, strings
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce, Null, Dict, set_default, join_field, split_field, unwraplist, listwrap, literal_field
from pyLibrary.dot.lists import DictList
from pyLibrary.dot import wrap
from pyLibrary.env import http
from pyLibrary.jsons.typed_encoder import json2typed
from pyLibrary.maths.randoms import Random
from pyLibrary.maths import Math
from pyLibrary.meta import use_settings
from pyLibrary.queries import jx
from pyLibrary.strings import utf82unicode
from pyLibrary.thread.threads import ThreadedQueue, Thread, Lock
from pyLibrary.times.durations import MINUTE


ES_NUMERIC_TYPES = ["long", "integer", "double", "float"]
ES_PRIMITIVE_TYPES = ["string", "boolean", "integer", "date", "long", "double"]


class Features(object):
    pass


class Index(Features):
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
        type=None,  # SCHEMA NAME, (DEFAULT TO TYPE IN INDEX, IF ONLY ONE)
        alias=None,
        explore_metadata=True,  # PROBING THE CLUSTER FOR METADATA IS ALLOWED
        read_only=True,
        tjson=False,  # STORED AS TYPED JSON
        timeout=None,  # NUMBER OF SECONDS TO WAIT FOR RESPONSE, OR SECONDS TO WAIT FOR DOWNLOAD (PASSED TO requests)
        debug=False,  # DO NOT SHOW THE DEBUG STATEMENTS
        settings=None
    ):
        if index==None:
            Log.error("not allowed")
        if index == alias:
            Log.error("must have a unique index name")

        self.cluster_state = None
        self.debug = debug
        self.settings = settings
        self.cluster = Cluster(settings)

        try:
            full_index = self.get_index(index)
            if full_index and alias==None:
                settings.alias = settings.index
                settings.index = full_index
            if full_index==None:
                Log.error("not allowed")
            if type == None:
                # NO type PROVIDED, MAYBE THERE IS A SUITABLE DEFAULT?
                with self.cluster.metadata_locker:
                    index_ = self.cluster._metadata.indices[self.settings.index]
                if not index_:
                    indices = self.cluster.get_metadata(index=self.settings.index).indices
                    index_ = indices[self.settings.index]

                candidate_types = list(index_.mappings.keys())
                if len(candidate_types) != 1:
                    Log.error("Expecting `type` parameter")
                self.settings.type = type = candidate_types[0]
        except Exception, e:
            # EXPLORING (get_metadata()) IS NOT ALLOWED ON THE PUBLIC CLUSTER
            Log.error("not expected", cause=e)

        if not type:
            Log.error("not allowed")

        self.path = "/" + full_index + "/" + type

        if self.debug:
            Log.alert("elasticsearch debugging for {{url}} is on", url=self.url)

    @property
    def url(self):
        return self.cluster.path.rstrip("/") + "/" + self.path.lstrip("/")

    def get_schema(self, retry=True):
        if self.settings.explore_metadata:
            indices = self.cluster.get_metadata().indices
            index = indices[self.settings.index]

            if index == None and retry:
                #TRY AGAIN, JUST IN CASE
                self.cluster.cluster_state = None
                return self.get_schema(retry=False)

            if not index.mappings[self.settings.type]:
                Log.error(
                    "ElasticSearch index {{index|quote}} does not have type {{type|quote}} in {{mapping|json}}",
                    index=self.settings.index,
                    type=self.settings.type,
                    mapping=indices
                )
            return index.mappings[self.settings.type]
        else:
            mapping = self.cluster.get(self.path + "/_mapping")
            if not mapping[self.settings.type]:
                Log.error(
                    "ElasticSearch index {{index|quote}} does not have type {{type|quote}} in {{mapping|json}}",
                    index=self.settings.index,
                    type=self.settings.type,
                    mapping=mapping
                )
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
        alias = coalesce(alias, self.settings.alias)
        self.cluster_state = None
        self.cluster.post(
            "/_aliases",
            data={
                "actions": [
                    {"add": {"index": self.settings.index, "alias": alias}}
                ]
            },
            timeout=coalesce(self.settings.timeout, 30)
        )

        # WAIT FOR ALIAS TO APPEAR
        while True:
            if alias in self.cluster.get("/_cluster/state").metadata.indices[self.settings.index].aliases:
                return
            Log.note("Waiting for alias {{alias}} to appear", alias=alias)
            Thread.sleep(seconds=1)

    def get_index(self, alias):
        """
        RETURN THE INDEX USED BY THIS alias
        """
        alias_list = self.cluster.get_aliases()
        output = sort([
            a.index
            for a in alias_list
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
        self.cluster.post("/" + self.settings.index + "/_flush", data={"wait_if_ongoing": True, "forced": True})

    def delete_record(self, filter):
        if self.settings.read_only:
            Log.error("Index opened in read only mode, no changes allowed")
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
                Log.error("Failure to delete from {{index}}", index=name)


    def extend(self, records):
        """
        records - MUST HAVE FORM OF
            [{"value":value}, ... {"value":value}] OR
            [{"json":json}, ... {"json":json}]
            OPTIONAL "id" PROPERTY IS ALSO ACCEPTED
        """
        if self.settings.read_only:
            Log.error("Index opened in read only mode, no changes allowed")
        lines = []
        try:
            for r in records:
                id = r.get("id")

                if id == None:
                    id = random_id()

                if "json" in r:
                    json = r["json"]
                elif "value" in r:
                    json = convert.value2json(r["value"])
                else:
                    json = None
                    Log.error("Expecting every record given to have \"value\" or \"json\" property")

                lines.append('{"index":{"_id": ' + convert.value2json(id) + '}}')
                if self.settings.tjson:
                    lines.append(json2typed(json))
                else:
                    lines.append(json)
            del records

            if not lines:
                return

            try:
                data_bytes = "\n".join(lines) + "\n"
                data_bytes = data_bytes.encode("utf8")
            except Exception, e:
                Log.error("can not make request body from\n{{lines|indent}}", lines=lines, cause=e)


            response = self.cluster.post(
                self.path + "/_bulk",
                data=data_bytes,
                headers={"Content-Type": "text"},
                timeout=self.settings.timeout
            )
            items = response["items"]

            fails = []
            if self.cluster.version.startswith("0.90."):
                for i, item in enumerate(items):
                    if not item.index.ok:
                        fails.append(i)
            elif any(map(self.cluster.version.startswith, ["1.4.", "1.5.", "1.6.", "1.7."])):
                for i, item in enumerate(items):
                    if item.index.status not in [200, 201]:
                        fails.append(i)
            else:
                Log.error("version not supported {{version}}", version=self.cluster.version)
            if fails:
                item = items[fails[0]]
                Log.error(
                    "{{num}} {{error}} while loading line id={{id}} into index {{index|quote}}:\n{{line}}",
                    num=item.index.status,
                    error=item.index.error,
                    line=strings.limit(lines[fails[0] * 2 + 1], 2000),
                    index=self.settings.index,
                    all_fails=fails,
                    id=item.index._id
                )

            if self.debug:
                Log.note("{{num}} documents added", num=len(items))
        except Exception, e:
            if e.message.startswith("sequence item "):
                Log.error("problem with {{data}}", data=repr(lines[int(e.message[14:16].strip())]), cause=e)
            Log.error("problem sending to ES", e)

    # RECORDS MUST HAVE id AND json AS A STRING OR
    # HAVE id AND value AS AN OBJECT
    def add(self, record):
        if self.settings.read_only:
            Log.error("Index opened in read only mode, no changes allowed")
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
        elif any(map(self.cluster.version.startswith, ["1.4.", "1.5.", "1.6.", "1.7."])):
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

    def search(self, query, timeout=None, retry=None):
        query = wrap(query)
        try:
            if self.debug:
                if len(query.facets.keys()) > 20:
                    show_query = query.copy()
                    show_query.facets = {k: "..." for k in query.facets.keys()}
                else:
                    show_query = query
                Log.note("Query:\n{{query|indent}}", query=show_query)
            return self.cluster.post(
                self.path + "/_search",
                data=query,
                timeout=coalesce(timeout, self.settings.timeout),
                retry=retry
            )
        except Exception, e:
            Log.error(
                "Problem with search (path={{path}}):\n{{query|indent}}",
                path=self.path + "/_search",
                query=query,
                cause=e
            )

    def threaded_queue(self, batch_size=None, max_size=None, period=None, silent=False):
        return ThreadedQueue(
            "push to elasticsearch: " + self.settings.index,
            self,
            batch_size=batch_size,
            max_size=max_size,
            period=period,
            silent=silent
        )

    def delete(self):
        self.cluster.delete_index(index=self.settings.index)


known_clusters = {}

class Cluster(object):

    @use_settings
    def __new__(cls, host, port=9200, settings=None):
        if not isinstance(port, int):
            Log.error("port must be integer")
        cluster = known_clusters.get((host, port))
        if cluster:
            return cluster

        cluster = object.__new__(cls)
        known_clusters[(host, port)] = cluster
        return cluster

    @use_settings
    def __init__(self, host, port=9200, explore_metadata=True, settings=None):
        """
        settings.explore_metadata == True - IF PROBING THE CLUSTER FOR METADATA IS ALLOWED
        settings.timeout == NUMBER OF SECONDS TO WAIT FOR RESPONSE, OR SECONDS TO WAIT FOR DOWNLOAD (PASSED TO requests)
        """
        if hasattr(self, "settings"):
            return

        self.settings = settings
        self.cluster_state = None
        self._metadata = None
        self.metadata_locker = Lock()
        self.debug = settings.debug
        self.version = None
        self.path = settings.host + ":" + unicode(settings.port)

        self.get_metadata()

    @use_settings
    def get_or_create_index(
        self,
        index,
        alias=None,
        schema=None,
        limit_replicas=None,
        read_only=False,
        tjson=False,
        settings=None
    ):
        best = self._get_best(settings)
        if not best:
            output = self.create_index(settings=settings, schema=schema, limit_replicas=limit_replicas)
            return output
        elif best.alias != None:
            settings.alias = best.alias
            settings.index = best.index
        elif settings.alias == None:
            settings.alias = settings.index
            settings.index = best.index

        index = settings.index
        meta = self.get_metadata(index=index)
        columns = parse_properties(index, [], meta.indices[index].mappings.values()[0].properties)
        if len(columns)!=0:
            settings.tjson = tjson or any(c.name.endswith("$value") for c in columns)

        return Index(settings)

    def _get_best(self, settings):
        from pyLibrary.queries import jx
        aliases = self.get_aliases()
        indexes = jx.sort([
            a
            for a in aliases
            if (a.alias == settings.index and settings.alias == None) or
            (re.match(re.escape(settings.index) + r'\d{8}_\d{6}', a.index) and settings.alias == None) or
            (a.index == settings.index and (a.alias == None or a.alias == settings.alias))
        ], "index")
        return indexes.last()

    @use_settings
    def get_index(self, index, type=None, alias=None, read_only=True, settings=None):
        """
        TESTS THAT THE INDEX EXISTS BEFORE RETURNING A HANDLE
        """
        if read_only:
            # GET EXACT MATCH, OR ALIAS
            aliases = self.get_aliases()
            if index in aliases.index:
                return Index(settings)
            if index in aliases.alias:
                match = [a for a in aliases if a.alias == index][0]
                settings.alias = match.alias
                settings.index = match.index
                return Index(settings)
            Log.error("Can not find index {{index_name}}", index_name=settings.index)
        else:
            # GET BEST MATCH, INCLUDING PROTOTYPE
            best = self._get_best(settings)
            if not best:
                Log.error("Can not find index {{index_name}}", index_name=settings.index)

            if best.alias != None:
                settings.alias = best.alias
                settings.index = best.index
            elif settings.alias == None:
                settings.alias = settings.index
                settings.index = best.index
            return Index(settings)

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
            return Index(read_only=True, settings=settings)
        Log.error("Can not find any index with alias {{alias_name}}",  alias_name= alias)

    def get_prototype(self, alias):
        """
        RETURN ALL INDEXES THAT ARE INTENDED TO BE GIVEN alias, BUT HAVE NO
        ALIAS YET BECAUSE INCOMPLETE
        """
        output = sort([
            a.index
            for a in self.get_aliases()
            if re.match(re.escape(alias) + "\\d{8}_\\d{6}", a.index) and not a.alias
        ])
        return output

    @use_settings
    def create_index(
        self,
        index,
        alias=None,
        schema=None,
        limit_replicas=None,
        read_only=False,
        tjson=False,
        settings=None
    ):
        if not settings.alias:
            settings.alias = settings.index
            index = settings.index = proto_name(settings.alias)

        if settings.alias == index:
            Log.error("Expecting index name to conform to pattern")

        if settings.schema_file:
            Log.error('schema_file attribute not supported.  Use {"$ref":<filename>} instead')

        if schema == None:
            Log.error("Expecting a schema")
        elif isinstance(schema, basestring):
            schema = convert.json2value(schema, leaves=True)
        else:
            schema = convert.json2value(convert.value2json(schema), leaves=True)

        if limit_replicas:
            # DO NOT ASK FOR TOO MANY REPLICAS
            health = self.get("/_cluster/health")
            if schema.settings.index.number_of_replicas >= health.number_of_nodes:
                Log.warning("Reduced number of replicas: {{from}} requested, {{to}} realized",
                    {"from": schema.settings.index.number_of_replicas},
                    to= health.number_of_nodes - 1
                )
                schema.settings.index.number_of_replicas = health.number_of_nodes - 1

        self.post(
            "/" + index,
            data=schema,
            headers={"Content-Type": "application/json"}
        )

        # CONFIRM INDEX EXISTS
        while True:
            try:
                state = self.get("/_cluster/state")
                if index in state.metadata.indices:
                    break
                Log.note("Waiting for index {{index}} to appear", index=index)
            except Exception, e:
                Log.warning("Problem while waiting for index {{index}} to appear", index=index, cause=e)
            Thread.sleep(seconds=1)
        Log.alert("Made new index {{index|quote}}", index=index)

        es = Index(settings=settings)
        return es

    def delete_index(self, index_name):
        url = self.settings.host + ":" + unicode(self.settings.port) + "/" + index_name
        try:
            response = http.delete(url)
            if response.status_code != 200:
                Log.error("Expecting a 200")
            details = convert.json2value(utf82unicode(response.content))
            if self.debug:
                Log.note("delete response {{response}}", response=details)
            return response
        except Exception, e:
            Log.error("Problem with call to {{url}}", url=url, cause=e)


    def get_aliases(self):
        """
        RETURN LIST OF {"alias":a, "index":i} PAIRS
        ALL INDEXES INCLUDED, EVEN IF NO ALIAS {"alias":Null}
        """
        data = self.get("/_cluster/state")
        output = []
        for index, desc in data.metadata.indices.items():
            if not desc["aliases"]:
                output.append({"index": index, "alias": None})
            else:
                for a in desc["aliases"]:
                    output.append({"index": index, "alias": a})
        return wrap(output)

    def get_metadata(self, index=None, force=False):
        with self.metadata_locker:
            if self.settings.explore_metadata:
                if not self._metadata or (force and index is None):
                    response = self.get("/_cluster/state")
                    self._metadata = wrap(response.metadata)
                    self.cluster_state = wrap(self.get("/"))
                    self.version = self.cluster_state.version.number
                elif index:  # UPDATE THE MAPPING FOR ONE INDEX ONLY
                    response = self.get("/"+index+"/_mapping")
                    if self.version.startswith("0.90."):
                        best = jx.sort(response.items(), 0).last()
                        self._metadata.indices[index].mappings = best[1]
                    else:
                        self._metadata.indices[index].mappings = jx.sort(response.items(), 0).last()[1].mappings
                    return Dict(indices={index: self._metadata.indices[index]})
            else:
                Log.error("Metadata exploration has been disabled")
            return self._metadata

    def post(self, path, **kwargs):
        url = self.settings.host + ":" + unicode(self.settings.port) + path

        try:
            wrap(kwargs).headers["Accept-Encoding"] = "gzip,deflate"

            data = kwargs.get(b'data')
            if data == None:
                pass
            elif isinstance(data, Mapping):
                kwargs[b'data'] = data =convert.unicode2utf8(convert.value2json(data))
            elif not isinstance(kwargs["data"], str):
                Log.error("data must be utf8 encoded string")

            if self.debug:
                sample = kwargs.get(b'data', "")[:300]
                Log.note("{{url}}:\n{{data|indent}}", url=url, data=sample)

            response = http.post(url, **kwargs)
            if response.status_code not in [200, 201]:
                Log.error(response.reason.decode("latin1") + ": " + strings.limit(response.content.decode("latin1"), 100 if self.debug else 10000))
            if self.debug:
                Log.note("response: {{response}}", response=utf82unicode(response.content)[:130])
            details = convert.json2value(utf82unicode(response.content))
            if details.error:
                Log.error(convert.quote2string(details.error))
            if details._shards.failed > 0:
                Log.error("Shard failures {{failures|indent}}",
                    failures="---\n".join(r.replace(";", ";\n") for r in details._shards.failures.reason)
                )
            return details
        except Exception, e:
            if url[0:4] != "http":
                suggestion = " (did you forget \"http://\" prefix on the host name?)"
            else:
                suggestion = ""

            if kwargs.get("data"):
                Log.error(
                    "Problem with call to {{url}}" + suggestion + "\n{{body|left(10000)}}",
                    url=url,
                    body=strings.limit(kwargs["data"], 100 if self.debug else 10000),
                    cause=e
                )
            else:
                Log.error("Problem with call to {{url}}" + suggestion, url=url, cause=e)



    def get(self, path, **kwargs):
        url = self.settings.host + ":" + unicode(self.settings.port) + path
        try:
            response = http.get(url, **kwargs)
            if response.status_code not in [200]:
                Log.error(response.reason+": "+response.all_content)
            if self.debug:
                Log.note("response: {{response}}", response=utf82unicode(response.all_content)[:130])
            details = wrap(convert.json2value(utf82unicode(response.all_content)))
            if details.error:
                Log.error(details.error)
            return details
        except Exception, e:
            Log.error("Problem with call to {{url}}", url=url, cause=e)

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
            if len(output) == 1:
                return output[0]
            try:
                return sort(output)
            except Exception:
                return output
        else:
            return r
    except Exception, e:
        Log.warning("Can not scrub: {{json}}",  json= r)



class Alias(Features):
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
                alias_list = self.cluster.get("/_alias/"+self.settings.index)
                candidates = [(name, i) for name, i in alias_list.items() if self.settings.index in i.aliases.keys()]
                full_name = jx.sort(candidates, 0).last()[0]
                index = self.cluster.get("/" + full_name + "/_mapping")[full_name]
            else:
                index = self.cluster.get("/"+self.settings.index+"/_mapping")[self.settings.index]

            # FIND MAPPING WITH MOST PROPERTIES (AND ASSUME THAT IS THE CANONICAL TYPE)
            max_prop = -1
            for _type, mapping in index.mappings.items():
                if _type == "_default_":
                    continue
                num_prop = len(mapping.properties.keys())
                if max_prop < num_prop:
                    max_prop = num_prop
                    self.settings.type = _type
                    type = _type

            if type == None:
                Log.error("Can not find schema type for index {{index}}", index=coalesce(self.settings.alias, self.settings.index))

        self.path = "/" + alias + "/" + type

    @property
    def url(self):
        return self.cluster.path.rstrip("/") + "/" + self.path.lstrip("/")

    def get_schema(self, retry=True):
        if self.settings.explore_metadata:
            indices = self.cluster.get_metadata().indices
            if not self.settings.alias or self.settings.alias==self.settings.index:
                #PARTIALLY DEFINED settings
                candidates = [(name, i) for name, i in indices.items() if self.settings.index in i.aliases]
                # TODO: MERGE THE mappings OF ALL candidates, DO NOT JUST PICK THE LAST ONE

                index = "dummy value"
                schema = wrap({"_routing": {}, "properties": {}})
                for _, ind in jx.sort(candidates, {"value": 0, "sort": -1}):
                    mapping = ind.mappings[self.settings.type]
                    set_default(schema._routing, mapping._routing)
                    schema.properties = _merge_mapping(schema.properties, mapping.properties)
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
                Log.note("Query {{path}}\n{{query|indent}}", path=self.path + "/_search", query=show_query)
            return self.cluster.post(
                self.path + "/_search",
                data=query,
                timeout=coalesce(timeout, self.settings.timeout)
            )
        except Exception, e:
            Log.error(
                "Problem with search (path={{path}}):\n{{query|indent}}",
                path=self.path + "/_search",
                query=query,
                cause=e
            )


def parse_properties(parent_index_name, parent_query_path, esProperties):
    """
    RETURN THE COLUMN DEFINITIONS IN THE GIVEN esProperties OBJECT
    """
    from pyLibrary.queries.meta import Column

    columns = DictList()
    for name, property in esProperties.items():
        if parent_query_path:
            index_name, query_path = parent_index_name, join_field(split_field(parent_query_path) + [name])
        else:
            index_name, query_path = parent_index_name, name

        if property.type == "nested" and property.properties:
            # NESTED TYPE IS A NEW TYPE DEFINITION
            # MARKUP CHILD COLUMNS WITH THE EXTRA DEPTH
            self_columns = parse_properties(index_name, query_path, property.properties)
            for c in self_columns:
                c.nested_path = unwraplist([query_path] + listwrap(c.nested_path))
            columns.extend(self_columns)
            columns.append(Column(
                table=index_name,
                es_index=index_name,
                name=query_path,
                es_column=query_path,
                type="nested",
                nested_path=query_path
            ))

            continue

        if property.properties:
            child_columns = parse_properties(index_name, query_path, property.properties)
            columns.extend(child_columns)
            columns.append(Column(
                table=index_name,
                es_index=index_name,
                name=query_path,
                es_column=query_path,
                type="source" if property.enabled == False else "object"
            ))

        if property.dynamic:
            continue
        if not property.type:
            continue
        if property.type == "multi_field":
            property.type = property.fields[name].type  # PULL DEFAULT TYPE
            for i, (n, p) in enumerate(property.fields.items()):
                if n == name:
                    # DEFAULT
                    columns.append(Column(
                        table=index_name,
                        es_index=index_name,
                        name=query_path,
                        es_column=query_path,
                        type=p.type
                    ))
                else:
                    columns.append(Column(
                        table=index_name,
                        es_index=index_name,
                        name=query_path + "\\." + n,
                        es_column=query_path + "\\." + n,
                        type=p.type
                    ))
            continue

        if property.type in ["string", "boolean", "integer", "date", "long", "double"]:
            columns.append(Column(
                table=index_name,
                es_index=index_name,
                name=query_path,
                es_column=query_path,
                type=property.type
            ))
            if property.index_name and name != property.index_name:
                columns.append(Column(
                    table=index_name,
                    es_index=index_name,
                    es_column=query_path,
                    name=query_path,
                    type=property.type
                ))
        elif property.enabled == None or property.enabled == False:
            columns.append(Column(
                table=index_name,
                es_index=index_name,
                name=query_path,
                es_column=query_path,
                type="source" if property.enabled==False else "object"
            ))
        else:
            Log.warning("unknown type {{type}} for property {{path}}", type=property.type, path=query_path)

    return columns


def random_id():
    return Random.hex(40)

def _merge_mapping(a, b):
    """
    MERGE TWO MAPPINGS, a TAKES PRECEDENCE
    """
    for name, b_details in b.items():
        a_details = a[literal_field(name)]
        if a_details.properties and not a_details.type:
            a_details.type = "object"
        if b_details.properties and not b_details.type:
            b_details.type = "object"

        if a_details:
            a_details.type = _merge_type[a_details.type][b_details.type]

            if b_details.type in ["object", "nested"]:
                _merge_mapping(a_details.properties, b_details.properties)
        else:
            a[literal_field(name)] = deepcopy(b_details)

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

