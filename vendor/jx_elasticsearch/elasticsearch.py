# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

import ast
import re
from copy import deepcopy

from jx_base import Column
from jx_python import jx
from mo_dots import Data, FlatList, Null, ROOT_PATH, SLOT, coalesce, concat_field, is_data, is_list, listwrap, \
    literal_field, set_default, split_field, lists, dict_to_data, to_data, list_to_data
from mo_files import File, mimetype
from mo_files.url import URL
from mo_future import binary_type, generator_types, is_binary, is_text, items, text
from mo_http import http
from mo_json import BOOLEAN, EXISTS, NESTED, NUMBER, OBJECT, STRING, json2value, value2json
from mo_json.typed_encoder import BOOLEAN_TYPE, EXISTS_TYPE, NESTED_TYPE, NUMBER_TYPE, STRING_TYPE, TYPE_PREFIX, \
    json_type_to_inserter_type
from mo_kwargs import override
from mo_logs import Log, strings
from mo_logs.exceptions import Except, suppress_exception
from mo_math import is_integer, is_number, randoms
from mo_threads import Lock, ThreadedQueue, Till, THREAD_STOP, Thread, MAIN_THREAD
from mo_times import Date, Timer, HOUR, Duration

DEBUG = True
DEBUG_METADATA_UPDATE = False

ES_STRUCT = ["object", "nested"]
ES_NUMERIC_TYPES = ["long", "integer", "double", "float"]
ES_PRIMITIVE_TYPES = ("boolean", "float", "integer", "date", "long", "double", "string", "keyword")

INDEX_DATE_FORMAT = "%Y%m%d_%H%M%S"
SUFFIX_PATTERN = r'\d{8}_\d{6}'
ID = Data(field='_id')
LF = "\n".encode('utf8')

STALE_METADATA = HOUR
DATA_KEY = text("data")


class IterHandle(object):
    __slots__ = ["iter"]

    def __init__(self, iter):
        self.iter = iter

    def __iter__(self):
        return self.iter()


def iterable(iter):
    """
    RETURN REUSABLE ITERABLE, USING iter AS THE CONSTRUCTOR
    :param iter:
    """
    return IterHandle(iter)


class Alias(object):
    """
    REPRESENT MULTIPLE INDICES, ALL WITH THE SAME INDEX
    """

    @override
    def __init__(
        self,
        alias,  # NAME OF THE ALIAS
        index=None,  # NO LONGER USED
        type=None,  # SCHEMA NAME, WILL HUNT FOR ONE IF None
        explore_metadata=True,  # IF PROBING THE CLUSTER FOR METADATA IS ALLOWED
        debug=False,
        timeout=None,  # NUMBER OF SECONDS TO WAIT FOR RESPONSE, OR SECONDS TO WAIT FOR DOWNLOAD (PASSED TO requests)
        kwargs=None
    ):
        if alias == None:
            Log.error("alias can not be None")
        if index != None:
            Log.error("index is no longer accepted")
        self.debug = debug
        self.settings = kwargs
        self.cluster = Cluster(kwargs)

        if type == None:
            if not explore_metadata:
                Log.error("Alias() was given no `type` (aka schema) and not allowed to explore metadata.  Do not know what to do now.")

            if not self.settings.alias or self.settings.alias == self.settings.index:
                alias_list = self.cluster.get("/_alias")
                candidates = (
                    [(name, i) for name, i in alias_list.items() if self.settings.index in i.aliases.keys()] +
                    [(name, Null) for name, i in alias_list.items() if self.settings.index == name]
                )
                full_name = jx.sort(candidates, 0).last()[0]
                if not full_name:
                    Log.error("No index by name of {{name}}", name=self.settings.index)
                settings = self.cluster.get("/" + full_name + "/_mapping")[full_name]
            else:
                index = self.cluster.get_best_matching_index(alias).index
                settings = self.cluster.get_metadata().indices[literal_field(index)]

            # FIND MAPPING WITH MOST PROPERTIES (AND ASSUME THAT IS THE CANONICAL TYPE)
            type, props = _get_best_type_from_mapping(settings.mappings)
            if type == None:
                Log.error("Can not find schema type for index {{index}}", index=coalesce(self.settings.alias, self.settings.index))

        self.debug and Log.alert("Elasticsearch debugging on {{alias|quote}} is on", alias=alias)
        self.path = "/" + alias + "/" + type

    @property
    def url(self):
        return self.cluster.url / self.path

    def get_snowflake(self, retry=True):
        # TODO: merge this with Index.get_properties()
        if self.settings.explore_metadata:
            indices = self.cluster.get_metadata().indices
            if not self.settings.alias or self.settings.alias == self.settings.index:
                # PARTIALLY DEFINED settings
                candidates = [(name, i) for name, i in indices.items() if self.settings.index in i.aliases]
                # TODO: MERGE THE mappings OF ALL candidates, DO NOT JUST PICK THE LAST ONE

                index = "dummy value"
                schema = dict_to_data({"properties": {}})
                for _, ind in jx.sort(candidates, {"value": 0, "sort": -1}):
                    mapping = ind.mappings[self.settings.type]
                    schema.properties = _merge_mapping(schema.properties, mapping.properties)
            else:
                # FULLY DEFINED settings
                index = indices[self.settings.index]
                schema = index.mappings[self.settings.type]

            if index == None and retry:
                # TRY AGAIN, JUST IN CASE
                self.cluster.info = None
                return self.get_schema(retry=False)

            # TODO: REMOVE THIS BUG CORRECTION
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
            return dict_to_data({"mappings": mapping[self.settings.type]})

    def search(self, query, timeout=None, retry=None, scroll=None):
        query = to_data(query)
        try:
            suffix = "/_search?scroll=" + scroll if scroll else "/_search"
            url = self.path + suffix

            self.debug and Log.note("Query: {{url}}\n{{query|indent}}", url=url, query=query)
            return self.cluster.post(
                url,
                data=query,
                timeout=coalesce(timeout, self.settings.timeout),
                retry=retry
            )
        except Exception as e:
            Log.error(
                "Problem with search (path={{path}}):\n{{query|indent}}",
                path=self.path + "/_search",
                query=query,
                cause=e
            )

    def multisearch(self, queries, timeout=None, retry=None):
        queries = listwrap(queries)
        try:
            url = self.cluster.url / self.path / "_msearch"

            @iterable
            def content():
                for q in queries:
                    yield b"{}\n"
                    yield value2json(q).encode("utf8")
                    yield b"\n"

            self.debug and Log.note("Query: {{url}}\n{{query|indent}}", url=url, query=queries)
            response = http.get(
                url,
                headers={"Content-Type": "application/x-ndjson"},
                data=content,
                timeout=coalesce(timeout, self.settings.timeout),
                retry=retry
            )
            if response.status_code not in [200, 201]:
                Log.error(
                    "Problem with search (url={{url}}):\n{{details}}\n{{query|indent}}",
                    url=url,
                    query=queries,
                    details=response.all_content
                )

            responses = json2value(response.content.decode('utf8')).responses

            for details in responses:
                if details.error:
                    Log.error("{{error|json}}", error=details.error)
                    if details._shards.failed > 0:
                        Log.error(
                            "{{num}} orf {{total}} shard failures {{failures|indent}}",
                            failures=details._shards.failures.reason,
                            num=details._shards.failed,
                            total=details._shards.total
                        )

            return responses
        except Exception as cause:
            Log.error(
                "Problem with search (path={{path}}):\n{{query|indent}}",
                path=self.path + "/_msearch",
                query=queries,
                cause=cause
            )

    def scroll(self, scroll_id):
        try:
            # POST /_search/scroll
            # {
            #     "scroll" : "1m",
            #     "scroll_id" : "DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAD4WYm9laVYtZndUQlNsdDcwakFMNjU1QQ=="
            # }
            return self.cluster.post(
                "_search/scroll",
                data={"scroll": "5m", "scroll_id": scroll_id}
            )
        except Exception as e:
            Log.error(
                "Problem with scroll (scroll_id={{scroll_id}})",
                path= "_search/scroll",
                scroll_id=scroll_id,
                cause=e
            )

    def refresh(self):
        self.cluster.post("/" + self.settings.alias + "/_refresh")


class Index(Alias):
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

    @override
    def __init__(
        self,
        index,  # NAME OF THE INDEX, EITHER ALIAS NAME OR FULL VERSION NAME
        id=ID,  # CUSTOM FIELD FOR _id AND version
        type=None,  # SCHEMA NAME, (DEFAULT TO TYPE IN INDEX, IF ONLY ONE)
        alias=None,
        explore_metadata=True,  # PROBING THE CLUSTER FOR METADATA IS ALLOWED
        read_only=True,
        typed=None,  # STORED AS TYPED JSON
        timeout=None,  # NUMBER OF SECONDS TO WAIT FOR RESPONSE, OR SECONDS TO WAIT FOR DOWNLOAD (PASSED TO requests)
        consistency="one",  # ES WRITE CONSISTENCY (https://www.elastic.co/guide/en/elasticsearch/reference/1.7/docs-index_.html#index-consistency)
        debug=False,  # DO NOT SHOW THE DEBUG STATEMENTS
        cluster=None,
        kwargs=None
    ):
        if kwargs.tjson != None:
            Log.error("used `typed` parameter, not `tjson`")
        if index == None:
            Log.error("not allowed")

        self.info = None
        self.debug = debug
        self.settings = kwargs
        self.cluster = cluster or Cluster(kwargs)

        try:
            full_index = self.cluster.get_canonical_index(index)
            if full_index and alias == None:
                kwargs.alias = kwargs.index
                kwargs.index = full_index
            if full_index == None:
                Log.error("not allowed")
            if type == None:
                # NO type PROVIDED, MAYBE THERE IS A SUITABLE DEFAULT?
                about = self.cluster.get_metadata().indices[literal_field(self.settings.index)]
                type = self.settings.type = _get_best_type_from_mapping(about.mappings)[0]
                if type == "_default_":
                    Log.error("not allowed")
            if not type:
                Log.error("not allowed")

            self.path = "/" + full_index + "/" + type
        except Exception as e:
            # EXPLORING (get_metadata()) IS NOT ALLOWED ON THE PUBLIC CLUSTER
            Log.error("not expected", cause=e)

        self.debug and Log.alert("elasticsearch debugging for {{url}} is on", url=self.url)

        props = self.get_properties()
        if not props:
            typed = coalesce(kwargs.typed, True)  # TYPED JSON IS DEFAULT
        elif props[EXISTS_TYPE]:
            if typed is False:
                Log.error("expecting typed parameter to match properties of {{index}}", index=index)
            elif typed == None:
                typed = kwargs.typed = True
        else:
            if typed is True:
                Log.error("expecting typed parameter to match properties of {{index}}", index=index)
            elif typed == None:
                typed = kwargs.typed = False

        if not read_only:
            if is_text(id):
                id_info = set_default({"field": id})
            elif is_data(id):
                if not id.field:
                    id.field = ID.field
                id_info = id
            else:
                Log.error("do not know how to handle id={{id}}", id=id)

            if typed:
                from jx_elasticsearch.typed_inserter import TypedInserter

                self.encode = TypedInserter(self, id_info).typed_encode
            else:
                self.encode = get_encoder(id_info)

    def get_properties(self, retry=True):
        if self.settings.explore_metadata:
            metadata = self.cluster.get_metadata()
            index = metadata.indices[literal_field(self.settings.index)]

            if index == None and retry:
                # TRY AGAIN, JUST IN CASE
                self.cluster.info = None
                return self.get_properties(retry=False)

            if not index.mappings[self.settings.type] and (index.mappings.keys() - {"_default_"}):
                Log.warning(
                    "ElasticSearch index {{index|quote}} does not have type {{type|quote}} in {{metadata|json}}",
                    index=self.settings.index,
                    type=self.settings.type,
                    metadata=jx.sort(index.mappings.keys())
                )
                return Null
            return index.mappings[self.settings.type].properties
        else:
            mapping = self.cluster.get(self.path + "/_mapping")
            if not mapping[self.settings.type]:
                Log.error(
                    "ElasticSearch index {{index|quote}} does not have type {{type|quote}}",
                    index=self.settings.index,
                    type=self.settings.type
                )
            return dict_to_data({"mappings": mapping[self.settings.type]})

    def delete_all_but_self(self):
        """
        DELETE ALL INDEXES WITH GIVEN PREFIX, EXCEPT name
        """
        prefix = self.settings.alias
        name = self.settings.index

        if prefix == name:
            Log.note("{{index_name}} will not be deleted", index_name=prefix)
        for a in self.cluster.get_aliases():
            # MATCH <prefix>YYMMDD_HHMMSS FORMAT
            if re.match(re.escape(prefix) + "\\d{8}_\\d{6}", a.index) and a.index != name:
                self.cluster.delete_index(a.index)

    def add_alias(self, alias=None):
        alias = coalesce(alias, self.settings.alias)
        self.info = None
        self.cluster.post(
            "/_aliases",
            data={
                "actions": [
                    {"add": {"index": self.settings.index, "alias": alias}}
                ]
            },
            timeout=coalesce(self.settings.timeout, 30),
            stream=False
        )
        self.settings.alias = alias

        # WAIT FOR ALIAS TO APPEAR
        while True:
            metadata = self.cluster.get_metadata(after=Date.now())
            if alias in metadata.indices[literal_field(self.settings.index)].aliases:
                return
            Log.note("Waiting for alias {{alias}} to appear", alias=alias)
            Till(seconds=1).wait()

    def is_proto(self, index):
        """
        RETURN True IF THIS INDEX HAS NOT BEEN ASSIGNED ITS ALIAS
        """
        for a in self.cluster.get_aliases():
            if a.index == index and a.alias:
                return False
        return True

    def flush(self, forced=False):
        """
        TELL ES TO FLUSH TO DISK (DO NOT USE)
        """
        Log.warning("depreciated: Do not use")
        try:
            self.cluster.post("/" + self.settings.index + "/_flush", data={"wait_if_ongoing": True, "forced": forced})
        except Exception as e:
            if "FlushNotAllowedEngineException" in e:
                Log.note("Flush is ignored")
            else:
                Log.error("Problem flushing", cause=e)

    @property
    def created(self):
        """
        :return:
        """
        return Date(self.settings.index[-15:])

    def refresh(self):
        """
        TELL ES TO INDEX THE GIVEN DATA, FLUSH MAY NOT HAPPEN
        """
        self.cluster.post("/" + self.settings.index + "/_refresh")

    def delete_record(self, filter):
        filter = to_data(filter)

        if self.settings.read_only:
            Log.error("Index opened in read only mode, no changes allowed")

        self.debug and Log.note("Delete bugs:\n{{query}}", query=filter)

        if self.cluster.info.version.number.startswith(("5.", "6.")):
            query = {"query": filter}

            wait_for_active_shards = coalesce(  # EARLIER VERSIONS USED "consistency" AS A PARAMETER
                self.settings.wait_for_active_shards,
                {"one": 1, None: None}[self.settings.consistency]
            )
            path = self.path + "/_delete_by_query"
            # DEBUG and Log.note("Delete: {{path}}\n{{query}}", path=path, query=query)
            result = self.cluster.post(
                path,
                json=query,
                timeout=600,
                params={"wait_for_active_shards": wait_for_active_shards}
            )

            if result.failures:
                Log.error("Failure to delete fom {{index}}:\n{{data|pretty}}", index=self.settings.index, data=result)
            return result
        else:
            raise NotImplementedError

    def delete_id(self, id):
        result = self.cluster.delete(
            path=self.path + "/" + id,
            timeout=600,
            # params={"wait_for_active_shards": wait_for_active_shards}
        )
        if result.failures:
            Log.error("Failure to delete fom {{index}}:\n{{data|pretty}}", index=self.settings.index, data=result)

    def extend(self, records):
        """
        records - MUST HAVE FORM OF
            [{"value":value}, ... {"value":value}] OR
            [{"json":json}, ... {"json":json}]
            OPTIONAL "id" PROPERTY IS ALSO ACCEPTED
        """
        if self.settings.read_only:
            Log.error("Index opened in read only mode, no changes allowed")
        if not records:
            return
        if isinstance(records, generator_types):
            Log.error("single use generators no longer accepted")
        if is_text(records):
            Log.error("records must have __iter__")
        if not hasattr(records, "__iter__"):
            Log.error("records must have __iter__")

        try:
            with Timer("Add document(s) to {{index}}", {"index": self.settings.index}, verbose=self.debug):
                wait_for_active_shards = coalesce(
                    self.settings.wait_for_active_shards,
                    {"one": 1, None: None}[self.settings.consistency]
                )

                response = self.cluster.post(
                    self.path + "/_bulk",
                    data=IterableBytes(self.encode, records),
                    zip=True,
                    headers={"Content-Type": "application/x-ndjson"},
                    timeout=self.settings.timeout,
                    retry=self.settings.retry,
                    params={"wait_for_active_shards": wait_for_active_shards}
                )
                items = response["items"]

                fails = []
                if self.cluster.version.startswith(("1.4.", "1.5.", "1.6.", "1.7.", "5.", "6.")):
                    for i, item in enumerate(items):
                        if item.index.status == 409:  # 409 ARE VERSION CONFLICTS
                            if "version conflict" not in item.index.error.reason:
                                fails.append(i)  # IF NOT A VERSION CONFLICT, REPORT AS FAILURE
                        elif item.index.status not in [200, 201]:
                            fails.append(i)
                else:
                    Log.error("version not supported {{version}}", version=self.cluster.version)

                if fails:
                    lines = list(IterableBytes(self.encode, records))
                    cause = [
                        Except(
                            template="{{status}} {{error}} (and {{some}} others) while loading line id={{id}} into index {{index|quote}} (typed={{typed}}):\n{{line}}",
                            params={
                                "status": items[i].index.status,
                                "error": items[i].index.error,
                                "some": len(fails) - 1,
                                "line": strings.limit(lines[i * 2 + 1], 500 if not self.debug else 100000),
                                "index": self.settings.index,
                                "typed": self.settings.typed,
                                "id": items[i].index._id
                            }
                        )
                        for i in fails[:3]
                    ]
                    Log.error("Problems with insert", cause=cause)
            pass
        except Exception as e:
            e = Except.wrap(e)
            lines = list(IterableBytes(self.encode, records))
            if e.message.startswith("sequence item "):
                Log.error("problem with {{data}}", data=text(repr(lines[int(e.message[14:16].strip())])), cause=e)
            Log.error("problem sending to ES", cause=e)

    # RECORDS MUST HAVE id AND json AS A STRING OR
    # HAVE id AND value AS AN OBJECT
    def add(self, record):
        if self.settings.read_only:
            Log.error("Index opened in read only mode, no changes allowed")
        if is_list(record):
            Log.error("add() has changed to only accept one record, no lists")
        if record is THREAD_STOP:
            return
        self.extend([record])

    def add_property(self, name, details):
        self.debug and Log.note("Adding property {{prop}} to {{index}}", prop=name, index=self.settings.index)
        for n in jx.reverse(split_field(name)):
            if n == NESTED_TYPE:
                details = {"properties": {n: set_default(details, {"type": "nested", "dynamic": True})}}
            elif n.startswith(TYPE_PREFIX) or details.get('type', 'object') in ES_PRIMITIVE_TYPES:
                details = {"properties": {n: details}}
            else:
                details = {"properties": {n: set_default(details, {"type": "object", "dynamic": True})}}

        try:
            self.cluster.put(
                "/" + self.settings.index + "/_mapping/" + self.settings.type,
                data=details,

            )
        except Exception as e:
            if "Limit of total fields [" in e:
                self.cluster.put(
                    "/" + self.settings.index + "/_settings",
                    data={"index.mapping.total_fields.limit": 2000},
                    timeout=600
                )
                self.cluster.put(
                    "/" + self.settings.index + "/_mapping/" + self.settings.type,
                    data=details
                )
            else:
               raise e

    def set_refresh_interval(self, seconds, **kwargs):
        """
        :param seconds:  -1 FOR NO REFRESH
        :param kwargs: ANY OTHER REQUEST PARAMETERS
        :return: None
        """
        if seconds <= 0:
            interval = -1
        else:
            interval = text(int(seconds)) + "s"

        if self.cluster.version.startswith(("1.4.", "1.5.", "1.6.", "1.7.", "5.", "6.")):
            result = self.cluster.put(
                "/" + self.settings.index + "/_settings",
                data={"index": {"refresh_interval": interval}},
                **kwargs
            )

            if not result.acknowledged:
                Log.error("Can not set refresh interval ({{error}})", {
                    "error": result
                })
        else:
            Log.error("Do not know how to handle ES version {{version}}", version=self.cluster.version)

    def threaded_queue(self, batch_size=None, max_size=None, period=None, silent=False):
        """
        USE THIS TO AVOID WAITING
        """

        def errors(e, _buffer):  # HANDLE ERRORS FROM extend()
            if e.cause.cause:
                not_possible = [f for f in listwrap(e.cause.cause) if any(h in f for h in HOPELESS)]
                still_have_hope = [f for f in listwrap(e.cause.cause) if all(h not in f for h in HOPELESS)]
            else:
                not_possible = [e]
                still_have_hope = []

            if still_have_hope:
                if "429 EsRejectedExecutionException[rejected execution (queue capacity" in e:
                    Log.note("waiting for ES to be free ({{num}} pending)", num=len(_buffer))
                elif "503 UnavailableShardsException" in e:
                    Log.note("waiting for ES to initialize shards ({{num}} pending)", num=len(_buffer))
                else:
                    Log.warning("Problem with sending to ES, trying again ({{num}} pending)", num=len(_buffer), cause=still_have_hope)
            elif not_possible:
                # THERE IS NOTHING WE CAN DO
                Log.warning("Not inserted, will not try again", cause=not_possible[0:10:])
                del _buffer[:]

        return ThreadedQueue(
            "push to elasticsearch: " + self.settings.index,
            self,
            batch_size=batch_size,
            max_size=max_size,
            period=period,
            silent=silent,
            error_target=errors
        )


HOPELESS = [
    "Document contains at least one immense term",
    "400 MapperParsingException",
    "400 RoutingMissingException",
    "500 IllegalArgumentException[cannot change DocValues type",
    "JsonParseException",
    " as object, but found a concrete value"
]

known_clusters = {}  # MAP FROM (host, port) PAIR TO CLUSTER INSTANCE


class Cluster(object):

    @override
    def __new__(cls, host, port=9200, kwargs=None):
        if not is_integer(port):
            Log.error("port must be integer")
        cluster = known_clusters.get((host, int(port)))
        if cluster:
            return cluster

        cluster = object.__new__(cls)
        known_clusters[(host, port)] = cluster
        return cluster

    @override
    def __init__(self, host, port=9200, explore_metadata=True, debug=False, kwargs=None):
        """
        settings.explore_metadata == True - IF PROBING THE CLUSTER FOR METADATA IS ALLOWED
        settings.timeout == NUMBER OF SECONDS TO WAIT FOR RESPONSE, OR SECONDS TO WAIT FOR DOWNLOAD (PASSED TO requests)
        """
        if hasattr(self, "settings"):
            return

        self.settings = kwargs
        self.info = None
        self._metadata = Null
        self.index_last_updated = {}  # MAP FROM INDEX NAME TO TIME THE INDEX METADATA HAS CHANGED
        self.metadata_locker = Lock()
        self.metatdata_last_updated = Date.now()
        self.debug = debug
        self._version = None
        self.url = URL(host, port=port)
        self.lang = None
        self.known_indices = {}
        if self.version.startswith("6."):
            from jx_elasticsearch.es52.expressions import ES52
            self.lang = ES52
        else:
            Log.error("Not a know version: {{version}}", version=self.version)

    @override
    def get_or_create_index(
        self,
        index,
        alias=None,
        schema=None,
        limit_replicas=None,
        read_only=False,
        typed=None,
        refresh_interval=None,
        kwargs=None
    ):
        if kwargs.tjson != None:
            Log.error("used `typed` parameter, not `tjson`")
        best = self.get_best_matching_index(index, alias)
        if not best:
            output = self.create_index(kwargs=kwargs, schema=schema, limit_replicas=limit_replicas)
            return output
        elif best.alias != None:
            kwargs.alias = best.alias
            kwargs.index = best.index
        elif kwargs.alias == None:
            kwargs.alias = best.alias
            kwargs.index = best.index

        index = kwargs.index
        meta = self.get_metadata()
        type, about = _get_best_type_from_mapping(meta.indices[literal_field(index)].mappings)

        if typed == None:
            typed = True
            columns = parse_properties(index, ".", ROOT_PATH, about.properties)
            if len(columns) > 0:
                typed = any(
                    c.name.startswith(TYPE_PREFIX) or
                    c.name.find("." + TYPE_PREFIX) != -1
                    for c in columns
                )
            kwargs.typed = typed

        return self._new_handle_to_index(kwargs)

    def _new_handle_to_index(self, kwargs):
        key = (kwargs.index, kwargs.typed, kwargs.read_only)
        known_index = self.known_indices.get(key)
        if not known_index:
            known_index = Index(kwargs=kwargs, cluster=self)
            self.known_indices[key] = known_index

        def set_refresh(please_stop):
            try:
                known_index.set_refresh_interval(seconds=Duration(kwargs.refresh_interval).seconds)
            except Exception as e:
                Log.note("Could not set refresh interval for {{index}}", index=known_index.settings.index, cause=e)
        if kwargs.refresh_interval:
            Thread.run("setting refresh interval", set_refresh, parent_thread=MAIN_THREAD).release()
        else:
            pass
        return known_index


    @override
    def get_index(self, index, alias=None, typed=None, read_only=True, kwargs=None):
        """
        TESTS THAT THE INDEX EXISTS BEFORE RETURNING A HANDLE
        """
        if kwargs.tjson != None:
            Log.error("used `typed` parameter, not `tjson`")
        if read_only:
            # GET EXACT MATCH, OR ALIAS
            aliases = to_data(self.get_aliases())
            if index in aliases.index:
                pass
            elif index in aliases.alias:
                match = [a for a in aliases if a.alias == index][0]
                kwargs.alias = match.alias
                kwargs.index = match.index
            else:
                Log.error("Can not find index {{index_name}}", index_name=kwargs.index)
            return self._new_handle_to_index(kwargs)
        else:
            # GET BEST MATCH, INCLUDING PROTOTYPE
            best = self.get_best_matching_index(index, alias)
            if not best:
                Log.error("Can not find index {{index_name}}", index_name=kwargs.index)

            if best.alias != None:
                kwargs.alias = best.alias
                kwargs.index = best.index
            elif kwargs.alias == None:
                kwargs.alias = kwargs.index
                kwargs.index = best.index

            return self._new_handle_to_index(kwargs)

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
            self._new_handle_to_index(set_default({"read_only": True}, settings))
        Log.error("Can not find any index with alias {{alias_name}}", alias_name=alias)

    def get_canonical_index(self, alias):
        """
        RETURN THE INDEX USED BY THIS alias
        THIS IS ACCORDING TO THE STRICT LIFECYCLE RULES:
        THERE IS ONLY ONE INDEX WITH AN ALIAS
        """
        output = jx.sort(set(
            i
            for ai in self.get_aliases()
            for a, i in [(ai.alias, ai.index)]
            if a == alias or i == alias or (re.match(re.escape(alias) + "\\d{8}_\\d{6}", i) and i != alias)
        ))

        if len(output) > 1:
            Log.error("only one index with given alias==\"{{alias}}\" expected", alias=alias)

        if not output:
            return Null

        return output.last()

    def get_best_matching_index(self, index, alias=None):
        indexes = jx.sort(
            [
                p
                for pattern in [re.escape(index) + SUFFIX_PATTERN]
                for p in self.get_aliases()
                for i, a in [(p.index, p.alias)]
                if (a == index and alias == None) or
                   (re.match(pattern, i) and alias == None) or
                   (i == index and (alias == None or a == None or a == alias))
            ],
            "index"
        )
        return indexes.last()

    def get_prototype(self, alias):
        """
        RETURN ALL INDEXES THAT ARE INTENDED TO BE GIVEN alias, BUT HAVE NO
        ALIAS YET BECAUSE INCOMPLETE
        """
        output = sort([
            p.index
            for p in self.get_aliases()
            if re.match(re.escape(alias) + "\\d{8}_\\d{6}", p.index) and not p.alias
        ])
        return output

    def delete_all_but(self, prefix, name):
        """
        :param prefix: INDEX MUST HAVE THIS AS A PREFIX AND THE REMAINDER MUST BE DATE_TIME
        :param name: INDEX WITH THIS NAME IS NOT DELETED
        :return:
        """
        if prefix == name:
            Log.note("{{index_name}} will not be deleted", {"index_name": prefix})
        for a in self.get_aliases():
            # MATCH <prefix>YYMMDD_HHMMSS FORMAT
            if re.match(re.escape(prefix) + "\\d{8}_\\d{6}", a.index) and a.index != name:
                self.delete_index(a.index)

    @override
    def create_index(
        self,
        index,
        alias=None,
        create_timestamp=None,
        schema=None,
        limit_replicas=None,
        limit_replicas_warning=True,
        read_only=False,
        typed=True,
        type=None,
        kwargs=None
    ):
        if kwargs.tjson != None:
            Log.error("used `typed` parameter, not `tjson`")
        if not alias:
            requested_name = kwargs.index

            index = kwargs.index = proto_name(requested_name, create_timestamp)
            if requested_name == index:
                kwargs.alias = None
            else:
                kwargs.alias = requested_name

        if not re.match('.*' + SUFFIX_PATTERN, index):
            Log.error("Expecting index name to conform to pattern {{pattern}}", pattern=INDEX_DATE_FORMAT)

        if kwargs.schema_file:
            Log.error('schema_file attribute not supported.  Use {"$ref":<filename>} instead')

        if schema == None:
            Log.error("Expecting a schema")
        elif is_text(schema):
            Log.error("Expecting a JSON schema")
        else:
            schema = to_data(schema)

        if type != None:
            if schema.mappings:
                if type not in schema.mappings:
                    Log.error(
                        "if you declare type={{type}}, and you declare a schema, then {{type|quote}} is expected in the schema",
                        type=type
                    )
            else:
                schema.mappings = {type: {}}
        elif not schema.mappings:
            Log.error("Expecting a schema, even if only the name of the type")

        for k, m in items(schema.mappings):
            if typed:
                m = schema.mappings[k] = to_data(add_typed_annotations(m))

            m.date_detection = False  # DISABLE DATE DETECTION
            m.dynamic_templates = (
                DEFAULT_DYNAMIC_TEMPLATES +
                m.dynamic_templates
            )
            if self.version.startswith("6."):
                m.dynamic_templates = [t for t in m.dynamic_templates if "default_integer" not in t]
        if self.version.startswith("5."):
            schema.settings.index.max_result_window = None  # NOT ACCEPTED BY ES5
            schema.settings.index.max_inner_result_window = None  # NOT ACCEPTED BY ES5
            schema = json2value(value2json(schema), leaves=True)
        elif self.version.startswith("6."):
            schema = json2value(value2json(schema), leaves=True)
        else:
            schema = retro_schema(json2value(value2json(schema), leaves=True))

        if limit_replicas:
            # DO NOT ASK FOR TOO MANY REPLICAS
            health = self.get("/_cluster/health", stream=False)
            if schema.settings.index.number_of_replicas >= health.number_of_nodes:
                if limit_replicas_warning:
                    Log.warning(
                        "Reduced number of replicas for {{index}}: {{from}} requested, {{to}} realized",
                        {"from": schema.settings.index.number_of_replicas},
                        to=health.number_of_nodes - 1,
                        index=index
                    )
                schema.settings.index.number_of_replicas = health.number_of_nodes - 1

        self.put(
            "/" + index,
            data=schema,
            headers={"Content-Type": mimetype.JSON},
            stream=False
        )

        # CONFIRM INDEX EXISTS
        while not Till(seconds=30):
            try:
                metadata = self.get_metadata(after=Date.now())
                if index in metadata.indices.keys():
                    break
                Log.note("Waiting for index {{index}} to appear", index=index)
            except Exception as e:
                Log.warning("Problem while waiting for index {{index}} to appear", index=index, cause=e)
            Till(seconds=1).wait()
        Log.alert("Made new index {{index|quote}}", index=index)

        return self._new_handle_to_index(kwargs)

    def delete_index(self, index_name):
        if not is_text(index_name):
            Log.error("expecting an index name")

        self.debug and Log.note("Deleting index {{index}}", index=index_name)

        # REMOVE ALL ALIASES TOO
        aliases = [a for a in self.get_aliases() if a.index == index_name and a.alias != None]
        if aliases:
            self.post(
                path="/_aliases",
                data={"actions": [{"remove": a} for a in aliases]}
            )

        url = self.settings.host + ":" + text(self.settings.port) + "/" + index_name
        try:
            response = http.delete(url)
            if response.status_code != 200:
                Log.error("Expecting a 200, got {{code}}", code=response.status_code)
            else:
                # making the metadata stale after deletion of the index
                self.metatdata_last_updated = self.metatdata_last_updated - STALE_METADATA

            details = json2value(response.content.decode('utf8'))
            self.debug and Log.note("delete response {{response}}", response=details)
            return response
        except Exception as e:
            Log.error("Problem with call to {{url}}", url=url, cause=e)

    def get_aliases(self, after=None):
        """
        RETURN LIST OF {"alias":a, "index":i} PAIRS
        ALL INDEXES INCLUDED, EVEN IF NO ALIAS {"alias":Null}
        """
        for index, desc in self.get_metadata(after=after).indices.items():
            if not desc["aliases"]:
                yield Data(index=index)
            elif desc['aliases'][0] == index:
                Log.error("should not happen")
            else:
                for alias in desc["aliases"]:
                    yield Data(index=index, alias=alias)

    def get_metadata(self, after=None):
        now = Date.now()

        if not self.settings.explore_metadata:
            Log.error("Metadata exploration has been disabled")
        if not after and self._metadata and now < self.metatdata_last_updated + STALE_METADATA:
            return self._metadata
        if after <= self.metatdata_last_updated:
            return self._metadata

        old_indices = self._metadata.indices
        response = self.get("/_cluster/state", retry={"times": 3}, timeout=30, stream=False)

        self.debug and Log.note("Got metadata for {{cluster}} at {{time}}", cluster=self.url, time=now)
        self.metatdata_last_updated = now  # ONLY UPDATE AFTER WE GET A RESPONSE

        with self.metadata_locker:
            self._metadata = to_data(response.metadata)
            for new_index_name, new_meta in self._metadata.indices.items():
                old_index = old_indices[literal_field(new_index_name)]
                if not old_index:
                    DEBUG_METADATA_UPDATE and Log.note("New index found {{index}} at {{time}}", index=new_index_name, time=now)
                    self.index_last_updated[new_index_name] = now
                else:
                    for type_name, new_about in new_meta.mappings.items():
                        old_about = old_index.mappings[type_name]
                        diff = diff_schema(new_about.properties, old_about.properties)
                        if diff:
                            DEBUG_METADATA_UPDATE and Log.note("More columns found in {{index}} at {{time}}", index=new_index_name, time=now)
                            self.index_last_updated[new_index_name] = now
            for old_index_name, old_meta in old_indices.items():
                new_index = self._metadata.indices[literal_field(old_index_name)]
                if not new_index:
                    DEBUG_METADATA_UPDATE and Log.note("Old index lost: {{index}} at {{time}}", index=old_index_name, time=now)
                    self.index_last_updated[old_index_name] = now
        self.info = to_data(self.get("/", stream=False))
        self._version = self.info.version.number
        return self._metadata

    @property
    def version(self):
        if self._version is None:
            self.get_metadata()
        return self._version

    def post(self, path, **kwargs):
        url = self.url / path  # self.settings.host + ":" + text(self.settings.port) + path

        data = kwargs.get(DATA_KEY)
        if data == None:
            pass
        elif is_data(data):
            data = kwargs[DATA_KEY] = value2json(data).encode('utf8')
        elif is_text(data):
            data = kwargs[DATA_KEY] = data.encode('utf8')
        elif hasattr(data, str("__iter__")):
            pass  # ASSUME THIS IS AN ITERATOR OVER BYTES
        else:
            Log.error("data must be utf8 encoded string")

        kwargs['stream'] = True

        try:
            heads = to_data(kwargs).headers
            heads["Accept-Encoding"] = "gzip,deflate"
            heads["Content-Type"] = mimetype.JSON

            if self.debug:
                if is_binary(data):
                    sample = kwargs.get(DATA_KEY, b"")[:300]
                    Log.note("{{url}}:\n{{data|indent}}", url=url, data=sample)
                else:
                    Log.note("{{url}}:\n\t<stream>", url=url)

            self.debug and Log.note("POST {{url}}", url=url)
            response = http.post(url, **kwargs)
            if response.status_code not in [200, 201]:
                Log.error(text(response.reason) + ": " + strings.limit(response.content.decode("latin1"), 1000 if self.debug else 10000))
            self.debug and Log.note("response: {{response}}", response=(response.content.decode('utf8'))[:130])

            # TODO: allow streaming of response.content.hits.hits
            details = json2value(response.content.decode('utf8'))
            if details.error:
                Log.error(quote2string(details.error))
            if details._shards.failed > 0:
                Log.error(
                    "{{num}} orf {{total}} shard failures {{failures|indent}}",
                    failures=details._shards.failures.reason,
                    num=details._shards.failed,
                    total=details._shards.total
                )
            return details
        except Exception as e:
            e = Except.wrap(e)
            if url.scheme != "http":
                suggestion = " (did you forget \"http://\" prefix on the host name?)"
            else:
                suggestion = ""

            if is_binary(data):
                Log.error(
                    "Problem with call to {{url}}" + suggestion + "\n{{body|left(10000)}}",
                    url=url,
                    body=strings.limit(kwargs[DATA_KEY].decode('utf8'), 500 if self.debug else 10000),
                    cause=e
                )
            else:
                Log.error("Problem with call to {{url}}" + suggestion, url=url, cause=e)

    def delete(self, path, **kwargs):
        url = self.settings.host + ":" + text(self.settings.port) + path
        try:
            response = http.delete(url, **kwargs)
            if response.status_code not in [200]:
                Log.error(response.reason + ": " + response.all_content)
            self.debug and Log.note("response: {{response}}", response=strings.limit(response.all_content.decode('utf8'), 500))
            details = json2value(response.all_content.decode('utf8'))
            if details.error:
                Log.error(details.error)
            return details
        except Exception as e:
            Log.error("Problem with call to {{url}}", url=url, cause=e)

    def get(self, path, **kwargs):
        url = self.settings.host + ":" + text(self.settings.port) + path
        try:
            self.debug and Log.note("GET {{url}}", url=url)
            response = http.get(url, **kwargs)
            if response.status_code not in [200]:
                Log.error(response.reason + ": " + response.all_content)
            self.debug and Log.note("response: {{response}}", response=strings.limit(response.all_content.decode('utf8'), 500))
            details = json2value(response.all_content.decode('utf8'))
            if details.error:
                Log.error(details.error)
            return details
        except Exception as e:
            Log.error("Problem with call to {{url}}", url=url, cause=e)

    def head(self, path, **kwargs):
        url = self.settings.host + ":" + text(self.settings.port) + path
        try:
            response = http.head(url, **kwargs)
            if response.status_code not in [200]:
                Log.error(response.reason + ": " + response.all_content)
            self.debug and Log.note("response: {{response}}", response=strings.limit(response.all_content.decode('utf8'), 500))
            if response.all_content:
                details = json2value(response.all_content.decode('utf8'))
                if details.error:
                    Log.error(details.error)
                return details
            else:
                return None  # WE DO NOT EXPECT content WITH HEAD REQUEST
        except Exception as e:
            Log.error("Problem with call to {{url}}", url=url, cause=e)

    def put(self, path, **kwargs):
        url = self.settings.host + ":" + text(self.settings.port) + path

        heads = to_data(kwargs).headers
        heads[text("Accept-Encoding")] = text("gzip,deflate")
        heads[text("Content-Type")] = mimetype.JSON

        data = kwargs.get(DATA_KEY)
        if data == None:
            pass
        elif is_data(data):
            kwargs[DATA_KEY] = value2json(data).encode('utf8')
        elif is_text(kwargs[DATA_KEY]):
            pass
        else:
            Log.error("data must be utf8 encoded string")

        if self.debug:
            sample = kwargs.get(DATA_KEY, "")[:1000]
            Log.note("{{url}}:\n{{data|indent}}", url=url, data=sample)
        try:
            response = http.put(url, **kwargs)
            if response.status_code not in [200]:
                Log.error("{{reason}}: {{content|limit(3000)}}", reason=response.reason, content=response.content)
            if not response.content:
                return Null

            self.debug and Log.note("response: {{response}}", response=(response.content.decode('utf8'))[0:300:])

            details = json2value(response.content.decode('utf8'))
            if details.error:
                Log.error(quote2string(details.error))
            if details._shards.failed > 0:
                Log.error(
                    "Shard failures {{failures|indent}}",
                    failures="---\n".join(r.replace(";", ";\n") for r in details._shards.failures.reason)
                )
            return details
        except Exception as e:
            Log.error("Problem with call to {{url}}", url=url, cause=e)


def export_schema(cluster, metadata):
    aliases = set(a for i, settings in metadata.indices.items() for a in settings.aliases)
    output = []

    for a in aliases:
        i = cluster.get_best_matching_index(a).index
        output.append("## "+a+"\n")
        output.append(strings.indent(value2json(metadata.indices[i].mappings.values()[0].properties, pretty=True), "    "))
        output.append("\n")

    File("temp" + text(cluster.url.port) + ".md").write(output)


def proto_name(prefix, timestamp=None):
    suffix = re.search(SUFFIX_PATTERN, prefix)
    if suffix:
        start, stop = suffix.regs[0]
        if stop == len(prefix):
            return prefix

    if not timestamp:
        timestamp = Date.now()
    else:
        timestamp = Date(timestamp)
    return prefix + timestamp.format(INDEX_DATE_FORMAT)


def sort(values):
    return to_data(sorted(values))


def scrub(r):
    """
    REMOVE KEYS OF DEGENERATE VALUES (EMPTY STRINGS, EMPTY LISTS, AND NULLS)
    CONVERT STRINGS OF NUMBERS TO NUMBERS
    RETURNS **COPY**, DOES NOT CHANGE ORIGINAL
    """
    return to_data(_scrub(r))


def _scrub(r):
    try:
        if r == None:
            return None
        elif r.__class__ in (text, binary_type):
            if r == "":
                return None
            return r
        elif is_number(r):
            return value2number(r)
        elif is_data(r):
            if r.__class__ is Data:
                r = object.__getattribute__(r, SLOT)
            output = {}
            for k, v in r.items():
                v = _scrub(v)
                if v != None:
                    output[k.lower()] = v
            if len(output) == 0:
                return None
            return output
        elif hasattr(r, '__iter__'):
            if isinstance(r, FlatList):
                r = r.list
            output = []
            for v in r:
                v = _scrub(v)
                if v != None:
                    output.append(v)
            if not output:
                return None
            elif len(output) == 1:
                return output[0]
            else:
                return output
        else:
            return r
    except Exception as e:
        Log.warning("Can not scrub: {{json}}", json=r, cause=e)


def parse_properties(parent_index_name, parent_name, nested_path, esProperties):
    """
    RETURN THE COLUMN DEFINITIONS IN THE GIVEN esProperties OBJECT
    """
    columns = FlatList()

    if parent_name == '.':
        # ROOT PROPERTY IS THE ELASTICSEARCH INDEX (A NESTED ARRAY OF OBJECTS)
        columns.append(Column(
            name='.',
            es_index=parent_index_name,
            es_column='.',
            es_type="nested",
            jx_type=NESTED,
            cardinality=1,
            last_updated=Date.now(),
            nested_path=nested_path,
            multi=1001
        ))
        columns.append(Column(
            name='.',
            es_index=parent_index_name,
            es_column='.',
            es_type="object",
            jx_type=OBJECT,
            cardinality=1,
            last_updated=Date.now(),
            nested_path=[parent_index_name]+nested_path,
            multi=1
        ))

    for short_name, property in esProperties.items():
        index_name = parent_index_name
        column_name = concat_field(parent_name, short_name)
        jx_name = column_name

        if property.type == "nested" and property.properties:
            # NESTED TYPE IS A NEW TYPE DEFINITION
            # MARKUP CHILD COLUMNS WITH THE EXTRA DEPTH
            self_columns = parse_properties(index_name, column_name, [column_name] + nested_path, property.properties)
            columns.extend(self_columns)
            columns.append(Column(
                name=jx_name,
                es_index=index_name,
                es_column=column_name,
                es_type="nested",
                jx_type=NESTED,
                cardinality=1,
                multi=1001,
                last_updated=Date.now(),
                nested_path=nested_path
            ))
            columns.append(Column(
                name=jx_name,
                es_index=index_name,
                es_column=column_name,
                es_type="object",
                jx_type=OBJECT,
                cardinality=1,
                multi=1,
                last_updated=Date.now(),
                nested_path=[column_name] + nested_path
            ))

            continue

        if property.properties:
            child_columns = parse_properties(index_name, column_name, nested_path, property.properties)
            columns.extend(child_columns)
            columns.append(Column(
                name=jx_name,
                es_index=index_name,
                es_column=column_name,
                es_type="source" if property.enabled == False else "object",
                jx_type=OBJECT,
                cardinality=1,
                last_updated=Date.now(),
                nested_path=nested_path,
                multi=1
            ))

        if property.dynamic:
            continue
        if not property.type:
            continue

        cardinality = 0 if not (property.store or property.enabled) and short_name != '_id' else None

        if property.fields:
            child_columns = parse_properties(index_name, column_name, nested_path, property.fields)
            if cardinality is None:
                for cc in child_columns:
                    cc.cardinality = None
            columns.extend(child_columns)

        if short_name == EXISTS_TYPE:
            columns.append(Column(
                name=jx_name,
                es_index=index_name,
                es_column=column_name,
                es_type=property.type,
                jx_type=EXISTS,
                cardinality=1,
                last_updated=Date.now(),
                nested_path=nested_path,
                multi=1
            ))
        elif property.type in es_type_to_json_type.keys():
            columns.append(Column(
                name=jx_name,
                es_index=index_name,
                es_column=column_name,
                es_type=property.type,
                jx_type=es_type_to_json_type[property.type],
                cardinality=cardinality,
                last_updated=Date.now(),
                nested_path=nested_path,
                multi=1
            ))
            if property.index_name and short_name != property.index_name:
                columns.append(Column(
                    name=jx_name,
                    es_index=index_name,
                    es_column=column_name,
                    es_type=property.type,
                    jx_type=es_type_to_json_type[property.type],
                    cardinality=0 if property.store else None,
                    last_updated=Date.now(),
                    nested_path=nested_path,
                    multi=1
                ))
        elif property.enabled == None or property.enabled == False:
            columns.append(Column(
                name=jx_name,
                es_index=index_name,
                es_column=column_name,
                es_type="source" if property.enabled == False else "object",
                jx_type=OBJECT,
                cardinality=0 if property.store else None,
                last_updated=Date.now(),
                nested_path=nested_path,
                multi=1
            ))
        else:
            Log.warning("unknown type {{type}} for property {{path}}", type=property.type, path=parent_name)

    return columns


def _get_best_type_from_mapping(mapping):
    """
    THERE ARE MULTIPLE TYPES IN AN INDEX, PICK THE BEST
    :param mapping: THE ES MAPPING DOCUMENT
    :return: (type_name, mapping) PAIR (mapping.properties WILL HAVE PROPERTIES
    """
    best_type_name = None
    best_mapping = None
    for k, m in mapping.items():
        if k == "_default_":
            continue
        if best_type_name is None or len(m.properties) > len(best_mapping.properties):
            best_type_name = k
            best_mapping = m
    if best_type_name == None:
        return "_default_", mapping["_default_"]
    return best_type_name, best_mapping


def get_encoder(id_info):
    get_id = jx.get(id_info.field)
    get_version = jx.get(id_info.version)

    def _encoder(r):
        id = r.get("id")
        r_value = r.get('value')
        if is_data(r_value):
            r_id = get_id(r_value)
            r_value.pop('_id', None)
            if id == None:
                id = r_id
            elif id != r_id and r_id != None:
                Log.error("Expecting id ({{id}}) and _id ({{_id}}) in the record to match", id=id, _id=r._id)
        if id == None:
            id = random_id()

        version = get_version(r_value)

        if "json" in r:
            Log.error("can not handle pure json inserts anymore")
            json = r["json"]
        elif r_value or is_data(r_value):
            json = value2json(r_value)
        else:
            raise Log.error("Expecting every record given to have \"value\" or \"json\" property")

        return id, version, json

    return _encoder


def random_id():
    return randoms.base64(25, extra="-_")


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

            if b_details.type in ES_STRUCT:
                _merge_mapping(a_details.properties, b_details.properties)
        else:
            a[literal_field(name)] = deepcopy(b_details)

    return a


def retro_schema(schema):
    """
    CONVERT SCHEMA FROM 5.x to 1.x
    :param schema:
    :return:
    """
    output = dict_to_data({
        "mappings": {
            typename: {
                "dynamic_templates": [
                    retro_dynamic_template(*(t.items()[0]))
                    for t in details.dynamic_templates
                ],
                "properties": retro_properties(details.properties)
            }
            for typename, details in schema.mappings.items()
        },
        "settings": schema.settings
    })
    return output


def retro_dynamic_template(name, template):
    template.mapping.doc_values = True
    if template.mapping.type == "keyword":
        template.mapping.type = "string"
        template.mapping.index = "not_analyzed"
    elif template.mapping.type == "text":
        template.mapping.type = "string"
        template.mapping.index = "analyzed"
    elif template.mapping.type == "string":
        template.mapping.type = "string"
        template.mapping.index = "analyzed"
    return {name: template}


def retro_properties(properties):
    output = {}
    for k, v in properties.items():
        v.doc_values = True
        v.fielddata = None
        if v.type == "keyword":
            v.type = "string"
            v.index = "not_analyzed"
        elif v.type == "text":
            v.type = "string"
            v.index = "analyzed"
            v.doc_values = None

        if v.properties:
            v.properties = retro_properties(v.properties)

        if v.fields:
            v.fields = retro_properties(v.fields)
            v.fields[k] = {
                "type": v.type,
                "index": v.index,
                "doc_values": v.doc_values,
                "analyzer": v.analyzer
            }
            v.type = "multi_field"
            v.index = None
            v.doc_values = None
            v.analyzer = None
        output[k] = v
    return output


def add_typed_annotations(meta):
    if meta.type in ["text", "keyword", "string", "float", "double", "integer", "boolean"]:
        return {
            "type": "object",
            "dynamic": True,
            "properties": {
                json_type_to_inserter_type[es_type_to_json_type[meta.type]]: meta,
                EXISTS_TYPE: {"type": "long", "store": True}
            }
        }
    else:
        output = {}
        for meta_name, meta_value in meta.items():
            if meta_name == 'properties':
                output[meta_name] = {
                    prop_name: add_typed_annotations(about) if prop_name not in [BOOLEAN_TYPE, NUMBER_TYPE, STRING_TYPE, BOOLEAN_TYPE] else about
                    for prop_name, about in meta_value.items()
                }
                output[meta_name][EXISTS_TYPE] = {"type": "long", "store": True}
            else:
                output[meta_name] = meta_value

        return output


def diff_schema(A, B):
    """
    RETURN PROPERTIES IN A, BUT NOT IN B
    :param A: elasticsearch properties
    :param B: elasticsearch properties
    :return: (name, properties) PAIRS WHERE name IS DOT-DELIMITED PATH
    """
    output = []

    def _diff_schema(path, A, B):
        for k, av in A.items():
            if k == "_id" and path == ".":
                continue  # DO NOT ADD _id TO ANY SCHEMA DIFF
            bv = B[k]
            if bv == None:
                output.append((concat_field(path, k), av))
            elif av.type == bv.type:
                pass  # OK
            elif (av.type == None and bv.type == 'object') or (av.type == 'object' and bv.type == None):
                pass  # OK
            else:
                Log.warning("inconsistent types: {{typeA}} vs {{typeB}}", typeA=av.type, typeB=bv.type)
            _diff_schema(concat_field(path, k), av.properties, bv.properties)

    # what to do with conflicts?
    _diff_schema(".", A, B)
    return output


DEFAULT_DYNAMIC_TEMPLATES = list_to_data([
    {
        "default_typed_boolean": {
            "mapping": {
                "type": "boolean",
                "store": True,
                "norms": False
            },
            "match": BOOLEAN_TYPE
        }
    },
    {
        "default_typed_number": {
            "mapping": {
                "type": "double",
                "store": True,
                "norms": False
            },
            "match": NUMBER_TYPE
        }
    },
    {
        "default_typed_string": {
            "mapping": {
                "type": "keyword",
                "store": True,
                "norms": False
            },
            "match": STRING_TYPE
        }
    },
    {
        "default_typed_exist": {
            "mapping": {
                "type": "long",
                "store": True,
                "norms": False
            },
            "match": EXISTS_TYPE
        }
    },
    {
        "default_typed_nested": {
            "mapping": {
                "type": "nested",
                "store": True,
                "norms": False
            },
            "match": NESTED_TYPE
        }
    },
    {
        "default_string": {
            "mapping": {
                "type": "keyword",
                "store": True,
                "norms": False
            },
            "match_mapping_type": "string"
        }
    },
    {
        "default_long": {
            "mapping": {"type": "long", "store": True},
            "match_mapping_type": "long"
        }
    },
    {
        "default_double": {
            "mapping": {"type": "double", "store": True},
            "match_mapping_type": "double"
        }
    },
    {
        "default_integer": {
            "mapping": {"type": "integer", "store": True},
            "match_mapping_type": "integer"
        }
    }
])

es_type_to_json_type = {
    "text": STRING,
    "string": STRING,
    "keyword": STRING,
    "float": NUMBER,
    "double": NUMBER,
    "long": NUMBER,
    "integer": NUMBER,
    "object": OBJECT,
    "nested": NESTED,
    "source": "json",
    "boolean": BOOLEAN,
    "exists": EXISTS
}

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


class IterableBytes(object):
    def __init__(self, encode, records):
        """
        DO NOT SERIALIZE TO BYTES UNTIL REQUIRED

        :param encode: FUNCTION TO ENCODE INTO JSON TEXT
        :param records: EXPECTING OBJECT WITH __iter__()
        """
        self.encode = encode
        self.records = records

    def __iter__(self):
        for r in self.records:
            try:
                if '_id' in r or 'value' not in r:  # I MAKE THIS MISTAKE SO OFTEN, I NEED A CHECK
                    Log.error('Expecting {"id":id, "value":document} form.  Not expecting _id')
                id, version, json_text = self.encode(r)

                if DEBUG and not json_text.startswith('{'):
                    self.encode(r)
                    Log.error("string {{doc}} will not be accepted as a document", doc=json_text)

                if version:
                    yield value2json({"index": {"_id": id, "version": int(version), "version_type": "external_gte"}}).encode('utf8')
                else:
                    yield ('{"index":{"_id": ' + value2json(id) + '}}').encode('utf8')
                yield LF
                yield json_text.encode('utf8')
                yield LF
            except Exception as cause:
                Log.error("Can not encode {{record|json}}", record=r, cause=cause)

lists.sequence_types = lists.sequence_types + (IterableBytes,)


def quote2string(value):
    with suppress_exception:
        return ast.literal_eval(value)


def value2number(v):
    try:
        if isinstance(v, float) and round(v, 0) != v:
            return v
            # IF LOOKS LIKE AN INT, RETURN AN INT
        return int(v)
    except Exception:
        try:
            return float(v)
        except Exception as e:
            Log.error("Not a number ({{value}})",  value= v, cause=e)

