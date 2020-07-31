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

import itertools
from datetime import date, datetime
from decimal import Decimal

import jx_base
from jx_base import TableDesc, Column
from jx_base.expressions import FALSE
from jx_base.meta_columns import (
    META_COLUMNS_DESC,
    META_COLUMNS_NAME,
    META_TABLES_DESC,
    META_TABLES_NAME,
)
from jx_base.table import Table as BaseTable
from jx_base.namespace import Namespace
from jx_base.expressions import QueryOp
from jx_elasticsearch import elasticsearch
from jx_elasticsearch.elasticsearch import (
    _get_best_type_from_mapping,
    es_type_to_json_type,
)
from jx_elasticsearch.meta_columns import ColumnList
from jx_python import jx
from jx_python.containers.list import ListContainer
from mo_dots import (
    Data,
    FlatList,
    NullType,
    ROOT_PATH,
    coalesce,
    concat_field,
    is_list,
    literal_field,
    relative_field,
    set_default,
    split_field,
    startswith_field,
    tail_field,
    listwrap,
    unwrap,
    to_data,
)
from mo_dots.lists import last
from mo_future import first, long, none_type, text
from mo_json import BOOLEAN, EXISTS, OBJECT, INTERNAL, STRUCT
from mo_json.typed_encoder import (
    unnest_path,
    untype_path,
    NESTED_TYPE,
    get_nested_path,
)
from mo_kwargs import override
from mo_logs import Log
from mo_logs.exceptions import Except
from mo_logs.strings import quote
from mo_threads import Queue, THREAD_STOP, Thread, Till, MAIN_THREAD
from mo_times import Date, HOUR, MINUTE, Timer, WEEK, Duration

DEBUG = False
ENABLE_META_SCAN = True
TOO_OLD = 24 * HOUR
OLD_METADATA = MINUTE
MAX_COLUMN_METADATA_AGE = "12hour"
TEST_TABLE_PREFIX = "testing"  # USED TO TURN OFF COMPLAINING ABOUT TEST INDEXES
TABLE_DOES_NOT_EXIST = "{{table|quote}} does not exist"

known_clusters = {}  # MAP FROM id(Cluster) TO ElasticsearchMetadata INSTANCE

KNOWN_MULTITYPES = ["build.type", "run.type", "build.platform", "file.path"]


class ElasticsearchMetadata(Namespace):
    """
    MANAGE SNOWFLAKE SCHEMAS FOR EACH OF THE ALIASES FOUND IN THE CLUSTER
    """

    @override
    def __new__(cls, kwargs, *args, **_kwargs):
        es_cluster = elasticsearch.Cluster(kwargs)  # NOTICE cls IS PASSED IN
        output = known_clusters.get(id(es_cluster))
        if output is None:
            output = object.__new__(cls)
            known_clusters[id(es_cluster)] = output
        return output

    @override
    def __init__(self, host, index, alias=None, name=None, port=9200, kwargs=None):
        if hasattr(self, "settings"):
            return

        self.settings = kwargs
        self.too_old = TOO_OLD
        self.es_cluster = elasticsearch.Cluster(kwargs=kwargs)
        self.index_does_not_exist = set()
        self.todo_priority = False
        self.todo = Queue("refresh metadata", max=100000, unique=True)

        self.meta = Data()
        self.meta.columns = ColumnList(self.es_cluster)
        self.meta.columns.extend(META_TABLES_DESC.columns)
        self.meta.tables = ListContainer(
            META_TABLES_NAME, [], jx_base.Schema(".", META_TABLES_DESC.columns)
        )
        self.meta.table.extend([META_COLUMNS_DESC, META_TABLES_DESC])
        self.alias_to_query_paths = {}
        for i, settings in self.es_cluster.get_metadata().indices.items():
            if len(settings.aliases) == 0:
                alias = i
            elif len(settings.aliases) == 1:
                alias = first(settings.aliases)
            else:
                Log.error("expecting only one alias per index")

            desc = TableDesc(
                name=alias,
                url=None,
                query_path=ROOT_PATH,
                last_updated=self.es_cluster.metatdata_last_updated,
                columns=[],
            )
            self.meta.tables.add(desc)
            self.alias_to_query_paths[alias] = [desc.query_path]
            self.alias_to_query_paths[self._find_alias(alias)] = [desc.query_path]

        # WE MUST PAUSE?

        # TODO: fix monitor so it does not bring down ES
        if ENABLE_META_SCAN:
            self.worker = Thread.run(
                "refresh metadata", self.monitor, parent_thread=MAIN_THREAD
            )
        else:
            self.worker = Thread.run(
                "not refresh metadata for " + host,
                self.not_monitor,
                parent_thread=MAIN_THREAD,
            )
        return

    @property
    def namespace(self):
        return self.meta.columns.namespace

    def _reload_columns(self, table_desc, after):
        """
        ENSURE ALL INDICES FOR A GIVEN ALIAS HAVE THE SAME COLUMNS

        :param alias: A REAL ALIAS (OR NAME OF INDEX THAT HAS NO ALIAS)
        :param after: ENSURE DATA IS YOUNGER THAN after
        :return:
        """

        # FIND ALL INDEXES OF ALIAS
        alias = table_desc.name
        metadata = self.es_cluster.get_metadata(after=after)
        canonical_index = self.es_cluster.get_best_matching_index(alias).index

        props = [
            # NOTICE THIS TRIPLE (index, type, properties)
            (self.es_cluster.get_index(index=i, type=t, debug=DEBUG), t, m.properties)
            for i, d in metadata.indices.items()
            if alias in d.aliases
            for t, m in [_get_best_type_from_mapping(d.mappings)]
        ]

        # CONFIRM ALL COLUMNS ARE SAME, FIX IF NOT
        dirty = 0
        all_comparisions = (
            list(jx.pairwise(props)) + list(jx.pairwise(jx.reverse(props)))
        )
        # NOTICE THE SAME (index, type, properties) TRIPLE FROM ABOVE
        for (i1, t1, p1), (i2, t2, p2) in all_comparisions:
            diff = elasticsearch.diff_schema(p2, p1)
            for name, es_details in diff:
                if es_details.type in {"object", "nested"}:
                    # QUERYING OBJECTS RETURNS NOTHING
                    continue
                col = first(self.meta.columns.find(alias, name))
                if col and col.last_updated > after and col.cardinality <= 0:
                    continue
                if col and col.jx_type in INTERNAL:
                    continue
                for i, t, _ in props:
                    if i is not i1:  # WE KNOW IT IS NOT IN i1 BECAUSE diff SAYS SO
                        try:
                            # TODO: THIS TAKES A LONG TIME, CACHE IN THE COLUMN METADATA?
                            # MAY NOT WORK - COLUMN METADATA IS FOR ALIASES, NOT INDEXES
                            result = i.search({
                                "query": {"exists": {"field": name}},
                                "size": 0,
                            })
                            if result.hits.total > 0:
                                dirty += 1
                                i1.add_property(name, es_details)
                                break
                        except Exception as e:
                            Log.warning(
                                "problem adding field {{field}}", field=name, cause=e,
                            )
                else:
                    # ALL OTHER INDEXES HAVE ZERO RECORDS FOR THIS COLUMN
                    zero_col = Column(
                        name=name,
                        es_column=name,
                        es_index=alias,
                        es_type=es_details.type,
                        jx_type=es_type_to_json_type[coalesce(
                            es_details.type, "object"
                        )],
                        nested_path=get_nested_path(name),
                        count=0,
                        cardinality=0,  # MARKED AS DELETED
                        multi=1001 if es_details.type == "nested" else 0,
                        partitions=None,
                        last_updated=Date.now(),
                    )
                    if len(zero_col.nested_path) > 1:
                        pass
                    self.meta.columns.add(zero_col)
        if dirty:
            metadata = self.es_cluster.get_metadata(after=Date.now())

        now = self.es_cluster.metatdata_last_updated
        meta = metadata.indices[literal_field(canonical_index)]
        es_details, mapping = _get_best_type_from_mapping(meta.mappings)
        mapping.properties["_id"] = {"type": "keyword"}
        columns = self._parse_properties(alias, mapping)
        table_desc.last_updated = now

        existing_columns = {c.es_column for c in columns}
        # DELETE SOME COLUMNS
        try:
            current_columns = self.meta.columns.find(alias)
            for c in current_columns:
                if c.es_column not in existing_columns:
                    self.meta.columns.remove(c, now)
        except Exception as e:
            Log.warning("problem removing columns from {{table}}", table=alias, cause=e)

        # ASK FOR COLUMNS TO BE RE-SCANNED
        rescan = [
            (c, after)
            for c in columns
            if c.es_index != META_COLUMNS_NAME
            and (c.cardinality == None or not (c.last_updated > after))
        ]
        # PUSH THESE COLUMNS SO THEY ARE SCANNED FIRST
        # WE ARE ASSUMING THIS TABLE IS HIGHER PRIORITY THAN SOME
        # BACKLOG CURRENTLY IN THE todo QUEUE
        self.todo.push_all(rescan)
        self.todo_priority = True
        DEBUG and Log.note(
            "asked for {{num}} columns to be rescanned for {{alias}}",
            num=len(rescan),
            alias=alias,
        )
        return columns

    def _parse_properties(self, alias, mapping):
        """
        PARSE THE mapping, UPDATE self.meta.columns, AND RETURN CANONICAL COLUMNS
        :param alias:
        :param mapping:
        :return:
        """

        abs_columns = elasticsearch.parse_properties(
            alias, ".", ROOT_PATH, mapping.properties
        )
        if DEBUG and any(c.cardinality == 0 and c.name != "_id" for c in abs_columns):
            Log.note(
                "Some columns are always missing in {{url}} {{index|quote}}"
                " table:\n{{names}}",
                url=self.es_cluster.url,
                index=alias,
                names=[
                    ".".join((c.es_index, c.name))
                    for c in abs_columns
                    if c.cardinality == 0
                ],
            )

        with Timer(
            "upserting {{num}} columns", {"num": len(abs_columns)}, verbose=DEBUG
        ):
            # LIST OF EVERY NESTED PATH
            query_paths = [[c.es_column] for c in abs_columns if c.es_type == "nested"]
            for a, b in itertools.product(query_paths, query_paths):
                aa = a[0]
                bb = b[0]
                if aa and bb.startswith(aa):
                    for i, b_prefix in enumerate(b):
                        if len(b_prefix) > len(aa):
                            continue
                        if aa == b_prefix:
                            break  # SPLIT ALREADY FOUND
                        b.insert(i, aa)
                        break
            for q in query_paths:
                q.append(".")
            query_paths.append(ROOT_PATH)

            # ENSURE ALL TABLES HAVE THE QUERY PATHS SET
            self.alias_to_query_paths[alias] = query_paths
            self.alias_to_query_paths[self._find_alias(alias)] = query_paths

            # REGISTER ALL COLUMNS
            canonicals = []
            for abs_column in abs_columns:
                canonical = self.meta.columns.add(abs_column)
                canonicals.append(canonical)

            return canonicals

    def query(self, _query):
        return self.meta.columns.query(QueryOp(set_default(
            {"from": self.meta.columns, "sort": ["table", "name"]}, _query.__data__(),
        )))

    def _find_alias(self, name):
        indices = self.es_cluster.get_metadata().indices
        settings = indices[name]
        if settings:
            aliases = settings.aliases
            if not aliases:
                return name
            else:
                return aliases[0]

        for settings in indices.values():
            if name in settings.aliases:
                return name

    def get_columns(self, table_name, column_name=None, after=None, timeout=None):
        """
        RETURN METADATA COLUMNS

        :param table_name: TABLE WE WANT COLUMNS FOR
        :param column_name:  OPTIONAL NAME, IF INTERESTED IN ONLY ONE COLUMN
        :param after: FORCE LOAD, WAITING FOR last_updated TO BE AFTER THIS TIME
        :param timeout: Signal; True when should give up
        :return:
        """
        DEBUG and after and Log.note(
            "getting columns for {{table}} after {{time}}", table=table_name, time=after
        )
        if table_name == META_TABLES_NAME:
            return self.meta.tables.schema.columns
        elif table_name == META_COLUMNS_NAME:
            root_table_name = table_name
        else:
            root_table_name, _ = tail_field(table_name)

        alias = self._find_alias(root_table_name)
        if not alias:
            self.es_cluster.get_metadata(after=after)
            alias = self._find_alias(root_table_name)
            if not alias:
                Log.error(TABLE_DOES_NOT_EXIST, table=table_name)

        try:
            table = self.get_table(alias)
            # LAST TIME WE GOT INFO FOR THIS TABLE
            if table == None:
                table = TableDesc(
                    name=alias,
                    url=None,
                    query_path=["."],
                    last_updated=Date.MIN,
                    columns=[],
                )
                with self.meta.tables.locker:
                    self.meta.tables.add(table)
                columns = self._reload_columns(table, after=after)
            elif after and table.last_updated < after:
                columns = self._reload_columns(table, after=after)
            elif table.last_updated < self.es_cluster.metatdata_last_updated:
                # TODO: THIS IS TOO EXTREME; WE SHOULD WAIT FOR SOME SENSE OF "OLDNESS"
                columns = self._reload_columns(
                    table, after=self.es_cluster.metatdata_last_updated
                )
            else:
                columns = self.meta.columns.find(alias, column_name)

            columns = jx.sort(columns, "name")

            if after is None:
                return columns  # DO NOT WAIT FOR COMPLETE COLUMNS

            # WAIT FOR THE COLUMNS TO UPDATE
            while True:
                pending = [c for c in columns if after >= c.last_updated]
                if not pending:
                    break
                if timeout:
                    Log.error("trying to gets columns timed out")
                if DEBUG:
                    if len(pending) > 10:
                        Log.note(
                            "waiting for {{num}} columns to update by {{timestamp}}",
                            num=len(pending),
                            timestamp=after,
                        )
                    else:
                        Log.note(
                            "waiting for columns to update by {{timestamp}};"
                            " {{columns|json}}",
                            timestamp=after,
                            columns=[
                                concat_field(c.es_index, c.es_column)
                                + " id="
                                + text(id(c))
                                for c in pending
                            ],
                        )
                Till(seconds=1).wait()
            return columns
        except Exception as e:
            Log.error("Failure to get columns for {{table}}", table=table_name, cause=e)

        return []

    def _update_cardinality(self, column):
        """
        QUERY ES TO FIND CARDINALITY AND PARTITIONS FOR A SIMPLE COLUMN
        """
        now = Date.now()
        if column.es_index in self.index_does_not_exist:
            return

        if column.jx_type in STRUCT:
            Log.error("not supported")
        try:
            if column.es_index == META_TABLES_NAME:
                partitions = jx.sort([
                    g[column.es_column]
                    for g, _ in jx.groupby(self.meta.tables, column.es_column)
                    if g[column.es_column] != None
                ])
                self.meta.columns.update({
                    "set": {
                        "partitions": partitions,
                        "count": len(self.meta.tables),
                        "cardinality": len(partitions),
                        "multi": 1,
                        "last_updated": now,
                    },
                    "where": {"eq": {
                        "es_index": column.es_index,
                        "es_column": column.es_column,
                    }},
                })
                return
            if column.es_index == META_COLUMNS_NAME:
                DEBUG and Log.note(
                    "{{column.es_column}} is metadata, not scanned", column=column
                )
                return

            es_index = column.es_index.split(".")[0]

            is_es_text = [
                cc
                for cc in self.meta.columns.find(column.es_index, column.es_column)
                if cc.es_type == "text"
            ]
            if is_es_text:
                # text IS A MULTIVALUE STRING THAT CAN ONLY BE FILTERED
                result = self.es_cluster.post(
                    "/" + es_index + "/_search",
                    data={"aggs": {"count": {"filter": {"match_all": {}}}}, "size": 0},
                )
                count = result.hits.total
                cardinality = max(1001, count)
                multi = 1001
            elif column.es_column == "_id":
                result = self.es_cluster.post(
                    "/" + es_index + "/_search",
                    data={"query": {"match_all": {}}, "size": 0},
                )
                count = cardinality = result.hits.total
                multi = 1
            elif column.es_type == EXISTS:
                result = self.es_cluster.post(
                    "/" + es_index + "/_search",
                    data={
                        "query": {"bool": {"must_not": {"missing": column.es_column}}},
                        "size": 0,
                    },
                )
                count = result.hits.total
                cardinality = 1
                multi = 1
            elif column.es_type == BOOLEAN:
                result = self.es_cluster.post(
                    "/" + es_index + "/_search",
                    data={"aggs": {"count": _counting_query(column)}, "size": 0},
                )
                count = result.hits.total
                cardinality = 2

                DEBUG and Log.note(
                    "{{table}}.{{field}} has {{num}} parts",
                    table=column.es_index,
                    field=column.es_column,
                    num=cardinality,
                )
                self.meta.columns.update({
                    "set": {
                        "count": count,
                        "cardinality": cardinality,
                        "partitions": [False, True],
                        "multi": 1,
                        "last_updated": now,
                    },
                    "clear": ["partitions"],
                    "where": {"eq": {
                        "es_index": column.es_index,
                        "es_column": column.es_column,
                    }},
                })
                return
            elif "_covered." in column.es_column or "_uncovered." in column.es_column:
                # DO NOT EVEN LOOK AT THESE COLUMNS
                self.meta.columns.update({
                    "set": {
                        "count": 1000 * 1000,
                        "cardinality": 10000,
                        "multi": 10000,
                        "last_updated": now,
                    },
                    "clear": ["partitions"],
                    "where": {"eq": {
                        "es_index": column.es_index,
                        "es_column": column.es_column,
                    }},
                })
                return
            else:
                es_query = {
                    "aggs": {
                        "count": _counting_query(column),
                        "_filter": {
                            "aggs": {"multi": {"max": {
                                "script": "doc["
                                + quote(column.es_column)
                                + "].values.size()"
                            }}},
                            "filter": {"bool": {"should": [
                                {"range": {"etl.timestamp.~n~": {"gte": (
                                    Date.today() - WEEK
                                )}}},
                                {"bool": {"must_not": {"exists": {
                                    "field": "etl.timestamp.~n~"
                                }}}},
                            ]}},
                        },
                    },
                    "size": 0,
                }

                result = self.es_cluster.post(
                    "/" + es_index + "/_search", data=es_query
                )
                agg_results = result.aggregations
                count = result.hits.total
                cardinality = coalesce(
                    agg_results.count.value,
                    agg_results.count._nested.value,
                    agg_results.count.doc_count,
                )
                multi = int(coalesce(agg_results._filter.multi.value, 1))
                if cardinality == None:
                    Log.error("logic error")

            query = Data(size=0)

            if column.es_column == "_id":
                self.meta.columns.update({
                    "set": {
                        "count": cardinality,
                        "cardinality": cardinality,
                        "multi": 1,
                        "last_updated": now,
                    },
                    "clear": ["partitions"],
                    "where": {"eq": {
                        "es_index": column.es_index,
                        "es_column": column.es_column,
                    }},
                })
                return
            elif (
                cardinality > 1000
                or (count >= 30 and cardinality == count)
                or (count >= 1000 and cardinality / count > 0.99)
            ):
                DEBUG and Log.note(
                    "{{table}}.{{field}} has {{num}} parts",
                    table=column.es_index,
                    field=column.es_column,
                    num=cardinality,
                )
                self.meta.columns.update({
                    "set": {
                        "count": count,
                        "cardinality": cardinality,
                        "multi": multi,
                        "last_updated": now,
                    },
                    "clear": ["partitions"],
                    "where": {"eq": {
                        "es_index": column.es_index,
                        "es_column": column.es_column,
                    }},
                })
                return
            elif column.es_type in elasticsearch.ES_NUMERIC_TYPES and cardinality > 30:
                DEBUG and Log.note(
                    "{{table}}.{{field}} has {{num}} parts",
                    table=column.es_index,
                    field=column.es_column,
                    num=cardinality,
                )
                self.meta.columns.update({
                    "set": {
                        "count": count,
                        "cardinality": cardinality,
                        "multi": multi,
                        "last_updated": now,
                    },
                    "clear": ["partitions"],
                    "where": {"eq": {
                        "es_index": column.es_index,
                        "es_column": column.es_column,
                    }},
                })
                return
            elif len(column.nested_path) != 1:
                query.aggs["_"] = {
                    "nested": {"path": column.nested_path[0]},
                    "aggs": {"_nested": {"terms": {"field": column.es_column}}},
                }
            elif cardinality == 0:  # WHEN DOES THIS HAPPEN?
                query.aggs["_"] = {"terms": {"field": column.es_column}}
            else:
                query.aggs["_"] = {"terms": {
                    "field": column.es_column,
                    "size": cardinality,
                }}

            result = self.es_cluster.post("/" + es_index + "/_search", data=query)

            aggs = result.aggregations._
            if aggs._nested:
                parts = jx.sort(aggs._nested.buckets.key)
            else:
                parts = jx.sort(aggs.buckets.key)

            DEBUG and Log.note(
                "update metadata for {{column.es_index}}.{{column.es_column}}"
                " (id={{id}}) card={{card}} at {{time}}",
                id=id(column),
                column=column,
                card=cardinality,
                time=now,
            )
            self.meta.columns.update({
                "set": {
                    "count": count,
                    "cardinality": cardinality,
                    "multi": multi,
                    "partitions": parts,
                    "last_updated": now,
                },
                "where": {"eq": {
                    "es_index": column.es_index,
                    "es_column": column.es_column,
                }},
            })
            META_COLUMNS_DESC.last_updated = now
        except Exception as e:
            # CAN NOT IMPORT: THE TEST MODULES SETS UP LOGGING
            # from tests.test_jx import TEST_TABLE
            e = Except.wrap(e)
            TEST_TABLE = "testdata"
            is_missing_index = any(
                w in e for w in ["IndexMissingException", "index_not_found_exception"]
            )
            is_test_table = column.es_index.startswith((TEST_TABLE_PREFIX, TEST_TABLE))

            meta_columns = self.meta.columns
            if is_missing_index:
                # WE EXPECT TEST TABLES TO DISAPPEAR
                if not is_test_table:
                    Log.warning("Missing index {{col.es_index}}", col=column)
                self.meta.columns.clear(column.es_index, after=now)
                self.index_does_not_exist.add(column.es_index)
                self.meta.columns.delete_from_es(column.es_index, after=now)
            elif "No field found for" in e:
                self.meta.columns.clear(column.es_index, column.es_column, after=now)
                Log.warning(
                    "Could not get column {{col.es_index}}.{{col.es_column}} info",
                    col=column,
                    cause=e,
                )
            else:
                self.meta.columns.update({
                    "set": {"last_updated": now},
                    "clear": ["count", "cardinality", "multi", "partitions"],
                    "where": {"eq": {
                        "es_index": column.es_index,
                        "es_column": column.es_column,
                    }},
                })
                Log.warning(
                    "Could not get {{col.es_index}}.{{col.es_column}} info",
                    col=column,
                    cause=e,
                )

    def monitor(self, please_stop):
        please_stop.then(lambda: self.todo.add(THREAD_STOP))
        while not please_stop:
            try:
                if not self.todo:
                    self.todo_priority = False
                    # LOOK FOR OLD COLUMNS WE CAN RE-SCAN
                    now = Date.now()
                    last_good_update = now - Duration(MAX_COLUMN_METADATA_AGE)

                    old_columns = [
                        c
                        for c in self.meta.columns
                        if (c.last_updated < last_good_update)
                        and c.es_index != META_COLUMNS_NAME
                    ]

                    if DEBUG:
                        if old_columns:
                            if len(old_columns) > 5:
                                Log.note("{{num}} old columns", num=len(old_columns))
                            else:
                                Log.note(
                                    "Old columns {{names|json}} last updated"
                                    " {{dates|json}}",
                                    names=to_data(old_columns).es_column,
                                    dates=[
                                        Date(t).format()
                                        for t in to_data(old_columns).last_updated
                                    ],
                                )
                        else:
                            Log.note("no more metatdata to update")

                    with Timer(
                        "Review {{num}} old columns",
                        param={"num": len(old_columns)},
                        verbose=DEBUG,
                    ):
                        for g, index_columns in jx.groupby(old_columns, "es_index"):
                            if self.todo_priority:
                                # WE GOT OTHER WORK TO DO
                                break
                            if g.es_index.startswith("testing_018"):
                                pass
                            try:
                                # TRIGGER COLUMN UNIFICATION BEFORE WE DO ANALYSIS
                                self.get_columns(g.es_index)
                            except Exception as cause:
                                if TABLE_DOES_NOT_EXIST in cause:
                                    DEBUG and Log.note(
                                        "removing {{index}} from metadata",
                                        index=g.es_index,
                                    )
                                    self.meta.columns.clear(g.es_index, after=now)
                                    self.meta.columns.delete_from_es(
                                        g.es_index, after=now
                                    )
                                    continue
                                Log.warning(
                                    "problem getting column info on {{table}}",
                                    table=g.es_index,
                                    cause=cause,
                                )

                            self.todo.extend(
                                (c, max(last_good_update, c.last_updated))
                                for c in index_columns
                            )

                        META_COLUMNS_DESC.last_updated = now

                work_item = self.todo.pop(Till(seconds=(10 * MINUTE).seconds))
                if work_item:
                    if work_item is THREAD_STOP:
                        continue
                    column, after = work_item

                    now = Date.now()
                    with Timer(
                        "review {{table}}.{{column}}",
                        param={"table": column.es_index, "column": column.es_column},
                        verbose=DEBUG,
                    ):
                        all_tables = [
                            n
                            for p in self.es_cluster.get_aliases(after=after)
                            for n in (p.index, p.alias)
                        ]
                        if column.es_index not in all_tables:
                            DEBUG and Log.note(
                                "{{column.es_column}} of {{column.es_index}} does not"
                                " exist",
                                column=column,
                            )
                            self.meta.columns.clear(column.es_index, after=now)
                            continue
                        if column.jx_type == EXISTS:
                            pass  # WE MUST PROBE ES TO SEE IF STILL EXISTS
                        elif column.jx_type in STRUCT:
                            if (
                                column.es_type == "nested"
                                or last(split_field(column.es_column)) == NESTED_TYPE
                            ) and (column.multi == None or column.multi < 2):
                                column.multi = 1001
                                Log.warning("fixing multi on nested problem")

                            column.last_updated = now
                            continue
                        elif column.cardinality is None:
                            pass  # NO CARDINALITY MEANS WE MUST GET UPDATE IT
                        elif after and column.last_updated < after:
                            pass  # COLUMN IS TOO OLD
                        elif column.last_updated < now - TOO_OLD:
                            pass  # COLUMN IS WAY TOO OLD
                        else:
                            # DO NOT UPDATE FRESH COLUMN METADATA
                            DEBUG and Log.note(
                                "{{column.es_column}} is still fresh ({{ago}} ago)",
                                column=column,
                                ago=(now - Date(column.last_updated)),
                            )
                            continue

                        try:
                            self._update_cardinality(column)
                            (
                                DEBUG
                                and not column.es_index.startswith(TEST_TABLE_PREFIX)
                                and Log.note("updated {{column.name}}", column=column)
                            )
                        except Exception as cause:
                            if '"status":404' in cause:
                                self.meta.columns.clear(
                                    column.es_index, column.es_column, after=now
                                )
                            else:
                                Log.warning(
                                    "problem getting cardinality for {{column.name}}",
                                    column=column,
                                    cause=cause,
                                )
                    META_COLUMNS_DESC.last_updated = now
            except Exception as cause:
                Log.warning("problem in cardinality monitor", cause=cause)

    def not_monitor(self, please_stop):
        Log.alert("metadata scan has been disabled")
        please_stop.then(lambda: self.todo.add(THREAD_STOP))
        while not please_stop:
            pair = self.todo.pop()
            if pair is THREAD_STOP:
                break
            column, after = pair

            with Timer(
                "Update {{col.es_index}}.{{col.es_column}}",
                param={"col": column},
                verbose=DEBUG,
                too_long=0.05,
            ):
                if column.jx_type in INTERNAL:
                    # DEBUG and Log.note("{{column.es_column}} is a struct", column=column)
                    continue
                elif after and column.last_updated > after:
                    continue  # COLUMN IS STILL YOUNG
                elif column.cardinality == 0:
                    # DO NOT UPDATE DELETED COLUMNS
                    DEBUG and Log.note(
                        "{{column.es_column}} does not exist, do not update",
                        column=column,
                    )
                    continue
                elif column.last_updated > Date.now() - TOO_OLD:
                    # DO NOT UPDATE FRESH COLUMN METADATA
                    DEBUG and Log.note(
                        "{{column.es_column}} is still fresh ({{ago}} ago)",
                        column=column,
                        ago=(Date.now() - Date(column.last_updated)).seconds,
                    )
                    continue
                elif untype_path(column.name) in KNOWN_MULTITYPES:
                    try:
                        self._update_cardinality(column)
                    except Exception as e:
                        Log.warning(
                            "problem getting cardinality for {{column.name}}",
                            column=column,
                            cause=e,
                        )
                    continue

                # SET THE REST TO UNKNOWN
                self.meta.columns.update({
                    "set": {"last_updated": Date.now()},
                    "clear": ["count", "cardinality", "multi", "partitions"],
                    "where": {"eq": {
                        "es_index": column.es_index,
                        "es_column": column.es_column,
                    }},
                })

    def get_table(self, name):
        if name == META_COLUMNS_NAME:
            pass
        with self.meta.tables.locker:
            return first(t for t in self.meta.tables.data if t.name == name)

    def get_snowflake(self, fact_table_name):
        return Snowflake(fact_table_name, self)

    def get_schema(self, name):
        if name == META_COLUMNS_NAME:
            return self.meta.columns.schema
        if name == META_TABLES_NAME:
            return self.meta.tables.schema
        root, rest = tail_field(name)
        return self.get_snowflake(root).get_schema(rest)


EXPECTING_SNOWFLAKE = "Expecting snowflake {{name|quote}} to exist"


class Snowflake(object):
    """
    REPRESENT ONE ALIAS, AND ITS NESTED ARRAYS
    """

    def __init__(self, name, namespace):
        self.name = name
        self.namespace = namespace
        if name not in self.namespace.alias_to_query_paths:
            Log.error(EXPECTING_SNOWFLAKE, name=name)

    def get_schema(self, query_path):
        return Schema(query_path, self)

    @property
    def query_paths(self):
        """
        RETURN A LIST OF ALL NESTED COLUMNS
        """
        output = self.namespace.alias_to_query_paths.get(self.name)
        if output:
            return output
        Log.error("Can not find index {{index|quote}}", index=self.name)

    @property
    def sorted_query_paths(self):
        """
        RETURN A LIST OF ALL SCHEMA'S IN DEPTH-FIRST TOPOLOGICAL ORDER
        """
        return list(reversed(sorted(
            p[0] for p in self.namespace.alias_to_query_paths.get(self.name)
        )))

    @property
    def columns(self):
        """
        RETURN ALL COLUMNS FROM ORIGIN OF FACT TABLE
        """
        return self.namespace.get_columns(literal_field(self.name))


class Schema(jx_base.Schema):
    """
    REPRESENT JUST ONE TABLE IN A SNOWFLAKE
    """

    def __init__(self, query_path, snowflake):
        if not is_list(snowflake.query_paths[0]):
            Log.error(
                "Snowflake query paths should be a list of string tuples (well,"
                " technically, a list of lists of strings)"
            )
        self.snowflake = snowflake
        try:
            path = first(
                p for p in snowflake.query_paths if untype_path(p[0]) == query_path
            )
            if path:
                # WE DO NOT NEED TO LOOK INTO MULTI-VALUED FIELDS AS A TABLE
                self.multi = None
                self.query_path = path
            else:
                # LOOK INTO A SPECIFIC MULTI VALUED COLUMN
                try:
                    self.multi = first([
                        c
                        for c in self.snowflake.columns
                        if (
                            untype_path(c.name) == query_path
                            and (
                                c.multi > 1
                                or last(split_field(c.es_column))
                                == NESTED_TYPE  # THIS IS TO COMPENSATE FOR BAD c.multi
                            )
                        )
                    ])
                    if not self.multi:
                        Log.error("expecting a nested column")
                    self.query_path = (
                        [self.multi.name] + unwrap(listwrap(self.multi.nested_path))
                    )
                except Exception as e:
                    # PROBLEM WITH METADATA UPDATE
                    self.multi = None
                    self.query_path = (query_path, ".")

                    Log.warning(
                        "Problem getting query path {{path|quote}} in snowflake"
                        " {{sf|quote}}",
                        path=query_path,
                        sf=snowflake.name,
                        cause=e,
                    )

            if (
                not is_list(self.query_path)
                or self.query_path[len(self.query_path) - 1] != "."
            ):
                Log.error("error")

        except Exception as e:
            Log.error("logic error", cause=e)

    def leaves(self, column_name):
        """
        :param column_name:
        :return: ALL COLUMNS THAT START WITH column_name, NOT INCLUDING DEEPER NESTED COLUMNS
        """
        clean_name = untype_path(column_name)
        columns = self.columns

        if clean_name == ".":
            # ALL COLUMNS
            return set(
                c
                for c in columns
                if c.name != "_id" and c.jx_type not in INTERNAL and c.cardinality != 0
            )

        if clean_name != column_name:
            # column_name IS AN EXPLICT NAME, INCLUDING TYPE INFO
            for path in self.query_path:
                output = [
                    c
                    for c in columns
                    if (
                        (c.name != "_id" or column_name == "_id")
                        and startswith_field(relative_field(c.name, path), column_name)
                    )
                ]
                if output:
                    return set(output)
            return set()

        # TODO: HOW TO REFER TO FIELDS THAT MAY BE SHADOWED BY A RELATIVE NAME?
        query_path = self.query_path[0]
        for path in self.query_path:
            if untype_path(path) == clean_name:
                # ASKING FOR LEAVES OF A NESTED COLUMN
                output = [
                    c
                    for c in columns
                    if (
                        (c.name != "_id" or clean_name == "_id")
                        and c.cardinality != 0
                        and c.jx_type not in (OBJECT, EXISTS)
                        and path == c.nested_path[0]  # EVERYTHING AT THIS LEVEL
                    )
                ]
                return set(output)

            output = [
                c
                for c in columns
                if (
                    (c.name != "_id" or clean_name == "_id")
                    and c.cardinality != 0
                    and c.jx_type not in (OBJECT, EXISTS)
                    # and startswith_field(query_path, c.nested_path[0])  # NOT DEEPER THAN THE SCHEMA
                    and startswith_field(
                        untype_path(relative_field(c.name, path)), clean_name
                    )
                )
            ]
            if output:
                return set(output)
        return set()

    def values(self, column_name, exclude_type=STRUCT):
        """
        RETURN ALL COLUMNS THAT column_name REFERS TO
        """
        clean_name = untype_path(column_name)
        columns = self.columns

        if clean_name != column_name:
            # SPECIFIC FIELD REQUESTED
            output = []
            for path in self.query_path:
                full_path = concat_field(path, column_name)
                for c in columns:
                    if c.jx_type in exclude_type:
                        continue
                    if c.cardinality == 0:
                        continue
                    if c.name == full_path:
                        output.append(c)
                if output:
                    return output
            return []

        output = []
        for path in self.query_path:
            full_path = untype_path(concat_field(path, column_name))
            for c in columns:
                if c.jx_type in exclude_type:
                    continue
                if c.cardinality == 0:
                    continue
                if untype_path(c.name) == full_path:
                    output.append(c)
            if output:
                return output
        return []

    def __getitem__(self, column_name):
        return self.values(column_name)

    @property
    def name(self):
        return concat_field(self.snowflake.name, self.query_path[0])

    @property
    def columns(self):
        return self.snowflake.columns

    def map_to_es(self):
        """
        RETURN A MAP FROM THE NAMESPACE TO THE es_column NAME
        """
        output = {}
        for path in self.query_path:
            set_default(
                output,
                {
                    k: c.es_column
                    for c in self.columns
                    if c.jx_type not in INTERNAL
                    for rel_name in [relative_field(c.name, path)]
                    for k in [rel_name, untype_path(rel_name), unnest_path(rel_name)]
                },
            )
        return output


class Table(BaseTable):
    def __init__(self, full_name, container):
        BaseTable.__init__(self, full_name)
        self.container = container
        self.schema = container.namespace.get_schema(full_name)

    def missing(self):
        return FALSE


def _counting_query(c):
    if c.es_column == "_id":
        return {"filter": {"match_all": {}}}
    elif len(c.nested_path) != 1:
        return {
            "nested": {"path": c.nested_path[0]},  # FIRST ONE IS LONGEST
            "aggs": {"_nested": {"cardinality": {
                "field": c.es_column,
                "precision_threshold": 10
                if c.es_type in elasticsearch.ES_NUMERIC_TYPES
                else 100,
            }}},
        }
    else:
        return {"cardinality": {"field": c.es_column}}


python_type_to_es_type = {
    none_type: "undefined",
    NullType: "undefined",
    bool: "boolean",
    str: "string",
    text: "string",
    int: "integer",
    long: "integer",
    float: "double",
    Data: "object",
    dict: "object",
    set: "nested",
    list: "nested",
    FlatList: "nested",
    Date: "double",
    Decimal: "double",
    datetime: "double",
    date: "double",
}

_merge_es_type = {
    "undefined": {
        "undefined": "undefined",
        "boolean": "boolean",
        "integer": "integer",
        "long": "long",
        "float": "float",
        "double": "double",
        "number": "number",
        "string": "string",
        "object": "object",
        "nested": "nested",
    },
    "boolean": {
        "undefined": "boolean",
        "boolean": "boolean",
        "integer": "integer",
        "long": "long",
        "float": "float",
        "double": "double",
        "number": "number",
        "string": "string",
        "object": None,
        "nested": None,
    },
    "integer": {
        "undefined": "integer",
        "boolean": "integer",
        "integer": "integer",
        "long": "long",
        "float": "float",
        "double": "double",
        "number": "number",
        "string": "string",
        "object": None,
        "nested": None,
    },
    "long": {
        "undefined": "long",
        "boolean": "long",
        "integer": "long",
        "long": "long",
        "float": "double",
        "double": "double",
        "number": "number",
        "string": "string",
        "object": None,
        "nested": None,
    },
    "float": {
        "undefined": "float",
        "boolean": "float",
        "integer": "float",
        "long": "double",
        "float": "float",
        "double": "double",
        "number": "number",
        "string": "string",
        "object": None,
        "nested": None,
    },
    "double": {
        "undefined": "double",
        "boolean": "double",
        "integer": "double",
        "long": "double",
        "float": "double",
        "double": "double",
        "number": "number",
        "string": "string",
        "object": None,
        "nested": None,
    },
    "number": {
        "undefined": "number",
        "boolean": "number",
        "integer": "number",
        "long": "number",
        "float": "number",
        "double": "number",
        "number": "number",
        "string": "string",
        "object": None,
        "nested": None,
    },
    "string": {
        "undefined": "string",
        "boolean": "string",
        "integer": "string",
        "long": "string",
        "float": "string",
        "double": "string",
        "number": "string",
        "string": "string",
        "object": None,
        "nested": None,
    },
    "object": {
        "undefined": "object",
        "boolean": None,
        "integer": None,
        "long": None,
        "float": None,
        "double": None,
        "number": None,
        "string": None,
        "object": "object",
        "nested": "nested",
    },
    "nested": {
        "undefined": "nested",
        "boolean": None,
        "integer": None,
        "long": None,
        "float": None,
        "double": None,
        "number": None,
        "string": None,
        "object": "nested",
        "nested": "nested",
    },
}


OBJECTS = (OBJECT, EXISTS)
