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

from copy import deepcopy

import mo_math
from jx_base.expressions import Variable, TRUE
from jx_base.language import is_op
from jx_base.query import _normalize_group
from jx_elasticsearch.es52.agg_format import format_list_from_groupby, format_table_from_groupby
from jx_elasticsearch.es52.agg_op import build_es_query
from mo_dots import listwrap, unwrap, Null, wrap, coalesce
from mo_files import TempFile, URL, mimetype
from mo_future import first
from mo_json import value2json
from mo_logs import Log, Except
from mo_math.randoms import Random
from mo_testing.fuzzytestcase import assertAlmostEqual
from mo_threads import Thread
from mo_times import Timer, Date
from pyLibrary.aws.s3 import Connection

DEBUG = False
MAX_CHUNK_SIZE = 5000
MAX_PARTITIONS = 200
URL_PREFIX = URL("https://active-data-query-results.s3-us-west-2.amazonaws.com")
S3_CONFIG = Null


def is_bulk_agg(esq, query):
    # ONLY ACCEPTING ONE DIMENSION AT THIS TIME
    if not S3_CONFIG:
        return False
    if query.destination not in {"s3", "url"}:
        return False
    if query.format not in {"list", "table"}:
        return False
    if len(listwrap(query.groupby)) != 1:
        return False

    gb = first(_normalize_group(first(listwrap(query.groupby)), 0, query.limit))
    if not is_op(gb.value, Variable):
        return False
    return True


def es_bulkaggsop(esq, frum, query):
    query = query.copy()  # WE WILL MARK UP THIS QUERY

    chunk_size = min(coalesce(query.chunk_size, MAX_CHUNK_SIZE), MAX_CHUNK_SIZE)
    schema = frum.schema
    query_path = first(schema.query_path)
    selects = listwrap(query.select)

    variable = first(query.groupby).value
    # FIND CARDINALITY

    cardinality_check = Timer(
        "Get cardinality for {{column}}", param={"column": variable.var}
    )

    with cardinality_check:
        columns = schema.leaves(variable.var)
        if len(columns) != 1:
            Log.error(
                "too many columns to bulk groupby:\n{{columns|json}}", columns=columns
            )
        column = first(columns)

        if query.where is TRUE:
            cardinality = column.cardinality
            if cardinality == None:
                esq.namespace._update_cardinality(column)
                cardinality = column.cardinality
        else:
            cardinality = esq.query(
                {
                    "select": {
                        "name": "card",
                        "value": variable,
                        "aggregate": "cardinality",
                    },
                    "from": frum.name,
                    "where": query.where,
                    "format": "cube",
                }
            ).card

        num_partitions = (cardinality + chunk_size - 1) // chunk_size

        if num_partitions > MAX_PARTITIONS:
            Log.error("Requesting more than {{num}} partitions", num=num_partitions)

        acc, decoders, es_query = build_es_query(selects, query_path, schema, query)
        guid = Random.base64(32, extra="-_")
        abs_limit = mo_math.MIN((query.limit, first(query.groupby).domain.limit))
        formatter = formatters[query.format](abs_limit)

        Thread.run(
            "extract to " + guid + ".json",
            extractor,
            guid,
            num_partitions,
            esq,
            query,
            selects,
            query_path,
            schema,
            chunk_size,
            cardinality,
            abs_limit,
            formatter,
            parent_thread=Null,
        )

    output = wrap(
        {
            "url": URL_PREFIX / (guid + ".json"),
            "status": URL_PREFIX / (guid + ".status.json"),
            "meta": {
                "format": query.format,
                "timing": {"cardinality_check": cardinality_check.duration},
                "es_query": es_query,
                "num_partitions": num_partitions,
                "cardinality": cardinality,
            },
        }
    )
    return output


def extractor(
    guid,
    num_partitions,
    esq,
    query,
    selects,
    query_path,
    schema,
    chunk_size,
    cardinality,
    abs_limit,
    formatter,
    please_stop,
):
    total = 0
    # WE MESS WITH THE QUERY LIMITS FOR CHUNKING
    query.limit = first(query.groupby).domain.limit = chunk_size * 2
    start_time = Date.now()

    try:
        write_status(
            guid,
            {
                "status": "starting",
                "chunks": num_partitions,
                "rows": min(abs_limit, cardinality),
                "start_time": start_time,
                "timestamp": Date.now(),
            },
        )

        with TempFile() as temp_file:
            with open(temp_file.abspath, "wb") as output:
                for i in range(0, num_partitions):
                    if please_stop:
                        Log.error("request to shutdown!")
                    is_last = i == num_partitions - 1
                    first(query.groupby).allowNulls = is_last
                    acc, decoders, es_query = build_es_query(
                        selects, query_path, schema, query
                    )
                    # REACH INTO THE QUERY TO SET THE partitions
                    terms = es_query.aggs._filter.aggs._match.terms
                    terms.include.partition = i
                    terms.include.num_partitions = num_partitions

                    result = esq.es.search(deepcopy(es_query), query.limit)
                    aggs = unwrap(result.aggregations)

                    formatter.add(aggs, acc, query, decoders, selects)
                    for b in formatter.bytes():
                        if b is DONE:
                            break
                        output.write(b)
                    else:
                        write_status(
                            guid,
                            {
                                "status": "working",
                                "chunk": i,
                                "chunks": num_partitions,
                                "row": total,
                                "rows": min(abs_limit, cardinality),
                                "start_time": start_time,
                                "timestamp": Date.now(),
                            },
                        )
                        continue
                    break
                for b in formatter.footer():
                    output.write(b)

            upload(guid + ".json", temp_file)
        write_status(
            guid,
            {
                "ok": True,
                "status": "done",
                "chunks": num_partitions,
                "rows": min(abs_limit, cardinality),
                "start_time": start_time,
                "end_time": Date.now(),
                "timestamp": Date.now(),
            },
        )
    except Exception as e:
        e = Except.wrap(e)
        write_status(
            guid,
            {
                "ok": False,
                "status": "error",
                "error": e,
                "start_time": start_time,
                "end_time": Date.now(),
                "timestamp": Date.now(),
            },
        )
        Log.warning("Could not extract", cause=e)


def upload(filename, temp_file):
    with Timer("upload file to S3 {{file}}", param={"file": filename}):
        try:
            connection = Connection(S3_CONFIG).connection
            bucket = connection.get_bucket(S3_CONFIG.bucket, validate=False)
            storage = bucket.new_key(filename)
            storage.set_contents_from_filename(
                temp_file.abspath, headers={"Content-Type": mimetype.JSON}
            )
            if S3_CONFIG.public:
                storage.set_acl("public-read")

        except Exception as e:
            Log.error(
                "Problem connecting to {{bucket}}", bucket=S3_CONFIG.bucket, cause=e
            )


def write_status(guid, status):
    try:
        filename = guid + ".status.json"
        with Timer("upload status to S3 {{file}}", param={"file": filename}, verbose=DEBUG):
            try:
                connection = Connection(S3_CONFIG).connection
                bucket = connection.get_bucket(S3_CONFIG.bucket, validate=False)
                storage = bucket.new_key(filename)
                storage.set_contents_from_string(
                    value2json(status), headers={"Content-Type": mimetype.JSON}
                )
                if S3_CONFIG.public:
                    storage.set_acl("public-read")

            except Exception as e:
                Log.error(
                    "Problem connecting to {{bucket}}",
                    bucket=S3_CONFIG.bucket,
                    cause=e
                )
    except Exception as e:
        Log.warning("problem setting status", cause=e)


DONE = object()


class ListFormatter(object):
    def __init__(self, abs_limit):
        self.header = b"{\"meta\":{\"format\":\"list\"},\"data\":[\n"
        self.count = 0
        self.abs_limit = abs_limit
        self.result = None

    def add(self, aggs, acc, query, decoders, selects):
        self.result = format_list_from_groupby(aggs, acc, query, decoders, selects)

    def bytes(self):
        yield self.header
        self.header = b",\n"

        comma = b""
        for r in self.result.data:
            yield comma
            comma = b",\n"
            yield value2json(r).encode('utf8')
            self.count += 1
            if self.count >= self.abs_limit:
                yield DONE

    def footer(self):
        yield b"\n]}"


class TableFormatter(object):
    def __init__(self, abs_limit):
        self.header = None

        self.count = 0
        self.abs_limit = abs_limit
        self.result = None
        self.pre = ""

    def add(self, aggs, acc, query, decoders, selects):
        self.result = format_table_from_groupby(aggs, acc, query, decoders, selects)
        # CONFIRM HEADER MATCH
        if self.header:
            assertAlmostEqual(self.header, self.result.header)
        else:
            self.header = self.result.header

    def bytes(self):
        if self.pre:
            yield self.pre
        else:
            self.pre = b",\n"
            yield b"{\"meta\":{\"format\":\"table\"},\"header\":"
            yield value2json(self.header).encode('utf8')
            yield b",\n\"data\":[\n"

        comma = b""
        for r in self.result.data:
            yield comma
            comma = b",\n"
            yield value2json(r).encode('utf8')
            self.count += 1
            if self.count >= self.abs_limit:
                yield DONE

    def footer(self):
        yield b"\n]}"


formatters = {
    "list": ListFormatter,
    "table": TableFormatter
}
