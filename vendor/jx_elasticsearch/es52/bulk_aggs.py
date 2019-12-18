# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from copy import deepcopy

import mo_math
from jx_base.expressions import Variable, value2json, TRUE
from jx_base.language import is_op
from jx_elasticsearch import post as es_post
from jx_elasticsearch.es52.aggs import build_es_query
from jx_elasticsearch.es52.format import format_list_from_groupby
from mo_dots import listwrap, unwrap, Null, wrap, coalesce, unwraplist
from mo_files import TempFile, URL, mimetype
from mo_future import first, is_text
from mo_logs import Log, Except
from mo_math.randoms import Random
from mo_threads import Thread
from mo_times import Timer, Date
from pyLibrary.aws.s3 import Connection

DEBUG = False
MAX_CHUNK_SIZE = 5000
MAX_PARTITIONS = 200
URL_PREFIX = URL("https://active-data-query-results.s3-us-west-2.amazonaws.com")
S3_CONFIG = Null


def is_bulkaggsop(esq, query):
    # ONLY ACCEPTING ONE DIMENSION AT THIS TIME
    if not S3_CONFIG:
        return False
    if query.destination != "s3":
        return False
    if query.format != "list":
        return False
    if len(listwrap(query.groupby)) != 1:
        return False
    if is_text(unwraplist(query.groupby)):
        return True
    if not is_op(first(query.groupby).value, Variable):
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
            parent_thread=Null,
        )

    output = wrap(
        {
            "url": URL_PREFIX / (guid + ".json"),
            "status": URL_PREFIX / (guid + ".status.json"),
            "meta": {
                "format": "list",
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
    please_stop,
):
    total = 0
    abs_limit = mo_math.MIN((query.limit, first(query.groupby.domain).limit))
    # WE MESS WITH THE QUERY LIMITS FOR CHUNKING
    query.limit = first(query.groupby.domain).limit = chunk_size * 2
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
                output.write(b"[\n")
                comma = b""
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

                    result = es_post(esq.es, deepcopy(es_query), query.limit)
                    aggs = unwrap(result.aggregations)
                    data = format_list_from_groupby(
                        aggs, acc, query, decoders, selects
                    ).data

                    for r in data:
                        output.write(comma)
                        comma = b",\n"
                        output.write(value2json(r).encode("utf8"))
                        total += 1
                        if total >= abs_limit:
                            break
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
                output.write(b"\n]\n")

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
        with Timer("upload status to S3 {{file}}", param={"file": filename}):
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
                    "Problem connecting to {{bucket}}", bucket=S3_CONFIG.bucket, cause=e
                )
    except Exception as e:
        Log.warning("problem setting status", cause=e)
