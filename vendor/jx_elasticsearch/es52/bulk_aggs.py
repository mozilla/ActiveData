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

import gzip
from contextlib import closing

from jx_base.expressions import Variable, value2json
from jx_base.language import is_op
from jx_elasticsearch import post as es_post
from jx_elasticsearch.es52.aggs import build_es_query
from jx_elasticsearch.es52.format import format_list_from_groupby
from mo_dots import listwrap, unwrap, unwraplist, Null
from mo_files import TempFile, URL, mimetype
from mo_future import first
from mo_logs import Log
from mo_math.randoms import Random
from mo_threads import Till, Thread
from mo_times import Date, DAY
from mo_times.timer import Timer
from pyLibrary.aws.s3 import Connection

DEBUG = False
CHUNK_SIZE = 5000
URL_PREFIX = URL("http://activedata.allizom.org/results/")
S3_CONFIG = Null


def is_bulkaggsop(es, query):
    # ONLY ACCEPTING ONE DIMENSION AT THIS TIME
    if not S3_CONFIG:
        return False
    if not query.meta.big:
        return False
    if len(listwrap(query.groupby)) != 1:
        return False
    if not is_op(unwraplist(query.groupby).value, Variable):
        return False
    if query.format != "list":
        return False
    return True


def es_bulkaggsop(es, frum, query):
    query = query.copy()  # WE WILL MARK UP THIS QUERY
    schema = frum.schema
    query_path = schema.query_path[0]
    selects = listwrap(query.select)

    variable = unwraplist(query.groupby).value
    # FIND CARDINALITY

    cardinality_check = Timer(
        "Get cardinality for {{column}}", param={"column": variable.var}
    )
    with cardinality_check:
        columns = es.namespace.get_columns(
            first(query_path),
            column_name=variable.var,
            after=Date.now() - DAY,
            timeout=Till(seconds=30),
        )

        num_partitions = (first(columns).cardinality + CHUNK_SIZE) // CHUNK_SIZE
        acc, decoders, es_query = build_es_query(selects, query_path, schema, query)
        filename = Random.base64(32)+ ".json.gz"
        if len(columns) != 1:
            Log.error("too many columns to bulk groupby")

        Thread.run(
            "extract to " + filename,
            extractor,
            filename,
            es_query,
            num_partitions,
            es,
            acc,
            query,
            decoders,
            selects,
        )

    output = {
        "meta": {
            "format": "bulk",
            "url": URL_PREFIX / filename ,
            "timing": {"cardinality_check": cardinality_check.duration},
            "es_query": es_query,
            "num_partitions": num_partitions,
        }
    }
    return output


def extractor(
    filename, es_query, num_partitions, es, acc, query, decoders, selects, please_stop
):
    # FIND THE include POINT
    curr = es_query
    while True:
        if curr._filter:
            curr = curr._filter.aggs
        elif curr._match:
            break
        else:
            Log.error("can not handle")
    missing = curr._missing
    curr._missing = None
    exists = curr._match

    exists.size = CHUNK_SIZE
    exists.include.num_partitions = num_partitions

    with TempFile() as temp_file:
        with open(temp_file._filename, "wb") as output_file:
            with closing(gzip.GzipFile(fileobj=output_file, mode='wb')) as archive:
                archive.write(b"[\n")
                comma = b""
                for i in range(0, num_partitions):
                    if please_stop:
                        return
                    is_last = i == num_partitions - 1
                    if is_last:
                        # INCLUDE MISSING AT LAST
                        curr._missing = missing
                    exists.include.partition = i

                    result = es_post(es, es_query, CHUNK_SIZE)
                    aggs = unwrap(result.aggregations)
                    data = format_list_from_groupby(
                        aggs, acc, query, decoders, selects
                    ).data

                    for r in data:
                        archive.write(comma)
                        comma = b",\n"
                        archive.write(value2json(r).encode("utf8"))
                archive.write(b"\n]\n")

        # PUSH FILE TO S3
        try:
            connection = Connection(S3_CONFIG).connection
            bucket = connection.get_bucket(S3_CONFIG.bucket, validate=False)
            storage = bucket.new_key(filename)
            storage.set_contents_from_filename(temp_file.abspath, headers={"Content-Type": mimetype.ZIP})
        except Exception as e:
            Log.error(
                "Problem connecting to {{bucket}}", bucket=S3_CONFIG.bucket, cause=e
            )

