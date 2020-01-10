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

from jx_base.query import DEFAULT_LIMIT
from jx_elasticsearch.es52.expressions import split_expression_by_path, ES52
from jx_elasticsearch.es52.set_format import set_formatters
from jx_elasticsearch.es52.set_op import get_selects, es_query_proto
from jx_elasticsearch.es52.util import jx_sort_to_es_sort
from mo_dots import Null, coalesce
from mo_files import URL, mimetype
from mo_logs import Log
from mo_times import Timer
from pyLibrary.aws.s3 import Connection

DEBUG = False
URL_PREFIX = URL("https://active-data-query-results.s3-us-west-2.amazonaws.com")
S3_CONFIG = Null
MAX_CHUNK_SIZE = 5000


def is_bulksetop(esq, query):
    # ONLY ACCEPTING ONE DIMENSION AT THIS TIME
    if not S3_CONFIG:
        return False
    if query.destination not in {"s3", "url"}:
        return False
    if query.format not in {"list"}:
        return False
    if query.groupby or query.edges:
        return False
    return True


def es_bulksetop(esq, frum, query):

    schema = query.frum.schema
    query_path = schema.query_path[0]
    new_select, split_select = get_selects(query)
    split_wheres = split_expression_by_path(query.where, schema, lang=ES52)
    es_query = es_query_proto(query_path, split_select, split_wheres, schema)
    es_query.size = coalesce(query.limit, DEFAULT_LIMIT)
    es_query.sort = jx_sort_to_es_sort(query.sort, schema)

    formatter, groupby_formatter, mime_type = set_formatters[query.format]

    with Timer("call to ES", silent=DEBUG) as call_timer:
        data = esq.es.search(es_query)

    T = data.hits.hits

    try:

        with Timer("formatter", silent=True):
            output = formatter(T, new_select, query)
        output.meta.timing.es = call_timer.duration
        output.meta.content_type = mime_type
        output.meta.es_query = es_query
        return output
    except Exception as e:
        Log.error("problem formatting", e)


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
