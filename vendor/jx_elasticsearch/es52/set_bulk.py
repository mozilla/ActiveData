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

from jx_elasticsearch.es52 import agg_bulk
from jx_elasticsearch.es52.agg_bulk import write_status, upload, URL_PREFIX
from jx_elasticsearch.es52.expressions import ES52
from jx_elasticsearch.es52.expressions._utils import split_expression_by_path_for_setop
from jx_elasticsearch.es52.set_format import doc_formatter, row_formatter, format_table_header
from jx_elasticsearch.es52.set_op import get_selects, es_query_proto
from jx_elasticsearch.es52.util import jx_sort_to_es_sort
from mo_dots import to_data, Null
from mo_files import TempFile
from mo_json import value2json
from mo_logs import Log, Except
from mo_math import MIN
from mo_math.randoms import Random
from mo_threads import Thread
from mo_times import Date, Timer

DEBUG = True
MAX_CHUNK_SIZE = 2000
MAX_DOCUMENTS = 10 * 1000 * 1000


def is_bulk_set(esq, query):
    # ONLY ACCEPTING ONE DIMENSION AT THIS TIME
    if not agg_bulk.S3_CONFIG:
        return False
    if query.destination not in {"s3", "url"}:
        return False
    if query.format not in {"list", "table"}:
        return False
    if query.groupby or query.edges:
        return False
    return True


def es_bulksetop(esq, frum, query):
    abs_limit = MIN([query.limit, MAX_DOCUMENTS])
    guid = Random.base64(32, extra="-_")

    schema = query.frum.schema
    new_select, split_select = get_selects(query)
    op, split_wheres = split_expression_by_path_for_setop(query.where, schema)
    es_query = es_query_proto(split_select, op, split_wheres, schema)
    es_query.size = MIN([query.chunk_size, MAX_CHUNK_SIZE])
    es_query.sort = jx_sort_to_es_sort(query.sort, schema)
    if not es_query.sort:
        es_query.sort = ["_doc"]

    formatter = formatters[query.format](abs_limit, new_select, query)

    Thread.run(
        "Download " + guid,
        extractor,
        guid,
        abs_limit,
        esq,
        es_query,
        formatter,
        parent_thread=Null,
    ).release()

    output = to_data(
        {
            "url": URL_PREFIX / (guid + ".json"),
            "status": URL_PREFIX / (guid + ".status.json"),
            "meta": {"format": query.format, "es_query": es_query, "limit": abs_limit},
        }
    )
    return output


def extractor(guid, abs_limit, esq, es_query, formatter, please_stop):
    start_time = Date.now()
    total = 0
    write_status(
        guid,
        {
            "status": "starting",
            "limit": abs_limit,
            "start_time": start_time,
            "timestamp": Date.now(),
        },
    )

    try:
        with TempFile() as temp_file:
            with open(temp_file.abspath, "wb") as output:
                result = esq.es.search(es_query, scroll="5m")

                while not please_stop:
                    scroll_id = result._scroll_id
                    hits = result.hits.hits
                    chunk_limit = abs_limit - total
                    hits = hits[:chunk_limit]
                    if len(hits) == 0:
                        break
                    formatter.add(hits)
                    for b in formatter.bytes():
                        if b is DONE:
                            break
                        output.write(b)
                    else:
                        total += len(hits)
                        DEBUG and Log.note(
                            "{{num}} of {{total}} downloaded",
                            num=total,
                            total=result.hits.total,
                        )
                        write_status(
                            guid,
                            {
                                "status": "working",
                                "row": total,
                                "rows": result.hits.total,
                                "start_time": start_time,
                                "timestamp": Date.now(),
                            },
                        )
                        with Timer("get more", verbose=DEBUG):
                            result = esq.es.scroll(scroll_id)
                        continue
                    break
                if please_stop:
                    Log.error("Bulk download stopped for shutdown")
                for b in formatter.footer():
                    output.write(b)

            write_status(
                guid,
                {
                    "status": "uploading to s3",
                    "rows": total,
                    "start_time": start_time,
                    "timestamp": Date.now(),
                },
            )
            upload(guid + ".json", temp_file)
        if please_stop:
            Log.error("shutdown requested, did not complete download")
        DEBUG and Log.note("Done. {{total}} uploaded", total=total)
        write_status(
            guid,
            {
                "ok": True,
                "status": "done",
                "rows": total,
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


class ListFormatter(object):
    def __init__(self, abs_limit, select, query):
        self.header = b"{\"meta\":{\"format\":\"list\"},\"data\":[\n"
        self.count = 0
        self.abs_limit = abs_limit
        self.formatter = doc_formatter(select, query)
        self.rows = None

    def add(self, rows):
        self.rows = rows

    def bytes(self):
        yield self.header
        self.header = b",\n"

        comma = b""
        for r in self.rows:
            yield comma
            comma = b",\n"
            yield value2json(self.formatter(r)).encode('utf8')
            self.count += 1
            if self.count >= self.abs_limit:
                yield DONE

    def footer(self):
        yield b"\n]}"


DONE = object()


class TableFormatter(object):
    def __init__(self, abs_limit, select, query):
        self.count = 0
        self.abs_limit = abs_limit
        self.formatter = row_formatter(select)
        self.rows = None
        self.pre = (
            b"{\"meta\":{\"format\":\"table\"},\"header\":" +
            value2json(format_table_header(select, query)).encode('utf8') +
            b",\n\"data\":[\n"
        )

    def add(self, rows):
        self.rows = rows

    def bytes(self):
        yield self.pre
        self.pre = b",\n"

        comma = b""
        for r in self.rows:
            yield comma
            comma = b",\n"
            yield value2json(self.formatter(r)).encode('utf8')
            self.count += 1
            if self.count >= self.abs_limit:
                yield DONE

    def footer(self):
        yield b"\n]}"


formatters = {
    "list": ListFormatter,
    "table": TableFormatter
}
