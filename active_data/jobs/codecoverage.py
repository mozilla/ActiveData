# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from jx_python import jx
from mo_dots import coalesce, wrap
from mo_logs import Log
from mo_logs import constants
from mo_logs import startup
from mo_math import UNION
from mo_threads import Thread, Signal, Queue, THREAD_STOP

from mo_times.dates import Date, unicode2Date
from mo_times.timer import Timer
from pyLibrary.env import http, elasticsearch

DEBUG = False
NUM_THREAD = 4


def process_batch(todo_queue, revision, coverage_index, coverage_summary_index, settings, please_stop):
    """
    :param todo: list of files to process as a single block 
    :param coverage_index: 
    :param coverage_summary_index: 
    :param settings: 
    :param please_stop: 
    :return: 
    """

    for todo in todo_queue:
        if please_stop:
            return

        # WHAT HAVE WE SUMMARIZED ALREADY?
        coverage_summary_records = http.post_json(settings.url, json={
            "from": "coverage-summary",
            "select": [{"name": "count", "value": "etl.num_source_records", "aggregate": "sum"}],
            "edges": ["source.file.name"],
            "where": {"and": [
                {"eq": {"build.revision12": revision}},
                {"in": {"source.file.name": todo.source.file.name}}
            ]},
            "limit": 100000,
            "format": "list"
        }).data
        existing_count_summary = {t.source.file.name: t.count for t in coverage_summary_records}

        refresh_required = [
            rec.source.file.name
            for rec in todo
            if rec.source.file.name and existing_count_summary.get(rec.source.file.name) != rec.count
        ]

        Log.note("More coverage for revision {{revision}}:\n{{files}}", revision=revision, files=refresh_required)

        # PULL AN EXAMPLE
        coverage_example = http.post_json(settings.url, json={
            "from": "coverage",
            "where": {"and": [
                {"missing": "source.method.name"},
                {"neq": {"source.file.total_covered": 0}},
                {"eq": {"build.revision12": revision}},
                {"in": {"source.file.name": refresh_required}}
            ]},
            "limit": 1,
            "format": "list"
        }).data

        # PULL THE COVERAGE RECORDS
        coverage_records = http.post_json(settings.url, json={
            "from": "coverage",
            "select": "source.file",
            "where": {"and": [
                {"missing": "source.method.name"},
                {"neq": {"source.file.total_covered": 0}},
                {"eq": {"build.revision12": revision}},
                {"in": {"source.file.name": refresh_required}}
            ]},
            "limit": 100000,
            "format": "list"
        }).data

        coverage_summaries = []
        for g, file_level_coverage_records in jx.groupby(coverage_records, "name"):
            source_file_name = g["name"]

            cov = UNION(file_level_coverage_records.covered)
            uncov = UNION(file_level_coverage_records.uncovered) - cov
            coverage = {
                "source": {
                    "language": coverage_example[0].source.language,
                    "file": {
                        "name": source_file_name,
                        "is_file": True,
                        "covered": jx.sort(cov),
                        "uncovered": jx.sort(uncov),
                        "total_covered": len(cov),
                        "total_uncovered": len(uncov),
                        "min_line_siblings": 0  # PLACEHOLDER TO INDICATE DONE
                    }
                },
                "build": coverage_example[0].build,
                "repo": coverage_example[0].repo,
                "etl": {
                    "timestamp": Date.now(),
                    "num_source_records": len(file_level_coverage_records)  # RECORD NUMBER OF RECORDS USED TO COMPOSE THIS; IF THERE ARE MORE IN THE FUTURE, RECALC
                }
            }
            coverage_summaries.append({
                "id": "|".join([revision, source_file_name]),  # SOMETHING UNIQUE, IN CASE WE RECALCULATE
                "value": coverage
            })

        coverage_summary_index.extend(coverage_summaries)


def loop(source, coverage_summary_index, settings, please_stop):
    Log.note("Started loop")
    try:
        cluster = elasticsearch.Cluster(source)
        aliases = cluster.get_aliases()
        candidates = []
        for pairs in aliases:
            if pairs.alias == source.index:
                candidates.append(pairs.index)
        candidates = jx.sort(candidates, {".": "desc"})

        for index_name in candidates:
            if please_stop:
                return

            coverage_index = elasticsearch.Index(index=index_name, read_only=False, kwargs=source)
            push_date_filter = unicode2Date(coverage_index.settings.index[-15::], elasticsearch.INDEX_DATE_FORMAT)

            # IDENTIFY NEW WORK
            with Timer("Pulling work from index {{index}}", param={"index": index_name}):
                revisions = http.post_json(settings.url, json={
                    "from": "coverage",
                    "groupby": ["build.revision12", "repo.push.date"],
                    "where": {"and": [
                        {"gte": {"repo.push.date": push_date_filter}}
                    ]},
                    "format": "list",
                    "sort": "repo.push.date",
                    "limit": 10000
                }).data

                for rev in revisions.build.revision12:
                    todo = http.post_json(settings.url, json={
                        "from": "coverage",
                        "groupby": ["source.file.name"],
                        "where": {"and": [
                            {"eq": {"build.revision12": rev}},
                            {"neq": {"source.file.total_covered": 0}},
                            {"missing": "source.method.name"}
                        ]},
                        "format": "list",
                        "limit": 100000
                    })

                    queue = Queue("pending source files to review")
                    queue.extend(_groupby_size(todo.data, size=10000))

                    num_threads = coalesce(settings.threads, NUM_THREAD)
                    Log.note("Launch {{num}} threads", num=num_threads)
                    threads = [
                        Thread.run(
                            "processor" + unicode(i),
                            process_batch,
                            queue,
                            rev,
                            coverage_index,
                            coverage_summary_index,
                            settings,
                            please_stop=please_stop
                        )
                        for i in range(num_threads)
                    ]

                    # ADD STOP MESSAGE
                    queue.add(THREAD_STOP)

                    # WAIT FOR THEM TO COMPLETE
                    for t in threads:
                        t.join()
        return

    except Exception, e:
        Log.warning("Problem processing", cause=e)
    finally:
        Log.note("Processing loop is done")
        please_stop.go()


def _groupby_size(items, size):
    acc = 0
    output = []
    for i in items:
        if acc + i.count > size:
            yield wrap(output)
            acc = 0
            output = []
        acc += i.count
        output.append(i)
    if output:
        yield wrap(output)


def main():
    try:
        config = startup.read_settings()
        with startup.SingleInstance(flavor_id=config.args.filename):
            constants.set(config.constants)
            Log.start(config.debug)

            please_stop = Signal("main stop signal")
            coverage_summary_index = elasticsearch.Cluster(config.destination).get_or_create_index(read_only=False, kwargs=config.destination)
            coverage_summary_index.add_alias(config.destination.index)
            Log.note("start processing")
            Thread.run(
                "processing loop",
                loop,
                config.source,
                coverage_summary_index,
                config,
                please_stop=please_stop
            )
            Thread.wait_for_shutdown_signal(please_stop)
    except Exception, e:
        Log.error("Problem with code coverage score calculation", cause=e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()

