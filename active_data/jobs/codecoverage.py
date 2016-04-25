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

from pyLibrary.collections import UNION, MIN
from pyLibrary.debugs import constants
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce, wrap, unwrap
from pyLibrary.env import http
from pyLibrary.queries import jx
from pyLibrary.testing import elasticsearch
from pyLibrary.thread.threads import Signal, Queue
from pyLibrary.thread.threads import Thread

NUM_THREAD=4


def process_batch(todo, coverage_index, settings, please_stop):
    for not_summarized in todo:
        if please_stop:
            return True

        # IS THERE MORE THAN ONE COVERAGE FILE FOR THIS REVISION?
        Log.note("Find dups for file {{file}}", file=not_summarized.source.file.name)
        dups = http.post_json(settings.url, json={
            "from": "coverage",
            "select": [
                {"name": "max_id", "value": "etl.source.id", "aggregate": "max"},
                {"name": "min_id", "value": "etl.source.id", "aggregate": "min"}
            ],
            "where": {"and": [
                {"missing": "source.method.name"},
                {"eq": {
                    "source.file.name": not_summarized.source.file.name,
                    "build.revision12": not_summarized.build.revision12
                }},
            ]},
            "groupby": [
                "test.url"
            ],
            "limit": 100000,
            "format": "list"
        })

        dups_found = False
        for d in dups.data:
            if d.max_id != d.min_id:
                dups_found = True
                Log.note(
                    "removing dups {{details|json}}",
                    details={
                        "id": int(d.max_id),
                        "test": d.test.url,
                        "source": not_summarized.source.file.name,
                        "revision": not_summarized.build.revision12
                    }
                )
                coverage_index.delete_record({"and": [
                    {"not": {"term": {"etl.source.id": int(d.max_id)}}},
                    {"term": {"test.url": d.test.url}},
                    {"term": {"source.file.name": not_summarized.source.file.name}},
                    {"term": {"build.revision12": not_summarized.build.revision12}}
                ]})
        if dups_found:
            continue

        # LIST ALL TESTS THAT COVER THIS FILE, AND THE LINES COVERED
        test_count = http.post_json(settings.url, json={
            "from": "coverage.source.file.covered",
            "where": {"and": [
                {"missing": "source.method.name"},
                {"eq": {
                    "source.file.name": not_summarized.source.file.name,
                    "build.revision12": not_summarized.build.revision12
                }},
            ]},
            "groupby": [
                "test.url",
                "line"
            ],
            "limit": 100000,
            "format": "list"
        })

        all_tests_covering_file = UNION(test_count.data.get("test.url"))
        num_tests = len(all_tests_covering_file)
        max_siblings = num_tests - 1
        Log.note("{{filename}} is covered by {{num}} tests", filename=not_summarized.source.file.name, num=num_tests)
        line_summary = list(
            (k, unwrap(wrap(list(v)).get("test.url")))
            for k, v in jx.groupby(test_count.data, keys="line")
        )

        # PULL THE RAW RECORD FOR MODIFICATION
        file_level_coverage_records = http.post_json(settings.url, json={
            "from": "coverage",
            "where": {"and": [
                {"missing": "source.method.name"},
                {"in": {"test.url": all_tests_covering_file}},
                {"eq": {
                    "source.file.name": not_summarized.source.file.name,
                    "build.revision12": not_summarized.build.revision12
                }}
            ]},
            "limit": 100000,
            "format": "list"
        })

        for test_name in all_tests_covering_file:
            siblings = [len(test_names)-1 for g, test_names in line_summary if test_name in test_names]
            min_siblings = MIN(siblings)
            coverage_candidates = jx.filter(file_level_coverage_records.data, lambda row, rownum, rows: row.test.url == test_name)
            coverage_record = coverage_candidates[0]
            coverage_record.source.file.max_test_siblings = max_siblings
            coverage_record.source.file.min_line_siblings = min_siblings
            coverage_record.source.file.score = (max_siblings - min_siblings) / (max_siblings + min_siblings + 1)

        if [d for d in file_level_coverage_records.data if d["source.file.min_line_siblings"] == None]:
            Log.warning("expecting all records to have summary")

        coverage_index.extend([{"id": d._id, "value": d} for d in file_level_coverage_records.data])


def loop(coverage_index, settings, please_stop):
    try:
        while not please_stop:
            coverage_index.refresh()

            # IDENTIFY NEW WORK
            Log.note("Identify new coverage to work on")
            todo = http.post_json(settings.url, json={
                "from": "coverage",
                "groupby": ["source.file.name", "build.revision12"],
                "where": {"and": [
                    {"missing": "source.method.name"},
                    {"missing": "source.file.min_line_siblings"}
                ]},
                "format": "list",
                "limit": coalesce(settings.batch_size, 100)
            })

            if not todo.data:
                please_stop.go()
                return

            queue = Queue("work queue")
            queue.extend(todo.data)

            threads = [
                Thread.run(
                    "processor" + unicode(i),
                    process_batch,
                    queue,
                    coverage_index,
                    settings,
                    please_stop=please_stop
                )
                for i in range(NUM_THREAD)
            ]

            # ADD A STOP MESSAGE FOR EACH THREAD
            for i in range(NUM_THREAD):
                queue.add(Thread.STOP)

            # WAIT FOR THEM TO COMPLETE
            for t in threads:
                t.join()

    except Exception, e:
        Log.warning("Problem processing", cause=e)
    finally:
        please_stop.go()


def main():
    try:
        config = startup.read_settings()
        constants.set(config.constants)
        Log.start(config.debug)

        please_stop = Signal("main stop signal")
        coverage_index = elasticsearch.Cluster(config.elasticsearch).get_index(read_only=False, settings=config.elasticsearch)
        Thread.run("processing loop", loop, coverage_index, config, please_stop=please_stop)
        Thread.wait_for_shutdown_signal(please_stop)
    except Exception, e:
        Log.error("Problem with code coverage score calculation", cause=e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()

