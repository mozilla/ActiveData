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

from pyLibrary import convert
from pyLibrary.collections import UNION, MIN
from pyLibrary.debugs import constants
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce, wrap
from pyLibrary.env import http
from pyLibrary.queries import jx
from pyLibrary.testing import elasticsearch
from pyLibrary.thread.threads import Signal
from pyLibrary.thread.threads import Thread


_ = convert

def process_batch(coverage_index, settings, please_stop):
    # IDENTIFY NEW WORK
    todo = http.post_json(settings.url, json={
        "from": "coverage",
        "groupby": ["source.file.name", "build.revision12"],
        "where": {"and": [
            {"missing": "source.method.name"},
            {"missing": "source.file.min_test_count"}
        ]},
        "format": "list",
        "limit": coalesce(settings.batch_size, 1000)
    })

    if not todo.data:
        return True

    for not_summarized in todo.data:
        if please_stop:
            return True

        Log.note("Summarize file {{filename}}", filename=not_summarized.source.file.name)

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
        Log.note("{{filename}} is covered by {{num}} tests", filename=not_summarized.source.file.name, num=len(all_tests_covering_file))
        line_summary = list(
            (k, wrap(list(v)))
            for k, v in jx.groupby(test_count.data, keys="line")
        )



        # PULL THE RAW RECORD FOR MODIFICATION
        file_level_coverage_records = http.post_json(settings.url, json={
            "from": "coverage",
            "where": {"and": [
                {"missing": "source.method.name"},
                {"in": {"test.url": all_tests_covering_file}},
                {"eq": {"source.file.name": not_summarized.source.file.name}},
            ]},
            "limit": 100000,
            "format": "list"
        })

        for test_name in all_tests_covering_file:
            siblings = [len(t)-1 for g, t in line_summary if test_name in t.get("test.url")]
            min_siblings = MIN(siblings)
            coverage_record = jx.filter(file_level_coverage_records.data, lambda row, rownum, rows: row.test.url == test_name)[0]
            coverage_record.source.file.min_siblings = min_siblings
            coverage_record.source.file.score = 1 / (min_siblings + 1)

        coverage_index.extend([{"id": d._id, "value": d} for d in file_level_coverage_records.data])


def loop(coverage_index, settings, please_stop):
    while not please_stop:
        try:
            done = process_batch(coverage_index, settings, please_stop)
            if done:
                return
        except Exception, e:
            Log.warning("Problem processing", cause=e)


def main():
    try:
        config = startup.read_settings()
        constants.set(config.constants)
        Log.start(config.debug)

        please_stop = Signal()
        coverage_index = elasticsearch.Cluster(config.elasticsearch).get_index(read_only=False, settings=config.elasticsearch)
        Thread.run("processing", loop, coverage_index, config, please_stop=please_stop)
        Thread.wait_for_shutdown_signal(please_stop)
    except Exception, e:
        Log.error("Problem with code coverage score calculation", cause=e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()

