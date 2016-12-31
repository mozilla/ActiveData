# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import

from pyLibrary import convert
from pyLibrary.env.elasticsearch import Index, Cluster
from pyLibrary.debugs.logs import Log
from pyLibrary.env.files import File
from pyLibrary.meta import use_settings
from pyLibrary.queries import jx
from pyDots import Data
from pyDots import unwrap, wrap


def make_test_instance(name, settings):
    if settings.filename:
        File(settings.filename).delete()
    return open_test_instance(name, settings)


def open_test_instance(name, settings):
    if settings.filename:
        Log.note("Using {{filename}} as {{type}}",
            filename= settings.filename,
            type= name)
        return Fake_ES(settings)
    else:
        Log.note("Using ES cluster at {{host}} as {{type}}",
            host= settings.host,
            type= name)

        Index(read_only=False, settings=settings).delete()

        es = Cluster(settings).create_index(settings, limit_replicas=True)
        return es




class Fake_ES():
    @use_settings
    def __init__(self, filename, host="fake", index="fake", settings=None):
        self.settings = settings
        self.filename = settings.filename
        try:
            self.data = convert.json2value(File(self.filename).read())
        except Exception:
            self.data = Data()

    def search(self, query):
        query = wrap(query)
        f = jx.get(query.query.filtered.filter)
        filtered = wrap([{"_id": i, "_source": d} for i, d in self.data.items() if f(d)])
        if query.fields:
            return wrap({"hits": {"total": len(filtered), "hits": [{"_id": d._id, "fields": unwrap(jx.select([unwrap(d._source)], query.fields)[0])} for d in filtered]}})
        else:
            return wrap({"hits": {"total": len(filtered), "hits": filtered}})

    def extend(self, records):
        """
        JUST SO WE MODEL A Queue
        """
        records = {v["id"]: v["value"] for v in records}

        unwrap(self.data).update(records)

        data_as_json = convert.value2json(self.data, pretty=True)

        File(self.filename).write(data_as_json)
        Log.note("{{num}} documents added",  num= len(records))

    def add(self, record):
        if isinstance(record, list):
            Log.error("no longer accepting lists, use extend()")
        return self.extend([record])

    def delete_record(self, filter):
        f = convert.esfilter2where(filter)
        self.data = wrap({k: v for k, v in self.data.items() if not f(v)})

    def set_refresh_interval(self, seconds):
        pass

