# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import division
from __future__ import unicode_literals

import json

from flask import Response
from mo_dots import coalesce
from mo_files.url import URL
from mo_future import text_type, xrange
from mo_json import value2json
from mo_kwargs import override
from mo_logs import Log
from mo_threads import Lock, Signal, Queue, Thread, Till
from mo_times import Date, SECOND, MINUTE
from pyLibrary.env import http
from pyLibrary.sql.sqlite import Sqlite, quote_value, quote_list

from mo_hg.rate_logger import RateLogger

APP_NAME = "HG Cache"
CONCURRENCY = 5
AMORTIZATION_PERIOD = SECOND
HG_REQUEST_PER_SECOND = 10
CACHE_RETENTION = 10 * MINUTE


class Cache(object):
    """
    For Caching hg.mo requests
    """

    @override
    def __init__(self, rate=None, amortization_period=None, source=None, database=None, kwargs=None):
        self.amortization_period = coalesce(amortization_period, AMORTIZATION_PERIOD)
        self.rate = coalesce(rate, HG_REQUEST_PER_SECOND)
        self.cache_locker = Lock()
        self.cache = {}  # MAP FROM url TO (ready, headers, response, timestamp) PAIR
        self.no_cache = {}  # VERY SHORT TERM CACHE
        self.workers = []
        self.todo = Queue(APP_NAME+" todo")
        self.requests = Queue(APP_NAME + " requests", max=int(self.rate * self.amortization_period.seconds))
        self.url = URL(source.url)
        self.db = Sqlite(database)
        self.inbound_rate = RateLogger("Inbound")
        self.outbound_rate = RateLogger("hg.mo")

        if not self.db.query("SELECT name FROM sqlite_master WHERE type='table'").data:
            with self.db.transaction() as t:
                t.execute(
                    "CREATE TABLE cache ("
                    "   path TEXT PRIMARY KEY, "
                    "   headers TEXT, "
                    "   response TEXT, "
                    "   timestamp REAL "
                    ")"
                )

        self.threads = [
            Thread.run(APP_NAME+" worker" + text_type(i), self._worker)
            for i in range(CONCURRENCY)
        ]
        self.limiter = Thread.run(APP_NAME+" limiter", self._rate_limiter)
        self.cleaner = Thread.run(APP_NAME+" cleaner", self._cache_cleaner)

    def _rate_limiter(self, please_stop):
        try:
            max_requests = self.requests.max
            recent_requests = []

            while not please_stop:
                now = Date.now()
                too_old = now - self.amortization_period

                recent_requests = [t for t in recent_requests if t > too_old]

                num_recent = len(recent_requests)
                if num_recent >= max_requests:
                    space_free_at = recent_requests[0] + self.amortization_period
                    (please_stop | Till(till=space_free_at.unix)).wait()
                    continue
                for _ in xrange(num_recent, max_requests):
                    request = self.todo.pop()
                    now = Date.now()
                    recent_requests.append(now)
                    self.requests.add(request)
        except Exception as e:
            Log.warning("failure", cause=e)

    def _cache_cleaner(self, please_stop):
        while not please_stop:
            now = Date.now()
            too_old = now-CACHE_RETENTION

            remove = set()
            with self.cache_locker:
                for path, (ready, headers, response, timestamp) in self.cache:
                    if timestamp < too_old:
                        remove.add(path)
                for r in remove:
                    del self.cache[r]
            (please_stop | Till(seconds=CACHE_RETENTION.seconds / 2)).wait()

    def please_cache(self, path):
        """
        :return: False if `path` is not to be cached
        """
        if path.endswith("/tip"):
            return False
        if any(k in path for k in ["/json-annotate/", "/json-info/", "/json-log/", "/json-rev/", "/rev/", "/raw-rev/", "/raw-file/", "/json-pushes", "/pushloghtml", "/file/"]):
            return True

        return False

    def request(self, method, path, headers):
        now = Date.now()
        self.inbound_rate.add(now)
        ready = Signal(path)

        # TEST CACHE
        with self.cache_locker:
            pair = self.cache.get(path)
            if pair is None:
                self.cache[path] = (ready, None, None, now)


        if pair is not None:
            # REQUEST IS IN THE QUEUE ALREADY, WAIT
            ready, headers, response, then = pair
            if response is None:
                ready.wait()
                with self.cache_locker:
                    ready, headers, response, timestamp = self.cache.get(path)
            with self.db.transaction() as t:
                t.execute("UPDATE cache SET timestamp=" + quote_value(now) + " WHERE path=" + quote_value(path) + " AND timestamp<" + quote_value(now))
            return Response(
                response,
                status=200,
                headers=json.loads(headers)
            )

        # TEST DB
        db_response = self.db.query("SELECT headers, response FROM cache WHERE path=" + quote_value(path)).data
        if db_response:
            headers, response = db_response[0]
            with self.db.transaction() as t:
                t.execute("UPDATE cache SET timestamp=" + quote_value(now) + " WHERE path=" + quote_value(path) + " AND timestamp<" + quote_value(now))
            with self.cache_locker:
                self.cache[path] = (ready, headers, response.encode('latin1'), now)
            ready.go()

            return Response(
                response,
                status=200,
                headers=json.loads(headers)
            )

        # MAKE A NETWORK REQUEST
        self.todo.add((ready, method, path, headers, now))
        ready.wait()
        with self.cache_locker:
            ready, headers, response, timestamp = self.cache[path]
        return Response(
            response,
            status=200,
            headers=json.loads(headers)
        )

    def _worker(self, please_stop):
        while not please_stop:
            pair = self.requests.pop(till=please_stop)
            if please_stop:
                break
            ready, method, path, req_headers, timestamp = pair

            try:
                url = self.url / path
                self.outbound_rate.add(Date.now())
                response = http.request(method, url, req_headers)

                del response.headers['transfer-encoding']
                resp_headers = value2json(response.headers)
                resp_content = response.raw.read()

                please_cache = self.please_cache(path)
                if please_cache:
                    with self.db.transaction() as t:
                        t.execute("INSERT INTO cache (path, headers, response, timestamp) VALUES" + quote_list((path, resp_headers, resp_content.decode('latin1'), timestamp)))
                with self.cache_locker:
                    self.cache[path] = (ready, resp_headers, resp_content, timestamp)
            except Exception as e:
                Log.warning("problem with request to {{path}}", path=path, cause=e)
                with self.cache_locker:
                    ready, headers, response = self.cache[path]
                    del self.cache[path]
            finally:
                ready.go()



