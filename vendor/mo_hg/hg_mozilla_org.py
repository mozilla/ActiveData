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

import re
from collections import Mapping
from copy import copy

from mo_future import text_type, binary_type

import mo_threads
from mo_dots import set_default, Null, coalesce, unwraplist, listwrap, wrap, Data
from mo_hg.parse import diff_to_json
from mo_hg.repos.changesets import Changeset
from mo_hg.repos.pushs import Push
from mo_hg.repos.revisions import Revision, revision_schema
from mo_json import json2value, value2json
from mo_kwargs import override
from mo_logs import Log, strings, machine_metadata
from mo_logs.exceptions import Explanation, assert_no_exception, Except, suppress_exception, WarnOnException
from mo_logs.strings import expand_template
from mo_math.randoms import Random
from mo_threads import Thread, Lock, Queue, THREAD_STOP
from mo_threads import Till
from mo_times.dates import Date
from mo_times.durations import SECOND, Duration, HOUR, MINUTE, DAY
from pyLibrary.env import http, elasticsearch
from pyLibrary.meta import cache

_hg_branches = None
_OLD_BRANCH = None


def _count(values):
    return len(list(values))


def _late_imports():
    global _hg_branches
    global _OLD_BRANCH

    from mo_hg import hg_branches as _hg_branches
    from mo_hg.hg_branches import OLD_BRANCH as _OLD_BRANCH

    _ = _hg_branches
    _ = _OLD_BRANCH


DEFAULT_LOCALE = "en-US"
DEBUG = False
DAEMON_DEBUG = False
DAEMON_HG_INTERVAL = 30 * SECOND  # HOW LONG TO WAIT BETWEEN HG REQUESTS (MAX)
DAEMON_WAIT_AFTER_TIMEOUT = 10 * MINUTE  # IF WE SEE A TIMEOUT, THEN WAIT
DAEMON_DO_NO_SCAN = ["try"]  # SOME BRANCHES ARE NOT WORTH SCANNING
DAEMON_QUEUE_SIZE = 2 ** 15
DAEMON_RECENT_HG_PULL = 2 * SECOND  # DETERMINE IF WE GOT DATA FROM HG (RECENT), OR ES (OLDER)
MAX_TODO_AGE = DAY  # THE DAEMON WILL NEVER STOP SCANNING; DO NOT ADD OLD REVISIONS TO THE todo QUEUE
MIN_ETL_AGE = Date("22sep2017").unix  # sept 22nd 2017  ARTIFACTS OLDER THAN THIS IN ES ARE REPLACED


GET_DIFF = True
MAX_DIFF_SIZE = 1000
DIFF_URL = "{{location}}/raw-rev/{{rev}}"
FILE_URL = "{{location}}/raw-file/{{rev}}{{path}}"


last_called_url = {}


class HgMozillaOrg(object):
    """
    USE hg.mozilla.org FOR REPO INFORMATION
    USE ES AS A FASTER CACHE FOR THE SAME
    """

    @override
    def __init__(
        self,
        hg=None,        # CONNECT TO hg
        repo=None,      # CONNECTION INFO FOR ES CACHE
        branches=None,  # CONNECTION INFO FOR ES CACHE
        use_cache=False,   # True IF WE WILL USE THE ES FOR DOWNLOADING BRANCHES
        timeout=30 * SECOND,
        kwargs=None
    ):
        if not _hg_branches:
            _late_imports()

        self.es_locker = Lock()
        self.todo = mo_threads.Queue("todo for hg daemon", max=DAEMON_QUEUE_SIZE)

        self.settings = kwargs
        self.timeout = Duration(timeout)

        # VERIFY CONNECTIVITY
        with Explanation("Test connect with hg"):
            response = http.head(self.settings.hg.url)

        if branches == None:
            self.branches = _hg_branches.get_branches(kwargs=kwargs)
            self.es = None
            return

        set_default(repo, {"schema": revision_schema})
        self.es = elasticsearch.Cluster(kwargs=repo).get_or_create_index(kwargs=repo)

        def setup_es(please_stop):
            with suppress_exception:
                self.es.add_alias()

            with suppress_exception:
                self.es.set_refresh_interval(seconds=1)

        Thread.run("setup_es", setup_es)
        self.branches = _hg_branches.get_branches(kwargs=kwargs)

        Thread.run("hg daemon", self._daemon)

    def _daemon(self, please_stop):
        while not please_stop:
            with Explanation("looking for work"):
                try:
                    branch, revisions = self.todo.pop(till=please_stop)
                except Exception as e:
                    if please_stop:
                        break
                    else:
                        raise e
                if branch.name in DAEMON_DO_NO_SCAN:
                    continue
                revisions = set(revisions)

                # FIND THE REVSIONS ON THIS BRANCH
                for r in list(revisions):
                    try:
                        rev = self.get_revision(Revision(branch=branch, changeset={"id": r}))
                        if DAEMON_DEBUG:
                            Log.note("found revision with push date {{date|datetime}}", date=rev.push.date)
                        revisions.discard(r)

                        if rev.etl.timestamp > Date.now()-DAEMON_RECENT_HG_PULL:
                            # SOME PUSHES ARE BIG, RUNNING THE RISK OTHER MACHINES ARE
                            # ALSO INTERESTED AND PERFORMING THE SAME SCAN. THIS DELAY
                            # WILL HAVE SMALL EFFECT ON THE MAJORITY OF SMALL PUSHES
                            # https://bugzilla.mozilla.org/show_bug.cgi?id=1417720
                            Till(seconds=Random.float(DAEMON_HG_INTERVAL).seconds).wait()

                    except Exception as e:
                        Log.warning(
                            "Scanning {{branch}} {{revision|left(12)}}",
                            branch=branch.name,
                            revision=r,
                            cause=e
                        )
                        if "Read timed out" in e:
                            Till(seconds=DAEMON_WAIT_AFTER_TIMEOUT.seconds).wait()


                # FIND ANY BRANCH THAT MAY HAVE THIS REVISION
                for r in list(revisions):
                    self._find_revision(r)

    @cache(duration=HOUR, lock=True)
    def get_revision(self, revision, locale=None, get_diff=False):
        """
        EXPECTING INCOMPLETE revision OBJECT
        RETURNS revision
        """
        rev = revision.changeset.id
        if not rev:
            return Null
        elif rev == "None":
            return Null
        elif revision.branch.name == None:
            return Null
        locale = coalesce(locale, revision.branch.locale, DEFAULT_LOCALE)
        output = self._get_from_elasticsearch(revision, locale=locale, get_diff=get_diff)
        if output:
            if not get_diff:  # DIFF IS BIG, DO NOT KEEP IT IF NOT NEEDED
                output.changeset.diff = None
            if DEBUG:
                Log.note("Got hg ({{branch}}, {{locale}}, {{revision}}) from ES", branch=output.branch.name, locale=locale, revision=output.changeset.id)
            if output.push.date >= Date.now()-MAX_TODO_AGE:
                self.todo.add((output.branch, listwrap(output.parents)))
                self.todo.add((output.branch, listwrap(output.children)))
            if output.push.date:
                return output

        found_revision = copy(revision)
        if isinstance(found_revision.branch, (text_type, binary_type)):
            lower_name = found_revision.branch.lower()
        else:
            lower_name = found_revision.branch.name.lower()

        if not lower_name:
            Log.error("Defective revision? {{rev|json}}", rev=found_revision.branch)

        b = found_revision.branch = self.branches[(lower_name, locale)]
        if not b:
            b = found_revision.branch = self.branches[(lower_name, DEFAULT_LOCALE)]
            if not b:
                Log.error("can not find branch ({{branch}}, {{locale}})", branch=lower_name, locale=locale)

        if Date.now() - Date(b.etl.timestamp) > _OLD_BRANCH:
            self.branches = _hg_branches.get_branches(kwargs=self.settings)

        push = self._get_push(found_revision.branch, found_revision.changeset.id)

        url1 = found_revision.branch.url.rstrip("/") + "/json-info?node=" + found_revision.changeset.id[0:12]
        url2 = found_revision.branch.url.rstrip("/") + "/json-rev/" + found_revision.changeset.id[0:12]
        with Explanation("get revision from {{url}}", url=url1, debug=DEBUG):
            raw_rev2 = Null
            try:
                raw_rev1 = self._get_raw_json_info(url1, found_revision.branch)
                raw_rev2 = self._get_raw_json_rev(url2, found_revision.branch)
            except Exception as e:
                if "Hg denies it exists" in e:
                    raw_rev1 = Data(node=revision.changeset.id)
                else:
                    raise e
            output = self._normalize_revision(set_default(raw_rev1, raw_rev2), found_revision, push, get_diff)
            if output.push.date >= Date.now()-MAX_TODO_AGE:
                self.todo.add((output.branch, listwrap(output.parents)))
                self.todo.add((output.branch, listwrap(output.children)))

            if not get_diff:  # DIFF IS BIG, DO NOT KEEP IT IF NOT NEEDED
                output.changeset.diff = None
            return output

    def _get_from_elasticsearch(self, revision, locale=None, get_diff=False):
        rev = revision.changeset.id
        if self.es.cluster.version.startswith("1.7."):
            query = {
                "query": {"filtered": {
                    "query": {"match_all": {}},
                    "filter": {"and": [
                        {"term": {"changeset.id12": rev[0:12]}},
                        {"term": {"branch.name": revision.branch.name}},
                        {"term": {"branch.locale": coalesce(locale, revision.branch.locale, DEFAULT_LOCALE)}},
                        {"range": {"etl.timestamp": {"gt": MIN_ETL_AGE}}}
                    ]}
                }},
                "size": 2000
            }
        else:
            query = {
                "query": {"bool": {"must": [
                    {"term": {"changeset.id12": rev[0:12]}},
                    {"term": {"branch.name": revision.branch.name}},
                    {"term": {"branch.locale": coalesce(locale, revision.branch.locale, DEFAULT_LOCALE)}},
                    {"range": {"etl.timestamp": {"gt": MIN_ETL_AGE}}}
                ]}},
                "size": 2000
            }

        for attempt in range(3):
            try:
                with self.es_locker:
                    docs = self.es.search(query).hits.hits
                break
            except Exception as e:
                e = Except.wrap(e)
                if "NodeNotConnectedException" in e:
                    # WE LOST A NODE, THIS MAY TAKE A WHILE
                    (Till(seconds=Random.int(5 * 60))).wait()
                    continue
                elif "EsRejectedExecutionException[rejected execution (queue capacity" in e:
                    (Till(seconds=Random.int(30))).wait()
                    continue
                else:
                    Log.warning("Bad ES call, fall back to TH", cause=e)
                    return None

        best = docs[0]._source
        if len(docs) > 1:
            for d in docs:
                if d._id.endswith(d._source.branch.locale):
                    best = d._source
            Log.warning("expecting no more than one document")

        if not GET_DIFF and not get_diff:
            return best
        elif best.changeset.diff:
            return best
        elif not best.changeset.files:
            return best  # NOT EXPECTING A DIFF, RETURN IT ANYWAY
        else:
            return None

    @cache(duration=HOUR, lock=True)
    def _get_raw_json_info(self, url, branch):
        raw_revs = self._get_and_retry(url, branch)
        if "(not in 'served' subset)" in raw_revs:
            Log.error("Tried {{url}}. Hg denies it exists.", url=url)
        if isinstance(raw_revs, text_type) and raw_revs.startswith("unknown revision '"):
            Log.error("Tried {{url}}. Hg denies it exists.", url=url)
        if len(raw_revs) != 1:
            Log.error("do not know what to do")
        return raw_revs.values()[0]

    @cache(duration=HOUR, lock=True)
    def _get_raw_json_rev(self, url, branch):
        raw_rev = self._get_and_retry(url, branch)
        return raw_rev

    @cache(duration=HOUR, lock=True)
    def _get_push(self, branch, changeset_id):
        if self.es.cluster.version.startswith("1.7."):
            query = {
                "query": {"filtered": {
                    "query": {"match_all": {}},
                    "filter": {"and": [
                        {"term": {"branch.name": branch.name}},
                        {"prefix": {"changeset.id": changeset_id[0:12]}}
                    ]}
                }},
                "size": 1
            }
        else:
            query = {
                "query": {"bool": {"must": [
                    {"term": {"branch.name": branch.name}},
                    {"prefix": {"changeset.id": changeset_id[0:12]}}
                ]}},
                "size": 1
            }

        try:
            # ALWAYS TRY ES FIRST
            with self.es_locker:
                response = self.es.search(query)
                json_push = response.hits.hits[0]._source.push
            if json_push:
                return json_push
        except Exception:
            pass

        url = branch.url.rstrip("/") + "/json-pushes?full=1&changeset=" + changeset_id
        with Explanation("Pulling pushlog from {{url}}", url=url, debug=DEBUG):
            Log.note(
                "Reading pushlog from {{url}}",
                url=url,
                changeset=changeset_id
            )
            data = self._get_and_retry(url, branch)
            # QUEUE UP THE OTHER CHANGESETS IN THE PUSH
            self.todo.add((branch, [c.node for cs in data.values().changesets for c in cs]))
            pushes = [
                Push(id=int(index), date=_push.date, user=_push.user)
                for index, _push in data.items()
            ]

        if len(pushes) == 0:
            return Null
        elif len(pushes) == 1:
            return pushes[0]
        else:
            Log.error("do not know what to do")

    def _normalize_revision(self, r, found_revision, push, get_diff):
        new_names = set(r.keys()) - {"rev", "node", "user", "description", "desc", "date", "files", "backedoutby", "parents", "children", "branch", "tags", "pushuser", "pushdate", "pushid", "phase", "bookmarks"}
        if new_names and not r.tags:
            Log.warning("hg is returning new property names ({{names}})", names=new_names)

        changeset = Changeset(
            id=r.node,
            id12=r.node[0:12],
            author=r.user,
            description=strings.limit(coalesce(r.description, r.desc), 2000),
            date=parse_hg_date(r.date),
            files=r.files,
            backedoutby=r.backedoutby if r.backedoutby else None,
            bug=self._extract_bug_id(r.description)
        )
        rev = Revision(
            branch=found_revision.branch,
            index=r.rev,
            changeset=changeset,
            parents=unwraplist(list(set(r.parents))),
            children=unwraplist(list(set(r.children))),
            push=push,
            phase=r.phase,
            bookmarks=unwraplist(r.bookmarks),
            etl={"timestamp": Date.now().unix, "machine": machine_metadata}
        )

        r.pushuser = None
        r.pushdate = None
        r.pushid = None
        r.node = None
        r.user = None
        r.desc = None
        r.description = None
        r.date = None
        r.files = None
        r.backedoutby = None
        r.parents = None
        r.children = None
        r.bookmarks = None

        set_default(rev, r)

        # ADD THE DIFF
        if get_diff or GET_DIFF:
            rev.changeset.diff = self._get_json_diff_from_hg(rev)

        try:
            _id = coalesce(rev.changeset.id12, "") + "-" + rev.branch.name + "-" + coalesce(rev.branch.locale, DEFAULT_LOCALE)
            with self.es_locker:
                self.es.add({"id": _id, "value": rev})
        except Exception as e:
            Log.warning("did not save to ES", cause=e)

        return rev

    def _get_and_retry(self, url, branch, **kwargs):
        """
        requests 2.5.0 HTTPS IS A LITTLE UNSTABLE
        """
        kwargs = set_default(kwargs, {"timeout": self.timeout.seconds})
        try:
            output = _get_url(url, branch, **kwargs)
            return output
        except Exception as e:
            output = Null

        try:
            (Till(seconds=5)).wait()
            return _get_url(url.replace("https://", "http://"), branch, **kwargs)
        except Exception as f:
            pass

        path = url.split("/")
        if path[3] == "l10n-central":
            # FROM https://hg.mozilla.org/l10n-central/tr/json-pushes?full=1&changeset=a6eeb28458fd
            # TO   https://hg.mozilla.org/mozilla-central/json-pushes?full=1&changeset=a6eeb28458fd
            path = path[0:3] + ["mozilla-central"] + path[5:]
            return self._get_and_retry("/".join(path), branch, **kwargs)
        elif len(path) > 5 and path[5] == "mozilla-aurora":
            # FROM https://hg.mozilla.org/releases/l10n/mozilla-aurora/pt-PT/json-pushes?full=1&changeset=b44a8c68fc60
            # TO   https://hg.mozilla.org/releases/mozilla-aurora/json-pushes?full=1&changeset=b44a8c68fc60
            path = path[0:4] + ["mozilla-aurora"] + path[7:]
            return self._get_and_retry("/".join(path), branch, **kwargs)
        elif len(path) > 5 and path[5] == "mozilla-beta":
            # FROM https://hg.mozilla.org/releases/l10n/mozilla-beta/lt/json-pushes?full=1&changeset=03fbf7556c94
            # TO   https://hg.mozilla.org/releases/mozilla-beta/json-pushes?full=1&changeset=b44a8c68fc60
            path = path[0:4] + ["mozilla-beta"] + path[7:]
            return self._get_and_retry("/".join(path), branch, **kwargs)
        elif len(path) > 7 and path[5] == "mozilla-release":
            # FROM https://hg.mozilla.org/releases/l10n/mozilla-release/en-GB/json-pushes?full=1&changeset=57f513ab03308adc7aa02cc2ea8d73fe56ae644b
            # TO   https://hg.mozilla.org/releases/mozilla-release/json-pushes?full=1&changeset=57f513ab03308adc7aa02cc2ea8d73fe56ae644b
            path = path[0:4] + ["mozilla-release"] + path[7:]
            return self._get_and_retry("/".join(path), branch, **kwargs)
        elif len(path) > 5 and path[4] == "autoland":
            # FROM https://hg.mozilla.org/build/autoland/json-pushes?full=1&changeset=3ccccf8e5036179a3178437cabc154b5e04b333d
            # TO  https://hg.mozilla.org/integration/autoland/json-pushes?full=1&changeset=3ccccf8e5036179a3178437cabc154b5e04b333d
            path = path[0:3] + ["try"] + path[5:]
            return self._get_and_retry("/".join(path), branch, **kwargs)

        Log.error("Tried {{url}} twice.  Both failed.", {"url": url}, cause=[e, f])

    @cache(duration=HOUR, lock=True)
    def _find_revision(self, revision):
        please_stop = False
        locker = Lock()
        output = []
        queue = Queue("branches", max=2000)
        queue.extend(b for b in self.branches if b.locale == DEFAULT_LOCALE and b.name in ["try", "mozilla-inbound", "autoland"])
        queue.add(THREAD_STOP)

        problems = []
        def _find(please_stop):
            for b in queue:
                if please_stop:
                    return
                try:
                    url = b.url + "json-info?node=" + revision
                    rev = self.get_revision(Revision(branch=b, changeset={"id": revision}))
                    with locker:
                        output.append(rev)
                    Log.note("Revision found at {{url}}", url=url)
                except Exception as f:
                    problems.append(f)

        threads = []
        for i in range(3):
            threads.append(Thread.run("find changeset " + text_type(i), _find, please_stop=please_stop))

        for t in threads:
            with assert_no_exception:
                t.join()

        return output

    def _extract_bug_id(self, description):
        """
        LOOK INTO description to FIND bug_id
        """
        if description == None:
            return None
        match = re.findall(r'[Bb](?:ug)?\s*([0-9]{5,7})', description)
        if match:
            return int(match[0])
        return None

    def _get_json_diff_from_hg(self, revision):
        """
        :param revision: INCOMPLETE REVISION OBJECT
        :return:
        """
        @cache(duration=MINUTE, lock=True)
        def inner(changeset_id):
            if self.es.cluster.version.startswith("1.7."):
                query = {
                    "query": {"filtered": {
                        "query": {"match_all": {}},
                        "filter": {"and": [
                            {"prefix": {"changeset.id": changeset_id}},
                            {"range": {"etl.timestamp": {"gt": MIN_ETL_AGE}}}
                        ]}
                    }},
                    "size": 1
                }
            else:
                query = {
                    "query": {"bool": {"must": [
                        {"prefix": {"changeset.id": changeset_id}},
                        {"range": {"etl.timestamp": {"gt": MIN_ETL_AGE}}}
                    ]}},
                    "size": 1
                }

            try:
                # ALWAYS TRY ES FIRST
                with self.es_locker:
                    response = self.es.search(query)
                    json_diff = response.hits.hits[0]._source.changeset.diff
                if json_diff:
                    return json_diff
            except Exception as e:
                pass

            url = expand_template(DIFF_URL, {"location": revision.branch.url, "rev": changeset_id})
            if DEBUG:
                Log.note("get unified diff from {{url}}", url=url)
            try:
                response = http.get(url)
                diff = response.content.decode("utf8", "replace")
                json_diff = diff_to_json(diff)
                num_changes = _count(c for f in json_diff for c in f.changes)
                if json_diff:
                    if num_changes < MAX_DIFF_SIZE:
                        return json_diff
                    elif revision.changeset.description.startswith("merge "):
                        return None  # IGNORE THE MERGE CHANGESETS
                    else:
                        Log.warning("Revision at {{url}} has a diff with {{num}} changes, ignored", url=url, num=num_changes)
                        for file in json_diff:
                            file.changes = None
                        return json_diff
            except Exception as e:
                Log.warning("could not get unified diff", cause=e)

        return inner(revision.changeset.id)

    def _get_source_code_from_hg(self, revision, file_path):
        response = http.get(expand_template(FILE_URL, {"location": revision.branch.url, "rev": revision.changeset.id, "path": file_path}))
        return response.content.decode("utf8", "replace")


def _trim(url):
    return url.split("/json-pushes?")[0].split("/json-info?")[0].split("/json-rev/")[0]


def _get_url(url, branch, **kwargs):
    with Explanation("get push from {{url}}", url=url, debug=DEBUG):
        response = http.get(url, **kwargs)
        data = json2value(response.content.decode("utf8"))
        if isinstance(data, (text_type, str)) and data.startswith("unknown revision"):
            Log.error("Unknown push {{revision}}", revision=strings.between(data, "'", "'"))
        branch.url = _trim(url)  # RECORD THIS SUCCESS IN THE BRANCH
        return data


def parse_hg_date(date):
    if isinstance(date, text_type):
        return Date(date)
    elif isinstance(date, list):
        # FIRST IN TUPLE (timestamp, time_zone) TUPLE, WHERE timestamp IS GMT
        return Date(date[0])
    else:
        Log.error("Can not deal with date like {{date|json}}", date=date)


def minimize_repo(repo):
    # output = set_default({}, _exclude_from_repo, repo)
    output = wrap(_copy_but(repo, _exclude_from_repo))
    output.changeset.description = strings.limit(output.changeset.description, 1000)
    return output


_exclude_from_repo = Data()  # A STRUCTURE TO
for k in [
    "changeset.files",
    "changeset.diff",
    "etl",
    "branch.last_used",
    "branch.description",
    "branch.etl",
    "branch.parent_name",
    "children",
    "parents",
    "phase",
    "bookmarks"
]:
    _exclude_from_repo[k] = True
_exclude_from_repo = _exclude_from_repo


def _copy_but(value, exclude):
    output = {}
    for k, v in value.items():
        e = exclude.get(k, {})
        if e!=True:
            if isinstance(v, Mapping):
                v2 = _copy_but(v, e)
                if v2 != None:
                    output[k] = v2
            elif v != None:
                output[k] = v
    return output if output else None
