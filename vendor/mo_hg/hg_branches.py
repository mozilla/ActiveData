# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from bs4 import BeautifulSoup

from mo_collections import UniqueIndex
from mo_dots import Data, set_default
from mo_hg.hg_mozilla_org import DEFAULT_LOCALE
from mo_kwargs import override
from mo_logs import Log
from mo_logs import startup, constants
from mo_math import MAX
from mo_times.dates import Date
from mo_times.durations import SECOND, DAY
from pyLibrary.env import elasticsearch, http

EXTRA_WAIT_TIME = 20 * SECOND  # WAIT TIME TO SEND TO AWS, IF WE wait_forever
OLD_BRANCH = DAY


@override
def get_branches(hg, branches, kwargs=None):
    # TRY ES
    try:
        es = elasticsearch.Cluster(kwargs=branches).get_index(kwargs=branches, read_only=False)

        query = {
            "query": {"match_all": {}},
            "size": 10000
        }

        found_branches = es.search(query).hits.hits._source
        # IF IT IS TOO OLD, THEN PULL FROM HG
        oldest = Date(MAX(found_branches.etl.timestamp))
        if oldest == None or Date.now() - oldest > OLD_BRANCH:
            found_branches = _get_branches_from_hg(hg)
            es.extend({"id": b.name + " " + b.locale, "value": b} for b in found_branches)
            es.flush()

        try:
            return UniqueIndex(["name", "locale"], data=found_branches, fail_on_dup=False)
        except Exception as e:
            Log.error("Bad branch in ES index", cause=e)
    except Exception as e:
        if "Can not find index " in e:
            set_default(branches, {"schema": branches_schema})
            es = elasticsearch.Cluster(kwargs=branches).get_or_create_index(kwargs=branches)
            es.add_alias()
            return get_branches(kwargs=kwargs)
        Log.error("problem getting branches", cause=e)


@override
def _get_branches_from_hg(kwarg):
    # GET MAIN PAGE
    response = http.get(kwarg.url)
    doc = BeautifulSoup(response.all_content, "html.parser")

    all_repos = doc("table")[1]
    branches = UniqueIndex(["name", "locale"], fail_on_dup=False)
    for i, r in enumerate(all_repos("tr")):
        dir, name = [v.text.strip() for v in r("td")]

        b = _get_single_branch_from_hg(kwarg, name, dir.lstrip("/"))
        branches.extend(b)

    # branches.add(set_default({"name": "release-mozilla-beta"}, branches["mozilla-beta", DEFAULT_LOCALE]))
    for b in list(branches["mozilla-beta", ]):
        branches.add(set_default({"name": "release-mozilla-beta"}, b))  # THIS IS THE l10n "name"
        b.url = "https://hg.mozilla.org/releases/mozilla-beta"          # THIS IS THE

    for b in list(branches["mozilla-release", ]):
        branches.add(set_default({"name": "release-mozilla-release"}, b))

    for b in list(branches["mozilla-aurora", ]):
        if b.locale == "en-US":
            continue
        branches.add(set_default({"name": "comm-aurora"}, b))
        # b.url = "https://hg.mozilla.org/releases/mozilla-aurora"

    for b in list(branches):
        if b.name.startswith("mozilla-esr"):
            branches.add(set_default({"name": "release-" + b.name}, b))  # THIS IS THE l10n "name"
            b.url = "https://hg.mozilla.org/releases/" + b.name

    #CHECKS
    for b in branches:
        if b.name != b.name.lower():
            Log.error("Expecting lowercase name")
        if not b.locale:
            Log.error("Not expected")
        if not b.url.startswith("http"):
            Log.error("Expecting a valid url")
        if not b.etl.timestamp:
            Log.error("Expecting a timestamp")

    return branches


def _get_single_branch_from_hg(settings, description, dir):
    if dir == "users":
        return []
    response = http.get(settings.url + "/" + dir)
    doc = BeautifulSoup(response.all_content, "html.parser")

    output = []
    try:
        all_branches = doc("table")[0]
    except Exception:
        return []

    for i, b in enumerate(all_branches("tr")):
        if i == 0:
            continue  # IGNORE HEADER
        columns = b("td")

        try:
            path = columns[0].a.get('href')
            if path == "/":
                continue

            name, desc, last_used = [c.text.strip() for c in columns][0:3]

            if last_used.startswith('at'):
                last_used = last_used[2:]

            detail = Data(
                name=name.lower(),
                locale=DEFAULT_LOCALE,
                parent_name=description,
                url=settings.url + path,
                description=desc,
                last_used=Date(last_used),
                etl={"timestamp": Date.now()}
            )
            if detail.description == "unknown":
                detail.description = None

            # SOME BRANCHES HAVE NAME COLLISIONS, IGNORE LEAST POPULAR
            if path in [
                "/projects/dxr/",                   # moved to webtools
                "/build/compare-locales/",          # ?build team likes to clone?
                "/build/puppet/",                   # ?build team likes to clone?
                "/SeaMonkey/puppet/",               # looses the popularity contest
                "/releases/gaia-l10n/v1_2/en-US/",  # use default branch
                "/releases/gaia-l10n/v1_3/en-US/",  # use default branch
                "/releases/gaia-l10n/v1_4/en-US/",  # use default branch
                "/releases/gaia-l10n/v2_0/en-US/",  # use default branch
                "/releases/gaia-l10n/v2_1/en-US/",  # use default branch
                "/build/autoland/"
            ]:
                continue

            # MARKUP BRANCH IF LOCALE SPECIFIC
            if path.startswith("/l10n-central"):
                _path = path.strip("/").split("/")
                detail.locale = _path[-1]
                detail.name = "mozilla-central"
            elif path.startswith("/releases/l10n/"):
                _path = path.strip("/").split("/")
                detail.locale = _path[-1]
                detail.name = _path[-2].lower()
            elif path.startswith("/releases/gaia-l10n/"):
                _path = path.strip("/").split("/")
                detail.locale = _path[-1]
                detail.name = "gaia-" + _path[-2][1::]
            elif path.startswith("/weave-l10n"):
                _path = path.strip("/").split("/")
                detail.locale = _path[-1]
                detail.name = "weave"

            Log.note("Branch {{name}} {{locale}}", name=detail.name, locale=detail.locale)
            output.append(detail)
        except Exception as e:
            Log.warning("branch digestion problem", cause=e)

    return output


branches_schema = {
    "settings": {
        "index.number_of_replicas": 1,
        "index.number_of_shards": 1
    },
    "mappings": {
        "branch": {
            "_all": {
                "enabled": False
            },
            "dynamic_templates": [
                {
                    "default_strings": {
                        "mapping": {
                            "type": "keyword"
                        },
                        "match_mapping_type": "string",
                        "match": "*"
                    }
                }
            ],
            "properties": {
                "name": {
                    "type": "keyword",
                    "store": True
                },
                "description": {
                    "type": "keyword",
                    "store": True
                }
            }
        }
    }
}


def main():

    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        branches = _get_branches_from_hg(settings.hg)

        es = elasticsearch.Cluster(kwargs=settings.hg.branches).get_or_create_index(kwargs=settings.hg.branches)
        es.add_alias()
        es.extend({"id": b.name + " " + b.locale, "value": b} for b in branches)
        Log.alert("DONE!")
    except Exception as e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
