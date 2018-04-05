
# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Trung Do (chin.bimbo@gmail.com)
#
from __future__ import division
from __future__ import unicode_literals

from mo_dots import listwrap
from mo_files import File
from mo_json import json2value, value2json
from mo_kwargs import override
from mo_logs import Log
from mo_times import Timer
from pyLibrary.env import http
from pyLibrary.sql import sql_iso, sql_list
from pyLibrary.sql.sqlite import Sqlite, quote_value

DEBUG = True


class TuidClient(object):

    @override
    def __init__(self, endpoint, timeout=30, db_filename="tuid.sqlite", kwargs=None):
        self.enabled = True
        self.tuid_endpoint = endpoint
        self.timeout = timeout

        if DEBUG:
            File(db_filename).delete()
        self.db = Sqlite(filename=db_filename)

        if not self.db.query("SELECT name FROM sqlite_master WHERE type='table';").data:
            self._setup()
        self.db.commit()

    def _setup(self):
        self.db.execute("""
        CREATE TABLE tuid (
            revision CHAR(12),
            file TEXT,
            tuids TEXT,
            PRIMARY KEY(revision, file)
        )
        """)

    def annotate_sources(self, revision, sources):
        """
        :param revision: REVISION NUMBER TO USE FOR MARKUP
        :param sources: LIST OF COVERAGE SOURCE STRUCTURES TO MARKUP
        :return: NOTHING, sources ARE MARKED UP
        """
        try:
            revision = revision[:12]
            sources = listwrap(sources)
            filenames = sources.file.name

            with Timer("markup sources for {{num}} files", {"num": len(filenames)}):
                # WHAT DO WE HAVE
                response = self.db.query(
                    "SELECT file, tuids FROM tuid WHERE revision=" + quote_value(revision) +
                    " AND file in " + sql_iso(sql_list(map(quote_value, filenames)))
                )
                found = {file: json2value(tuids) for file, tuids in response.data}

                remaining = set(filenames) - set(found.keys())
                if remaining:
                    more = self._get_tuid_from_endpoint(revision, remaining)
                    if more == None:
                        return
                    found.update(more)

                for source in sources:
                    line_to_tuid = found[source.file.name]
                    if line_to_tuid is not None:
                        source.file.tuid_covered = [
                            {"line": line, "tuid": line_to_tuid[line]}
                            for line in source.file.covered
                            if line_to_tuid[line]
                        ]
                        source.file.tuid_uncovered = [
                            {"line": line, "tuid": line_to_tuid[line]}
                            for line in source.file.uncovered
                            if line_to_tuid[line]
                        ]
        except Exception as e:
            self.enabled = False
            Log.warning("unexpected failure", cause=e)

    def get_tuid(self, revision, file):
        """
        :param revision:
        :param file:
        :return: A LIST OF TUIDS
        """
        if not self.enabled:
            return None

        revision = revision[:12]
        # TRY THE DATABASE
        response = self.db.query("SELECT tuids FROM tuid WHERe revision=" + quote_value(revision) + " AND file=" + quote_value(file))
        if response:
            return json2value(response.data[0][0])

        return self._get_tuid_from_endpoint(revision, [file])[file]

    def _get_tuid_from_endpoint(self, revision, files):
        """
        GET TUIDS FROM ENDPOINT, AND STORE IN DB
        :param revision:
        :param files:
        :return: MAP FROM FILENAME TO TUID LIST
        """

        with Timer(
            "ask tuid service for {{num}} files at {{revision|left(12)}}",
            {"num": len(files), "revision": revision},
            debug=DEBUG
        ):
            try:
                timeout = self.timeout
                if not self.enabled:
                    timeout = 0

                response = http.post_json(
                    self.tuid_endpoint,
                    json={
                        "from": "files",
                        "where": {"and": [
                            {"eq": {"revision": revision}},
                            {"in": {"path": files}}
                        ]},
                        "format": "list"
                    },
                    timeout=timeout
                )

                self.db.execute(
                    "INSERT INTO tuid (revision, file, tuids) VALUES " + sql_list(
                        sql_iso(sql_list(map(quote_value, (revision, r.path, value2json(r.tuids)))))
                        for r in response.data
                    )
                )
                self.db.commit()

                return {r.path: r.tuids for r in response.data}

            except Exception as e:
                if not self.enabled:
                    Log.warning("TUID service has problems, disabling.", cause=e)
                self.enabled = False
                return None
