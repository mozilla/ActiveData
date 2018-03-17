# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import os
import subprocess

import whatthepatch

from mo_dots import Null, coalesce
from mo_files import File
from mo_future import text_type
from mo_hg.hg_mozilla_org import HgMozillaOrg
from mo_kwargs import override
from mo_logs import Log, Except
from pyLibrary.env import http
from pyLibrary.sql import sql_list, sql_iso
from pyLibrary.sql.sqlite import quote_value
from tuid import sql

DEBUG = False
RETRY = {"times": 3, "sleep": 5}

GET_LINES_QUERY = (
    "SELECT tuid, line" +
    " FROM temporal" +
    " WHERE file=? and revision=?" +
    " ORDER BY line"
)

GET_TUID_QUERY = "SELECT tuid FROM temporal WHERE file=? and revision=? and line=?"

GET_ANNOTATION_QUERY = "SELECT annotation FROM annotations WHERE revision=? and file=?"

GET_LATEST_MODIFICATION = "SELECT revision FROM latestFileMod WHERE file=?"

GET_PAST_MODIFICATIONS = "SELECT pastRevisions FROM latestFileMod WHERE file=?"


class TUIDService:

    @override
    def __init__(self, database, hg, hg_cache, conn=None, kwargs=None):
        try:
            self.config = kwargs
            self.DEBUG = self.config['debugTUID']

            self.conn = conn if conn else sql.Sql(self.config.database.name)
            self.hg_cache = HgMozillaOrg(hg_cache) if hg_cache else Null

            if not self.conn.get_one("SELECT name FROM sqlite_master WHERE type='table';"):
                self.init_db()

            self.next_tuid = coalesce(self.conn.get_one("SELECT max(tuid)+1 FROM temporal")[0], 1)
        except Exception as e:
            Log.error("can not setup service", cause=e)


    def tuid(self):
        """
        :return: next tuid
        """
        try:
            return self.next_tuid
        finally:
            self.next_tuid += 1


    def init_db(self):
        self.conn.execute('''
        CREATE TABLE temporal (
            tuid     INTEGER,
            revision CHAR(12) NOT NULL,
            file     TEXT,
            line     INTEGER
        );''')

        self.conn.execute('''
        CREATE TABLE annotations (
            revision       CHAR(12) NOT NULL,
            file           TEXT,
            annotation     TEXT,
            PRIMARY KEY(revision, file)
        );''')

        # Used in frontier updating
        self.conn.execute('''
        CREATE TABLE latestFileMod (
            file           TEXT,
            revision       CHAR(12) NOT NULL,
            pastRevisions  TEXT,
            PRIMARY KEY(file)
        );''')

        self.conn.execute("CREATE UNIQUE INDEX temporal_rev_file ON temporal(revision, file, line)")
        Log.note("Table created successfully")


    # True if dummy, false if not.
    def _dummy_tuid_exists(self, file_name, rev):
        # None means there is no entry.
        return None != self.conn.get_one("select 1 from temporal where file=? and revision=? and line=?",
                                         (quote_value(file_name), quote_value(rev), 0))


    # True if dummy, false if not.
    def _dummy_annotate_exists(self, file_name, rev):
        # None means there is no entry.
        return None != self.conn.get_one("select 1 from annotations where file=? and revision=? and annotation=?",
                                         (quote_value(file_name), quote_value(rev), quote_value('')))


    # Inserts a dummy tuid: (-1,rev,file_name,0)
    def insert_tuid_dummy(self, rev, file_name):
        if not self._dummy_tuid_exists(file_name, rev):
            self.conn.execute(
                "INSERT INTO temporal (tuid, revision, file, line) VALUES (?, ?, ?, ?)",
                (-1, quote_value(rev[:12]), quote_value(file_name), 0)
            )
            self.conn.commit()
        return [(-1,0)]


    # Inserts annotation dummy: (rev, '')
    def insert_annotate_dummy(self, rev, file_name):
        if not self._dummy_annotate_exists(file_name, rev):
            self.conn.execute(
                "INSERT INTO annotations (revision, file, annotation) VALUES (?, ?, ?)",
                (quote_value(rev[:12]), quote_value(file_name), quote_value(''))
            )
            self.conn.commit()
        return [(rev[:12],file_name,'')]


    # Returns annotation for this file at the given revision.
    def _get_annotation(self, rev, file):
        return self.conn.get_one(GET_ANNOTATION_QUERY, (quote_value(rev), quote_value(file)))


    def _get_one_tuid(self, cset, path, line):
        return self.conn.get_one("select 1 from temporal where revision=? and file=? and line=?",
                                 (cset, path, int(line)))


    def _get_latest_revision(self, file):
        return self.conn.get_one(GET_LATEST_MODIFICATION, (file,))


    def _get_past_file_revisions(self, file):
        tmp_result = self.conn.get_one(GET_PAST_MODIFICATIONS, (file,))
        if tmp_result:
            return [entry.replace("'", "") for entry in tmp_result[0].split(',')]
        return None


    def stringify_pastrevs(self, pastrevs):
        return ",".join(pastrevs)

    def stringify_tuids(self, tuid_list):
        return "\n".join([','.join([str(ent) for ent in x]) for x in tuid_list])


    def destringify_tuids(self, tuids_string):
        lines = str(tuids_string[0]).splitlines()
        line_origins = []
        for line in lines:
            entry = line.split(',')
            line_origins.append((int(entry[0].replace("'", "")), int(entry[1].replace("'", ""))))
        return line_origins


    # Gets the TUIDs for the files modified by a revision.
    def get_tuids_from_revision(self, revision):
        result = []
        URL_TO_FILES = 'https://hg.mozilla.org/' + self.config['hg']['branch'] + '/json-info/' + revision
        try:
            mozobject = http.get_json(url=URL_TO_FILES, retry=RETRY)
        except Exception as e:
            Log.warning("Unexpected error trying to get file list for revision {{revision}}", cause=e)
            return None

        files = mozobject[revision]['files']
        total = len(files)

        for count, file in enumerate(files):
            Log.note("{{file}} {{percent|percent(decimal=0)}}", file=file, percent=count / total)
            tmp_res = self.get_tuids(file, revision)
            if tmp_res:
                result.append((file, tmp_res))
            else:
                Log.note("Error occured for file {{file}} in revision {{revision}}", file=file, revision=revision)
                result.append((file, [(-1,0)]))
        return result


    def get_tuids_from_files(self, files, revision):
        """
        Gets the TUIDs for a set of files, at a given revision.
        list(tuids) is an array of tuids, one tuid for each line, in order, and `null` if no tuid assigned

        Uses frontier updating to build and maintain the tuids for
        the given set of files. Use changelog to determine what revisions
        to process and get the files that need to be updated by looking
        at the diffs. If the latestFileMod table is empty, for any file,
        we perform an annotation-based update.

        :param files: list of files
        :param revision:
        :return: generator of (file, list(tuids)) tuples
        """
        result = []
        revision = revision[:12]
        files = [file.lstrip('/') for file in files]
        frontier_update_list = []

        # TODO: Do this in a single SQL call to database
        total = len(files)
        for count, file in enumerate(files):
            if self.DEBUG:
                Log.note(" {{percent|percent(decimal=0)}}|{{file}}", file=file, percent=count / total)

            latest_rev = self._get_latest_revision(file)
            past_revisions = self._get_past_file_revisions(file)

            already_collected = False
            if past_revisions and revision in past_revisions:
                already_collected = True

            if (latest_rev and latest_rev[0] != revision) and not already_collected:
                if self.DEBUG:
                    Log.note("Will update frontier for file {{file}}.", file=file)
                frontier_update_list.append((file, latest_rev[0]))
            else:
                tmp_res = self.get_tuids(file, revision)
                if tmp_res:
                    result.append((file, tmp_res))
                else:
                    Log.note("Error occured for file " + file + " in revision " + revision)
                    result.append((file, [(-1, 0)]))

                # If this file has not been seen before,
                # add it to the latest modifications, else
                # it's already in there update it with past
                # revisions.
                if not latest_rev:
                    self.conn.execute("""INSERT INTO latestFileMod (file, revision, pastRevisions) VALUES (?,?,?)""",
                                      (file, revision, ''))
                    self.conn.commit()
                else:
                    if not past_revisions:
                        past_revisions = []
                    past_revisions.append(latest_rev[0])
                    self.conn.execute("""UPDATE latestFileMod SET pastRevisions=? WHERE file=?""",
                                      (self.stringify_pastrevs(past_revisions), file))
                    self.conn.commit()

        # If we have files that need to have their frontier updated
        if len(frontier_update_list) > 0:
            tmp = self._update_file_frontiers(frontier_update_list,revision)
            result.extend(tmp)
        return result


    # Update the frontier for all given files,
    # up to the given revision.
    #
    # Built for quick continuous _forward_ updating of large sets
    # of files of TUIDs. Backward updating should be done through
    # get_tuids(file, revision). If we cannot find a frontier, we will
    # stop looking after max_csets_proc and update all files at the given
    # revision.
    #
    def _update_file_frontiers(self, frontier_list, revision, max_csets_proc=10):
        # Get the changelogs and revisions until we find the
        # last one we've seen, and get the modified files in
        # each one.

        # Holds the files modified up to the last frontiers.
        files_to_process = {}

        # Holds all known frontiers
        latest_csets = {cset: True for cset in list(set([rev for (file,rev) in frontier_list]))}
        found_last_frontier = False
        if len(latest_csets) <= 1 and frontier_list[0][1] == revision:
            found_last_frontier = True

        final_rev = revision  # Revision we are searching from
        csets_proced = 0
        if self.DEBUG:
            Log.note("Searching for the following frontiers: {{csets}}", csets=str([cset for cset in latest_csets]))
        while not found_last_frontier:
            # Get a changelog
            clog_url = 'https://hg.mozilla.org/' + self.config['hg']['branch'] + '/json-log/' + final_rev
            try:
                Log.note("Searching through changelog {{url}}", url=clog_url)
                clog_obj = http.get_json(clog_url, retry=RETRY)
            except Exception as e:
                Log.error("Unexpected error getting changset-log for {{url}}", url=clog_url, error=e)

            # For each changeset/node
            still_looking = True
            for clog_cset in clog_obj['changesets']:
                cset_len12 = clog_cset['node'][:12]

                if still_looking:
                    # If there are still frontiers left to explore,
                    # add the files this node modifies to the processing list.
                    info_url = 'https://hg.mozilla.org/' + self.config['hg']['branch'] + '/json-info?node=' + cset_len12
                    try:
                        info_obj = http.get_json(info_url, retry=RETRY)
                    except Exception as e:
                        Log.error("Unexpected error getting changed files: {{url}}", url=info_url, error=e)

                    # Add the files
                    for f_added in info_obj[cset_len12]['files']:
                        if f_added in files_to_process:
                            files_to_process[f_added].append(cset_len12)
                        else:
                            files_to_process[f_added] = [cset_len12]

                if cset_len12 in latest_csets:
                    # Found a frontier, remove it from search list.
                    latest_csets[cset_len12] = False
                    still_looking = any([latest_csets[cs] for cs in latest_csets])

            csets_proced += 1
            if not still_looking:
                # End searching
                found_last_frontier = True
            elif csets_proced >= max_csets_proc:
                # In this case, all files need to be updated to this revision to ensure
                # line ordering consistency (between past, and future) when a revision
                # that is in the past is asked for.
                found_last_frontier = True

                files_to_process = {f: revision for (f,r) in frontier_list}

            if not found_last_frontier:
                # Go to the next log page
                final_rev = clog_obj['changesets'][len(clog_obj['changesets'])-1]['node'][:12]

        # Process each file that needs it based on the
        # files_to_process list.
        result = []
        total = len(frontier_list)
        for count, file_n_rev in enumerate(frontier_list):
            file = file_n_rev[0]
            rev = file_n_rev[1]

            # If the file was modified, get it's newest
            # annotation and update the file.
            proc_rev = rev
            if file in files_to_process:
                proc_rev = revision
                Log.note("Frontier update: {{rev}}|{{file}} {{percent|percent(decimal=0)}}", file=file, rev=proc_rev, percent=count / total)

            tmp_res = self.get_tuids(file, proc_rev)
            if tmp_res:
                result.append((file, tmp_res))
                if proc_rev != revision:
                    # If the file hasn't changed up to this revision,
                    # reinsert all the lines with the same annotate.
                    insert_list = []
                    for (tuid, line) in tmp_res:
                        if not self._get_one_tuid(revision, file, line):
                            insert_list.append({'node': revision, 'abspath': file, 'targetline': line})

                    if len(insert_list) > 0:
                        self._update_file_changesets(insert_list)
                    if not self._get_annotation(revision, file):
                        annotate = self.destringify_tuids(self._get_annotation(rev, file))
                        self.conn.execute("INSERT INTO annotations (revision, file, annotation) VALUES (?,?,?)",
                                          (quote_value(revision), quote_value(file),
                                           quote_value(self.stringify_tuids(annotate))))
                        self.conn.commit()
            else:
                Log.note("Error occured for file {{file}} in revision {{revision}}", file=file, revision=proc_rev)
                result.append((file, [(-1,0)]))

            if csets_proced < max_csets_proc and not still_looking:
                self.conn.execute("""UPDATE latestFileMod SET revision=? WHERE file=?""", (revision, file))

            past_revisions = self._get_past_file_revisions(file)
            if past_revisions:
                past_revisions.append(rev)
            else:
                past_revisions = [rev]
            self.conn.execute("""UPDATE latestFileMod SET pastRevisions=? WHERE file=?""",
                              (self.stringify_pastrevs(past_revisions), file))
        self.conn.commit()

        return result

    # Inserts new lines from all changesets (this is all that is required).
    def _update_file_changesets(self, annotated_lines):
        count = 0
        total = len(annotated_lines)
        quickfill_list = []

        for anline in annotated_lines:
            count += 1
            cset = anline['node'][:12]
            #if self.DEBUG:
            #    Log.note("{{rev}}|{{file}} {{percent|percent(decimal=0)}}", file=anline['abspath'], rev=cset, percent=count / total)
            if not self._get_one_tuid(cset, anline['abspath'], int(anline['targetline'])):
                quickfill_list.append((cset, anline['abspath'], int(anline['targetline'])))
        self._quick_update_file_changeset(list(set(quickfill_list)))


    def _quick_update_file_changeset(self, qf_list):
        for i in qf_list:
            self.conn.execute(
                "INSERT INTO temporal (tuid, revision, file, line)" +
                " VALUES (?, ?, ?, ?)", (self.tuid(), i[0], i[1], i[2])
            )
        self.conn.commit()


    # Returns (TUID, line) tuples for a given file at a given revision.
    #
    # Uses json-annotate to find all lines in this revision, then it updates
    # the database with any missing revisions for the file changes listed
    # in annotate. Then, we use the information from annotate coupled with the
    # diff information that was inserted into the DB to return TUIDs. This way
    # we don't have to deal with child, parents, dates, etc..
    def get_tuids(self, file, revision):
        revision = revision[:12]
        file = file.lstrip('/')

        # Get annotated file (cannot get around using this).
        # Unfortunately, this also means we always have to
        # deal with a small network delay.
        url = 'https://hg.mozilla.org/' + self.config['hg']['branch'] + '/json-annotate/' + revision + "/" + file

        existing_tuids = {}
        tmp_tuids = []
        already_ann = self._get_annotation(revision, file)
        # If it's not defined, or there is a dummy record
        if not already_ann:
            if self.DEBUG:
                Log.note("HG: {{url}}", url=url)
            try:
                annotated_object = http.get_json(url, retry=RETRY)
                if isinstance(annotated_object, (text_type, str)):
                    Log.error("Annotated object does not exist.")
            except Exception as e:
                # If we can't get the annotated file, return dummy record.
                Log.warning("Error while obtaining annotated file for file {{file}} in revision {{revision}}", file=file, revision=revision, cause=e)
                Log.note("Inserting dummy entry...")
                self.insert_tuid_dummy(revision, file)
                self.insert_annotate_dummy(revision, file)
                return []

            # Gather all missing csets and the
            # corresponding lines.
            annotated_lines = []
            line_origins = []
            existing_tuids = {}
            for node in annotated_object['annotate']:
                cset_len12 = node['node'][:12]

                # If the cset is not in the database, process it
                #
                # Use the 'abspath' field to determine the current filename in
                # case it has changed.
                tuid_tmp = self.conn.get_one(GET_TUID_QUERY, (node['abspath'], cset_len12, int(node['targetline'])))
                if (not tuid_tmp):
                    annotated_lines.append(node)
                else:
                    existing_tuids[int(node['lineno'])] = tuid_tmp[0]
                # Used to gather TUIDs later
                line_origins.append((node['abspath'], cset_len12, int(node['targetline'])))

            # Update DB with any revisions found in annotated
            # object that are not in the DB.
            if len(annotated_lines) > 0:
                self._update_file_changesets(annotated_lines)
        elif len([[x for x in t.split(',')] for t in already_ann[0].splitlines()][0]) < 2:
            return []
        else:
            return self.destringify_tuids(already_ann)

        # Get the TUIDs for each line (can probably be optimized with a join)
        tuids = tmp_tuids
        for line_num in range(1, len(line_origins) + 1):
            if line_num in existing_tuids:
                tuids.append((existing_tuids[line_num], line_num))
                continue
            try:
                tuid_tmp = self.conn.get_one(GET_TUID_QUERY,
                                             line_origins[line_num - 1])

                # Return dummy line if we can't find the TUID for this entry
                # (likely because of an error from insertion).
                if tuid_tmp:
                    tuids.append((tuid_tmp[0], line_num))
                else:
                    tuids.append((-1, 0))
            except Exception as e:
                Log.note("Unexpected error searching {{cause}}", cause=e)

        if not already_ann:
            self.conn.execute("INSERT INTO annotations (revision, file, annotation) VALUES (?,?,?)",
                              (quote_value(revision), quote_value(file), quote_value(self.stringify_tuids(tuids))))
            self.conn.commit()

        return tuids