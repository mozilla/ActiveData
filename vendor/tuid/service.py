# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import namedtuple

from mo_dots import Null, coalesce
from mo_future import text_type
from mo_kwargs import override
from mo_logs import Log

from mo_hg.hg_mozilla_org import HgMozillaOrg
from mo_hg.parse import diff_to_moves
from pyLibrary.env import http
from pyLibrary.sql import sql_list, sql_iso
from pyLibrary.sql.sqlite import quote_value
from tuid import sql

DEBUG = False
RETRY = {"times": 3, "sleep": 5}
SQL_BATCH_SIZE = 500

GET_TUID_QUERY = "SELECT tuid FROM temporal WHERE file=? and revision=? and line=?"

GET_ANNOTATION_QUERY = "SELECT annotation FROM annotations WHERE revision=? and file=?"

GET_LATEST_MODIFICATION = "SELECT revision FROM latestFileMod WHERE file=?"


class TUIDService:

    @override
    def __init__(self, database, hg, hg_cache, conn=None, kwargs=None):
        try:
            self.config = kwargs

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
        '''
        Creates all the tables, and indexes needed for the service.

        :return: None
        '''
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
            PRIMARY KEY(file)
        );''')

        self.conn.execute("CREATE UNIQUE INDEX temporal_rev_file ON temporal(revision, file, line)")
        self.conn.commit()
        Log.note("Tables created successfully")


    def _dummy_tuid_exists(self, file_name, rev):
        # True if dummy, false if not.
        # None means there is no entry.
        return None != self.conn.get_one("select 1 from temporal where file=? and revision=? and line=?",
                                         (file_name, rev, 0))


    def _dummy_annotate_exists(self, file_name, rev):
        # True if dummy, false if not.
        # None means there is no entry.
        return None != self.conn.get_one("select 1 from annotations where file=? and revision=? and annotation=?",
                                         (file_name, rev, ''))


    def insert_tuid_dummy(self, rev, file_name, commit=True):
        # Inserts a dummy tuid: (-1,rev,file_name,0)
        if not self._dummy_tuid_exists(file_name, rev):
            self.conn.execute(
                "INSERT INTO temporal (tuid, revision, file, line) VALUES (?, ?, ?, ?)",
                (-1, rev[:12], file_name, 0)
            )
            if commit:
                self.conn.commit()
        return MISSING


    def insert_annotate_dummy(self, rev, file_name, commit=True):
        # Inserts annotation dummy: (rev, file, '')
        if not self._dummy_annotate_exists(file_name, rev):
            self.conn.execute(
                "INSERT INTO annotations (revision, file, annotation) VALUES (?, ?, ?)",
                (rev[:12], file_name, ''))
            if commit:
                self.conn.commit()
        return [(rev[:12], file_name, '')]


    def _get_annotation(self, rev, file):
        # Returns an annotation if it exists
        return self.conn.get_one(GET_ANNOTATION_QUERY, (rev, file))


    def _get_one_tuid(self, cset, path, line):
        # Returns a single TUID if it exists
        return self.conn.get_one("select 1 from temporal where revision=? and file=? and line=?",
                                 (cset, path, int(line)))


    def _get_latest_revision(self, file):
        # Returns the latest revision that we
        # have information on the requested file.
        return self.conn.get_one(GET_LATEST_MODIFICATION, (file,))


    def stringify_tuids(self, tuid_list):
        # Turns the TuidMap list to a string for storage in
        # the annotations table.
        return "\n".join([','.join([str(x.tuid), str(x.line)]) for x in tuid_list])


    def destringify_tuids(self, tuids_string):
        # Builds up TuidMap list from annotation cache entry.
        lines = str(tuids_string[0]).splitlines()
        line_origins = []
        for line in lines:
            entry = line.split(',')
            line_origins.append(TuidMap(int(entry[0].replace("'", "")), int(entry[1].replace("'", ""))))
        return line_origins


    def get_diff(self, cset):
        """
        Returns the diff for a given revision.

        :param cset: revision to get diff from
        :return: unified diff object from diff_to_moves
        """
        url = 'https://hg.mozilla.org/' + self.config['hg']['branch'] + '/raw-rev/' + cset
        if DEBUG:
            Log.note("HG: {{url}}", url=url)

        # Ensure we get the diff before continuing
        try:
            diff_object = http.get(url, retry=RETRY)
        except Exception as e:
            Log.error("Unexpected error while trying to get diff for: " + url  + " because of {{cause}}", cause=e)
            return None
        return diff_to_moves(str(diff_object.content.decode('utf8')))


    def get_tuids_from_revision(self, revision):
        """
        Gets the TUIDs for the files modified by a revision.

        :param revision: revision to get files from
        :return: list of (file, list(tuids)) tuples
        """
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
                result.append((file, []))
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

        This function assumes the newest file names are given, if they
        are not, then no TUIDs are returned for that file.

        :param files: list of files
        :param revision: revision to get files at
        :return: list of (file, list(tuids)) tuples
        """
        result = []
        revision = revision[:12]
        files = [file.lstrip('/') for file in files]
        frontier_update_list = []

        total = len(files)
        latestFileMod_inserts = {}

        with self.conn.transaction():
            for count, file in enumerate(files):
                # Go through all requested files and
                # either update their frontier or add
                # them to the DB through an initial annotation.

                if DEBUG:
                    Log.note(" {{percent|percent(decimal=0)}}|{{file}}", file=file, percent=count / total)

                latest_rev = self._get_latest_revision(file)

                # Check if the file has already been collected at
                # this revision and get the result if so
                already_ann = self._get_annotation(revision, file)
                if already_ann:
                    result.append((file,self.destringify_tuids(already_ann)))
                    continue
                elif already_ann[0] == '':
                    result.append((file,[]))
                    continue

                if (latest_rev and latest_rev[0] != revision):
                    # File has a frontier, let's update it
                    if DEBUG:
                        Log.note("Will update frontier for file {{file}}.", file=file)
                    frontier_update_list.append((file, latest_rev[0]))
                else:
                    # File has never been seen before, get it's initial
                    # annotation to work from in the future.
                    tmp_res = self.get_tuids(file, revision, commit=False)
                    if tmp_res:
                        result.append((file, tmp_res))
                    else:
                        Log.note("Error occured for file " + file + " in revision " + revision)
                        result.append((file, []))

                    # If this file has not been seen before,
                    # add it to the latest modifications, else
                    # it's already in there so update its past
                    # revisions.
                    latestFileMod_inserts[file] = (file, revision)

            # If we have files that need to have their frontier updated
            if len(frontier_update_list) > 0:
                tmp = self._update_file_frontiers(frontier_update_list,revision)
                result.extend(tmp)

            if len(latestFileMod_inserts) > 0:
                count = 0
                listed_inserts = [latestFileMod_inserts[i] for i in latestFileMod_inserts]
                while count < len(listed_inserts):
                    inserts_list = listed_inserts[count:count + SQL_BATCH_SIZE]
                    count += SQL_BATCH_SIZE
                    self.conn.execute(
                        "INSERT OR REPLACE INTO latestFileMod (file, revision) VALUES " +
                        sql_list(
                            sql_iso(sql_list(map(quote_value, i)))
                            for i in inserts_list
                        )
                    )

        return result


    def _apply_diff(self, annotation, diff, cset, file):
        '''
        Using an annotation ([(tuid,line)] - array
        of TuidMap objects), we change the line numbers to
        reflect a given diff and return them. diff must
        be a diff object returned from get_diff(cset, file).
        Only for going forward in time, not back.

        :param annotation: list of TuidMap objects
        :param diff: unified diff from get_diff
        :param cset: revision to apply diff at
        :param file: name of file diff is applied to
        :return:
        '''
        # Add all added lines into the DB.
        list_to_insert = []
        new_ann = [x for x in annotation]
        new_ann.sort(key=lambda x: x.line)

        def add_one(tl_tuple, lines):
            start = tl_tuple.line
            return lines[:start - 1] + [tl_tuple] + [TuidMap(tmap.tuid, int(tmap.line) + 1) for tmap in lines[start - 1:]]

        def remove_one(start, lines):
            return lines[:start - 2] + [TuidMap(tmap.tuid, int(tmap.line) - 1) for tmap in lines[start:]]

        for f_proc in diff:
            if f_proc['new'].name.lstrip('/') != file:
                continue

            f_diff = f_proc['changes']
            for change in f_diff:
                if change.action == '+':
                    new_tuid = self.tuid()
                    new_ann = add_one(TuidMap(new_tuid, change.line+1), new_ann)
                    list_to_insert.append((new_tuid, cset, file, change.line+1))
                elif change.action == '-':
                    new_ann = remove_one(change.line+1, new_ann)
            break # Found the file, exit searching

        if len(list_to_insert) > 0:
            count = 0
            while count < len(list_to_insert):
                inserts_list = list_to_insert[count:count + SQL_BATCH_SIZE]
                count += SQL_BATCH_SIZE
                self.conn.execute(
                    "INSERT INTO temporal (tuid, revision, file, line)" +
                    " VALUES " +
                    sql_list(sql_iso(sql_list(map(quote_value, tp))) for tp in inserts_list)
                )

        return new_ann



    #
    def _update_file_frontiers(self, frontier_list, revision, max_csets_proc=10):
        '''
        Update the frontier for all given files, up to the given revision.

        Built for quick continuous _forward_ updating of large sets
        of files of TUIDs. Backward updating should be done through
        get_tuids(file, revision). If we cannot find a frontier, we will
        stop looking after max_csets_proc and update all files at the given
        revision.

        :param frontier_list: list of files to update
        :param revision: revision to update files to
        :param max_csets_proc: maximum number of changeset logs to look through
                               to find past frontiers.
        :return: list of (file, list(tuids)) tuples
        '''

        # Get the changelogs and revisions until we find the
        # last one we've seen, and get the modified files in
        # each one.

        # Holds the files modified up to the last frontiers.
        files_to_process = {}

        # Holds all known frontiers
        latest_csets = {cset: True for cset in list(set([rev for (file,rev) in frontier_list]))}
        file_to_frontier = {tp[0]: tp[1] for tp in frontier_list}
        found_last_frontier = False
        if len(latest_csets) <= 1 and frontier_list[0][1] == revision:
            # If the latest revision is the requested revision,
            # continue to the tuid querys.
            found_last_frontier = True

        final_rev = revision  # Revision we are searching from
        csets_proced = 0
        diffs_cache = {}
        removed_files = {}
        if DEBUG:
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
                    if cset_len12 in latest_csets:
                        # Found a frontier, remove it from search list.
                        latest_csets[cset_len12] = False
                        still_looking = any([latest_csets[cs] for cs in latest_csets])

                        if not still_looking:
                            break

                    # If there are still frontiers left to explore,
                    # add the files this node modifies to the processing list.
                    parsed_diff = self.get_diff(cset_len12)

                    for f_added in parsed_diff:
                        # Get new entries for removed files.
                        new_name = f_added['new'].name.lstrip('/')
                        old_name = f_added['old'].name.lstrip('/')

                        # If we don't need this file, skip it
                        if new_name not in file_to_frontier:
                            # If the file was removed, set a
                            # flag and return no tuids later.
                            if new_name == 'dev/null':
                                removed_files[old_name] = True
                            continue

                        # At this point, file is in the database, and is
                        # asked to be processed, and we are still
                        # searching for the last frontier.

                        # If we are past the frontier for this file,
                        # or if we are at the frontier skip it.
                        if file_to_frontier[new_name] == '':
                            continue
                        if file_to_frontier[new_name] == cset_len12:
                            file_to_frontier[new_name] = ''
                            continue

                        # Skip diffs that change file names, this is the first
                        # annotate entry to the new file_name and it doesn't do
                        # anything to the old other than bring it to new.
                        # We should never make it to this point unless there was an error elsewhere
                        # because any frontier for the new_name file should be at this revision or
                        # further ahead - never earlier.
                        if old_name != new_name:
                            Log.error("Should not have made it here, can't find a frontier for {{file}}", file=new_name)

                        if new_name in files_to_process:
                            files_to_process[new_name].append(cset_len12)
                        else:
                            files_to_process[new_name] = [cset_len12]
                    diffs_cache[cset_len12] = parsed_diff

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
        ann_inserts = []
        latestFileMod_inserts = {}
        total = len(frontier_list)
        for count, file_n_rev in enumerate(frontier_list):
            file = file_n_rev[0]
            rev = file_n_rev[1]

            # If the file was modified, get it's newest
            # annotation and update the file.
            proc_rev = rev
            proc = False
            if file in files_to_process:
                proc = True
                proc_rev = revision
                Log.note("Frontier update: {{count}}/{{total}} - {{percent|percent(decimal=0)}} | {{rev}}|{{file}} ", count=count,
                                                total=total, file=file, rev=proc_rev, percent=count / total)

            if proc and file not in removed_files:
                # Process this file using the diffs found

                # Reverse the list, we always find the newest diff first
                csets_to_proc = files_to_process[file][::-1]
                old_ann = self.destringify_tuids(self._get_annotation(rev, file))

                # Apply all the diffs
                tmp_res = old_ann
                for i in csets_to_proc:
                    tmp_res = self._apply_diff(tmp_res, diffs_cache[i], i, file)

                ann_inserts.append((revision, file, self.stringify_tuids(tmp_res)))
            elif file not in removed_files:
                # File is new, or the name was changed - we need to create
                # a new initial entry for this file.
                tmp_res = self.get_tuids(file, proc_rev, commit=False)
            else:
                # File was removed
                tmp_res = None

            if tmp_res:
                result.append((file, tmp_res))
                if proc_rev != revision:
                    # If the file hasn't changed up to this revision,
                    # reinsert it with the same previous annotate.
                    if not self._get_annotation(revision, file):
                        annotate = self.destringify_tuids(self._get_annotation(rev, file))
                        ann_inserts.append((revision, file, self.stringify_tuids(annotate)))
            else:
                Log.note("Error occured for file {{file}} in revision {{revision}}", file=file, revision=proc_rev)
                ann_inserts.append((revision, file, ''))
                result.append((file, []))

            latest_rev = rev
            if csets_proced < max_csets_proc and not still_looking:
                # If we have found all frontiers, update to the
                # latest revision. Otherwise, the requested
                # revision is too far away (can't be sure
                # if it's past).
                latest_rev = revision

            # Get any past revisions, and include the previous
            # latest in it.
            latestFileMod_inserts[file] = (file, latest_rev)

        if len(latestFileMod_inserts) > 0:
            count = 0
            listed_inserts = [latestFileMod_inserts[i] for i in latestFileMod_inserts]
            while count < len(listed_inserts):
                tmp_inserts = listed_inserts[count:count + SQL_BATCH_SIZE]
                count += SQL_BATCH_SIZE
                self.conn.execute(
                    "INSERT OR REPLACE INTO latestFileMod (file, revision) VALUES " +
                    sql_list(sql_iso(sql_list(map(quote_value, i))) for i in tmp_inserts)
                )

        if len(ann_inserts) > 0:
            count = 0
            while count < len(ann_inserts):
                tmp_inserts = ann_inserts[count:count + SQL_BATCH_SIZE]
                count += SQL_BATCH_SIZE
                self.conn.execute(
                    "INSERT INTO annotations (revision, file, annotation) VALUES " +
                    sql_list(sql_iso(sql_list(map(quote_value, i))) for i in tmp_inserts)
                )

        return result


    def _update_file_changesets(self, annotated_lines):
        '''
        Inserts new lines from all changesets in the given annotation.

        :param annotated_lines: Response from annotation request from HGMO
        :return: None
        '''
        quickfill_list = []

        for anline in annotated_lines:
            cset = anline['node'][:12]
            if not self._get_one_tuid(cset, anline['abspath'], int(anline['targetline'])):
                quickfill_list.append((cset, anline['abspath'], int(anline['targetline'])))
        self._quick_update_file_changeset(list(set(quickfill_list)))


    def _quick_update_file_changeset(self, qf_list):
        '''
        Updates temporal table to include any new TUIDs.

        :param qf_list: List to insert
        :return: None
        '''
        count = 0
        while count < len(qf_list):
            tmp_qf_list = qf_list[count:count+SQL_BATCH_SIZE]
            count += SQL_BATCH_SIZE
            self.conn.execute(
                "INSERT INTO temporal (tuid, revision, file, line)" +
                " VALUES " +
                sql_list(sql_iso(sql_list(map(quote_value, (self.tuid(), i[0], i[1], i[2])))) for i in tmp_qf_list)
            )


    def get_tuids(self, file, revision, commit=True):
        '''
        Returns (TUID, line) tuples for a given file at a given revision.

        Uses json-annotate to find all lines in this revision, then it updates
        the database with any missing revisions for the file changes listed
        in annotate. Then, we use the information from annotate coupled with the
        diff information that was inserted into the DB to return TUIDs. This way
        we don't have to deal with child, parents, dates, etc..

        :param file: name of file to get
        :param revision: revision at which to get the file
        :param commit: True to commit new TUIDs else False
        :return: List of TuidMap objects
        '''
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
            if DEBUG:
                Log.note("HG: {{url}}", url=url)
            try:
                annotated_object = http.get_json(url, retry=RETRY)
                if isinstance(annotated_object, (text_type, str)):
                    Log.error("Annotated object does not exist.")
            except Exception as e:
                # If we can't get the annotated file, return dummy record.
                Log.warning("Error while obtaining annotated file for file {{file}} in revision {{revision}}", file=file, revision=revision, cause=e)
                Log.note("Inserting dummy entry...")
                self.insert_tuid_dummy(revision, file, commit=commit)
                self.insert_annotate_dummy(revision, file, commit=commit)
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
                # If we are using get_tuids within another transaction
                if not commit:
                    self._update_file_changesets(annotated_lines)
                else:
                    with self.conn.transaction():
                        self._update_file_changesets(annotated_lines)
        elif already_ann[0] == '':
            return []
        else:
            return self.destringify_tuids(already_ann)

        # Get the TUIDs for each line (can probably be optimized with a join)
        tuids = tmp_tuids
        for line_num in range(1, len(line_origins) + 1):
            if line_num in existing_tuids:
                tuids.append(TuidMap(existing_tuids[line_num], line_num))
                continue
            try:
                tuid_tmp = self.conn.get_one(GET_TUID_QUERY,
                                             line_origins[line_num - 1])

                # Return dummy line if we can't find the TUID for this entry
                # (likely because of an error from insertion).
                if tuid_tmp:
                    tuids.append(TuidMap(tuid_tmp[0], line_num))
                else:
                    tuids.append(MISSING)
            except Exception as e:
                Log.note("Unexpected error searching {{cause}}", cause=e)

        if not already_ann:
            self.conn.execute(
                "INSERT INTO annotations (revision, file, annotation) VALUES (?,?,?)",
                (
                    revision,
                    file,
                    self.stringify_tuids(tuids)
                )
            )

            if commit:
                self.conn.commit()

        return tuids


# Used for increasing readability
# Can be accessed with tmap_obj.line, tmap_obj.tuid
TuidMap = namedtuple(str("TuidMap"), [str("tuid"), str("line")])
MISSING = TuidMap(-1, 0)
