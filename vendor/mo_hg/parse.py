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

from mo_dots import wrap
from mo_logs import Log, strings

MAX_CONTENT_LENGTH = 500  # SOME "lines" FOR CODE ARE REALLY TOO LONG

GET_DIFF = "{{location}}/rev/{{rev}}"
GET_FILE = "{{location}}/file/{{rev}}{{path}}"

HUNK_HEADER = re.compile(r"^-(\d+),(\d+) \+(\d+),(\d+) @@.*")
FILE_SEP = re.compile(r"^--- ", re.MULTILINE)
HUNK_SEP = re.compile(r"^@@ ", re.MULTILINE)

MOVE = {
    ' ': lambda c: (c[0]+1, c[1]+1),
    '\\': lambda c: c,  # FOR "\ no newline at end of file
    '+': lambda c: (c[0]+1, c[1]),
    '-': lambda c: (c[0], c[1]+1)
}
no_change = MOVE[' ']


def diff_to_json(unified_diff):
    """
    CONVERT UNIFIED DIFF TO EASY-TO-STORE JSON FORMAT
    :param unified_diff: text
    :return: JSON details
    """
    output = []
    files = FILE_SEP.split(unified_diff)[1:]
    for file_ in files:
        changes = []
        old_file_header, new_file_header, file_diff = file_.split("\n", 2)
        old_file_path = old_file_header[1:]  # eg old_file_header == "a/testing/marionette/harness/marionette_harness/tests/unit/unit-tests.ini"
        new_file_path = new_file_header[5:]  # eg new_file_header == "+++ b/tests/resources/example_file.py"

        c = 0, 0
        hunks = HUNK_SEP.split(file_diff)[1:]
        for hunk in hunks:
            line_diffs = hunk.split("\n")
            old_start, old_length, new_start, new_length = HUNK_HEADER.match(line_diffs[0]).groups()
            next_c = max(0, int(new_start)-1), max(0, int(old_start)-1)
            if next_c[0] - next_c[1] != c[0] - c[1]:
                Log.error("expecting a skew of {{skew}}", skew=next_c[0] - next_c[1])
            if c[0] > next_c[0]:
                Log.error("can not handle out-of-order diffs")
            while c[0] != next_c[0]:
                c = no_change(c)

            for line in line_diffs[1:]:
                if not line:
                    continue
                if (
                    line.startswith("new file mode") or
                    line.startswith("deleted file mode") or
                    line.startswith("index ") or
                    line.startswith("diff --git")
                ):
                    # HAPPENS AT THE TOP OF NEW FILES
                    # diff --git a/security/sandbox/linux/SandboxFilter.cpp b/security/sandbox/linux/SandboxFilter.cpp
                    # u'new file mode 100644'
                    # u'deleted file mode 100644'
                    # index a763e390731f5379ddf5fa77090550009a002d13..798826525491b3d762503a422b1481f140238d19
                    # GIT binary patch
                    # literal 30804
                    break
                d = line[0]
                if d == '+':
                    changes.append({"new": {"line": int(c[0]), "content": strings.limit(line[1:], MAX_CONTENT_LENGTH)}})
                elif d == '-':
                    changes.append({"old": {"line": int(c[1]), "content": strings.limit(line[1:], MAX_CONTENT_LENGTH)}})
                try:
                    c = MOVE[d](c)
                except Exception as e:
                    Log.warning("bad line {{line|quote}}", line=line, cause=e)

        output.append({
            "new": {"name": new_file_path},
            "old": {"name": old_file_path},
            "changes": changes
        })
    return wrap(output)
