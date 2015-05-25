# encoding: utf-8
#
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

from pyLibrary.env.processes import Process


def get_git_revision():
    """
    GET THE CURRENT GIT REVISION
    """
    proc = Process(["git", "log", "-1"])

    try:
        while True:
            line = proc.readline().strip()
            if not line:
                continue
            if line.startswith("commit "):
                return line[7:]
    finally:
        try:
            proc.join()
        except Exception:
            pass
