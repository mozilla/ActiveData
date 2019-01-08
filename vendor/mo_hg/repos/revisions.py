# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

from mo_dots import Data
from mo_files import File


class Revision(Data):
    def __hash__(self):
        return hash((self.branch.name.lower(), self.changeset.id[:12]))

    def __eq__(self, other):
        if other == None:
            return False
        return (self.branch.name.lower(), self.changeset.id[:12]) == (other.branch.name.lower(), other.changeset.id[:12])


revision_schema = (File(__file__).parent / "revision_schema.json").read_json()
