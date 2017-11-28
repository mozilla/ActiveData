# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from io import TextIOWrapper

from mo_future import text_type

from mo_logs.log_usingNothing import StructuredLogger
from mo_logs.strings import expand_template


class StructuredLogger_usingStream(StructuredLogger):
    def __init__(self, stream):
        assert stream
        if isinstance(stream, TextIOWrapper):
            self.writer = stream.write
        else:
            self.writer = lambda v: stream.write(v.encode('utf8'))

    def write(self, template, params):
        value = expand_template(template, params)
        self.writer(value + "\n")

    def stop(self):
        pass

