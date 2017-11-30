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

from email.header import UTF8
from io import TextIOWrapper

import sys

from mo_future import text_type

from mo_logs.log_usingNothing import StructuredLogger
from mo_logs.strings import expand_template


class StructuredLogger_usingStream(StructuredLogger):
    def __init__(self, stream):
        assert stream
        if stream in (sys.stdout, sys.stderr):
            self.writer = lambda v: stream.write(v.encode('utf8'))
        elif hasattr(stream, 'encoding'):
            self.writer = lambda v: stream.write(v.encode('utf8'))
        else:
            self.writer = stream.write

    def write(self, template, params):
        value = expand_template(template, params)
        self.writer(value + "\n")

    def stop(self):
        pass

