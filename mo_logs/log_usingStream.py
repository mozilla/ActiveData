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

from mo_logs.log_usingNothing import StructuredLogger
from mo_logs.strings import expand_template


class StructuredLogger_usingStream(StructuredLogger):
    def __init__(self, stream):
        assert stream
        self.stream = stream

    def write(self, template, params):
        value = expand_template(template, params)
        if isinstance(value, unicode):
            value = value.encode('utf8')
        self.stream.write(value + b"\n")

    def stop(self):
        pass

