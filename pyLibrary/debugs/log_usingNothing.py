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

from pyLibrary.debugs.text_logs import TextLog
from pyLibrary.meta import use_settings


class TextLog_usingNothing(TextLog):

    @use_settings
    def __init__(self, settings=None):
        TextLog.__init__(self)

    def write(self, template, params):
        pass

    def stop(self):
        pass
