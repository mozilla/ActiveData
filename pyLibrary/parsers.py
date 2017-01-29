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

from urlparse import urlparse

from pyDots import wrap

_convert = None
_Log = None


def _late_import():
    global _convert
    global _Log
    from pyLibrary import convert as _convert
    from mo_logs import Log as _Log
    _ = _convert
    _ = _Log

