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

from boto.ses import connect_to_region

from pyLibrary.debugs.exceptions import ALARM, NOTE
from pyLibrary.debugs.logs import Log
from pyLibrary.debugs.text_logs import TextLog
from pyDots import listwrap, unwrap
from pyLibrary.meta import use_settings
from pyLibrary.strings import expand_template
from pyLibrary.thread.threads import Lock
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import HOUR, MINUTE

WAIT_TO_SEND_MORE = HOUR


class TextLog_usingSES(TextLog):

    @use_settings
    def __init__(
        self,
        from_address,
        to_address,
        subject,
        region,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        log_type="ses",
        settings=None
    ):
        assert settings.log_type == "ses", "Expecing settings to be of type 'ses'"
        self.settings = settings
        self.accumulation = []
        self.next_send = Date.now() + MINUTE
        self.locker = Lock()

    def write(self, template, params):
        with self.locker:
            if params.context not in [NOTE, ALARM]:  # SEND ONLY THE NOT BORING STUFF
                self.accumulation.append(expand_template(template, params))

            if Date.now() > self.next_send:
                self._send_email()

    def stop(self):
        with self.locker:
            self._send_email()

    def _send_email(self):
        try:
            if self.accumulation:
                conn = connect_to_region(
                    self.settings.region,
                    aws_access_key_id=unwrap(self.settings.aws_access_key_id),
                    aws_secret_access_key=unwrap(self.settings.aws_secret_access_key)
                )

                conn.send_email(
                    source=self.settings.from_address,
                    to_addresses=listwrap(self.settings.to_address),
                    subject=self.settings.subject,
                    body="\n\n".join(self.accumulation),
                    format="text"
                )

                conn.close()
            self.next_send = Date.now() + WAIT_TO_SEND_MORE
            self.accumulation = []
        except Exception, e:
            self.next_send = Date.now() + WAIT_TO_SEND_MORE
            Log.warning("Could not send", e)



