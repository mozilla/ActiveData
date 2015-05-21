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
from pyLibrary.debugs.logs import BaseLog, Log

from pyLibrary.env.emailer import Emailer
from pyLibrary.meta import use_settings
from pyLibrary.strings import expand_template
from pyLibrary.thread.threads import Lock
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import Duration


WAIT_TO_SEND_MORE = Duration.HOUR



class Log_usingEmail(BaseLog):

    @use_settings
    def __init__(
        self,
        from_address,
        to_address,
        subject,
        host,
        username,
        password,
        port=465,
        use_ssl=1,
        log_type="email",
        settings=None
    ):
        """
        SEND WARNINGS AND ERRORS VIA EMAIL

        settings = {
            "log_type":"email",
            "from_address": "klahnakoski@mozilla.com",
            "to_address": "klahnakoski@mozilla.com",
            "subject": "Problem in Pulse Logger",
            "host": "mail.mozilla.com",
            "port": 465,
            "username": "username",
            "password": "password",
            "use_ssl": 1
        }

        """
        assert settings.log_type == "email", "Expecing settings to be of type 'email'"
        self.settings = settings
        self.accumulation = []
        self.last_sent = Date.now()-Duration.YEAR
        self.locker = Lock()

    def write(self, template, params):
        with self.locker:
            if params.params.warning.template or params.params.warning.template:
                self.accumulation.append(expand_template(template, params))

                if Date.now() > self.last_sent + WAIT_TO_SEND_MORE:
                    self._send_email()

    def stop(self):
        with self.locker:
            self._send_email()

    def _send_email(self):
        try:
            if self.accumulation:
                with Emailer(self.settings) as emailer:
                    emailer.send_email(
                        from_address=self.settings.from_address,
                        to_address=self.settings.to_address,
                        subject=self.settings.subject,
                        text_data="\n\n".join(self.accumulation)
                    )
            self.last_sent = Date.now()
            self.accumulation = []
        except Exception, e:
            self.last_sent = Date.now()
            Log.warning("Could not send", e)



