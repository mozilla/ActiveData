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
from pyLibrary.testing.fuzzytestcase import FuzzyTestCase
from pyLibrary.times.dates import parse, Date
from pyLibrary.times.durations import Duration, WEEK, MONTH, DAY


class TestTimeParser(FuzzyTestCase):
    def test_now(self):
        self.assertAlmostEqual(parse("now").milli, Date.now().milli, places=12)  # IGNORE THE LEAST SIGNIFICANT MILLISECOND

    def test_today(self):
        self.assertAlmostEqual(parse("today").milli, Date.today().milli)

    def test_yesterday(self):
        self.assertAlmostEqual(parse("today-day").milli, (Date.today() - DAY).milli)

    def test_last_week(self):
        self.assertAlmostEqual(parse("today-7day").milli, (Date.today() - DAY * 7).milli)

    def test_next_week(self):
        self.assertAlmostEqual(parse("today+7day").milli, (Date.today() + DAY * 7).milli)

    def test_week_before(self):
        self.assertAlmostEqual(parse("today-2week").milli, (Date.today() - WEEK * 2).milli)

    def test_last_year(self):
        self.assertAlmostEqual(parse("today-12month").milli, (Date.today() - MONTH * 12).milli)

    def test_beginning_of_month(self):
        self.assertAlmostEqual(parse("today|month").milli, Date.today().floor(MONTH).milli)

    def test_end_of_month(self):
        self.assertAlmostEqual(parse("today|month+month").milli, Date.today().floor(MONTH).add(MONTH).milli)

    def test_13_weeks(self):
        self.assertAlmostEqual(parse("13week").milli, (WEEK * 13).milli)

    def test_bad_floor(self):
        self.assertRaises(Exception, parse, "today - week|week")
