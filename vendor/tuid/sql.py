# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import sqlite3

class Sql:
    def __init__(self,dbname):
        self.db = sqlite3.connect(dbname)

    def execute(self,sql,params=None):
        if params:
            self.db.execute(sql,params)
        else:
            self.db.execute(sql)

    def commit(self):
        self.db.commit()

    def get(self,sql,params=None):
        if params:
            return self.db.execute(sql,params).fetchall()
        else:
            return self.db.execute(sql).fetchall()


    def get_one(self,sql,params=None):
        if params:
            return self.db.execute(sql,params).fetchone()
        else:
            return self.db.execute(sql).fetchone()

