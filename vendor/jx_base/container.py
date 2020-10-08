# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from copy import copy

from mo_dots import Data, is_data, is_many, join_field, set_default, split_field, to_data
from mo_future import is_text
from mo_imports import expect
from mo_logs import Log
from jx_base.expressions import QueryOp

ListContainer, Cube, run = expect("ListContainer", "Cube", "run")

type2container = Data()
config = Data()   # config.default IS EXPECTED TO BE SET BEFORE CALLS ARE MADE


class Container(object):
    """
    CONTAINERS HOLD MULTIPLE INDICES AND CAN HANDLE
    GENERAL JSON QUERY EXPRESSIONS ON ITS CONTENTS
    METADATA FOR A Container IS CALLED A Namespace
    """
    @classmethod
    def new_instance(type, frum, schema=None):
        """
        Factory!
        """
        if isinstance(frum, Container):
            return frum
        elif isinstance(frum, Cube):
            return frum
        elif isinstance(frum, QueryOp):
            return run(frum)
        elif is_many(frum):
            return ListContainer(frum)
        elif is_text(frum):
            # USE DEFAULT STORAGE TO FIND Container
            if not config.default.settings:
                Log.error("expecting jx_base.container.config.default.settings to contain default elasticsearch connection info")

            settings = set_default(
                {
                    "index": join_field(split_field(frum)[:1:]),
                    "name": frum,
                },
                config.default.settings
            )
            settings.type = None  # WE DO NOT WANT TO INFLUENCE THE TYPE BECAUSE NONE IS IN THE frum STRING ANYWAY
            return type2container["elasticsearch"](settings)
        elif is_data(frum):
            frum = to_data(frum)
            if frum.type and type2container[frum.type]:
                return type2container[frum.type](frum.settings)
            elif frum["from"]:
                frum = copy(frum)
                frum["from"] = Container(frum["from"])
                return QueryOp.wrap(frum)
            else:
                Log.error("Do not know how to handle {{frum|json}}", frum=frum)
        else:
            Log.error("Do not know how to handle {{type}}", type=frum.__class__.__name__)

    def query(self, query):
        if query.frum != self:
            Log.error("not expected")
        raise NotImplementedError()

    def filter(self, where):
        return self.where(where)

    def where(self, where):
        _ = where
        raise NotImplementedError()

    def sort(self, sort):
        _ = sort
        raise NotImplementedError()

    def select(self, select):
        _ = select
        raise NotImplementedError()

    def window(self, window):
        raise NotImplementedError()

    def format(self, format):
        _ = format
        raise NotImplementedError()

    def get_columns(self, table_name):
        """
        USE THE frum TO DETERMINE THE COLUMNS
        """
        raise NotImplementedError()

    @property
    def schema(self):
        raise NotImplementedError()
