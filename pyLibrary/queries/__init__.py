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
from collections import Mapping

from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap, set_default, split_field, join_field
from pyLibrary.dot.dicts import Dict
from pyLibrary.queries import containers

type2container = Dict()
config = Dict()   # config.default IS EXPECTED TO BE SET BEFORE CALLS ARE MADE
_ListContainer = None

def _delayed_imports():
    global type2container
    global _ListContainer

    from pyLibrary.queries.containers.lists import ListContainer as _ListContainer
    _ = _ListContainer

    try:
        from pyLibrary.queries.jx_usingMySQL import MySQL as _MySQL
    except Exception:
        _MySQL = None

    from pyLibrary.queries.jx_usingES import FromES as _FromES
    from pyLibrary.queries.meta import FromESMetadata as _FromESMetadata

    set_default(type2container, {
        "elasticsearch": _FromES,
        "mysql": _MySQL,
        "memory": None,
        "meta": _FromESMetadata
    })


def wrap_from(frum, schema=None):
    """
    :param frum:
    :param schema:
    :return:
    """
    if not type2container:
        _delayed_imports()

    frum = wrap(frum)

    if isinstance(frum, basestring):
        if not containers.config.default.settings:
            Log.error("expecting pyLibrary.queries.query.config.default.settings to contain default elasticsearch connection info")

        type_ = None
        index = frum
        if frum.startswith("meta."):
            from pyLibrary.queries.meta import FromESMetadata

            if frum == "meta.columns":
                return meta.singlton.columns
            elif frum == "meta.table":
                return meta.singlton.tables
            else:
                Log.error("{{name}} not a recognized table", name=frum)
        else:
            type_ = containers.config.default.type
            index = join_field(split_field(frum)[:1:])

        settings = set_default(
            {
                "index": index,
                "name": frum
            },
            containers.config.default.settings
        )
        settings.type = None
        return type2container[type_](settings)
    elif isinstance(frum, Mapping) and frum.type and type2container[frum.type]:
        # TODO: Ensure the frum.name is set, so we capture the deep queries
        if not frum.type:
            Log.error("Expecting from clause to have a 'type' property")
        return type2container[frum.type](frum.settings)
    elif isinstance(frum, Mapping) and (frum["from"] or isinstance(frum["from"], (list, set))):
        from pyLibrary.queries.query import Query
        return Query(frum, schema=schema)
    elif isinstance(frum, (list, set)):
        return _ListContainer("test_list", frum)
    else:
        return frum



class Schema(object):

    def get_column(self, name, table):
        raise NotImplementedError()
