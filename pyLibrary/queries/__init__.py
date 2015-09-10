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
from pyLibrary.dot import wrap, set_default, split_field
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

    from pyLibrary.queries.qb_usingMySQL import MySQL
    from pyLibrary.queries.qb_usingES import FromES
    from pyLibrary.queries.meta import FromESMetadata

    set_default(type2container, {
        "elasticsearch": FromES,
        "mysql": MySQL,
        "memory": None,
        "meta": FromESMetadata
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
            type_ = "meta"
        else:
            type_ = containers.config.default.type
            index = split_field(frum)[0]

        settings = set_default(
            {
                "index": index,
                "name": frum
            },
            containers.config.default.settings
        )
        return type2container[type_](settings)
    elif isinstance(frum, Mapping) and frum.type and type2container[frum.type]:
        # TODO: Ensure the frum.name is set, so we capture the deep queries
        if not frum.type:
            Log.error("Expecting from clause to have a 'type' property")
        return type2container[frum.type](frum.settings)
    elif isinstance(frum, Mapping) and (frum["from"] or isinstance(frum["from"], (list, set))):
        from pyLibrary.queries.query import Query
        return Query(frum, schema=schema)
    elif isinstance(frum, list):
        return _ListContainer(frum)
    else:
        return frum



import es09.util
