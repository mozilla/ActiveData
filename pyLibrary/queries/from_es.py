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

from pyLibrary.dot import wrap, Dict, set_default

type2container = Dict()
config = Dict()   # config.default IS EXPECTED TO BE SET BEFORE CALLS ARE MADE


def _delayed_imports():
    global type2container

    from pyLibrary.queries.db_query import DBQuery
    from pyLibrary.queries.es_query import ESQuery
    set_default(type2container, {
        "elasticsearch": ESQuery,
        "mysql": DBQuery,
        "memory": None
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
        settings = set_default({
            "index": frum,
            "name": frum,
        }, config.default)
        return type2container["elasticsearch"](settings)
    elif isinstance(frum, dict) and frum["type"] and type2container[frum["type"]]:
        # TODO: Ensure the frum.name is set, so we capture the deep queries
        return type2container[frum["type"]](frum)
    elif isinstance(frum, dict) and (frum["from"] or isinstance(frum["from"], (list, set))):
        from pyLibrary.queries.query import Query
        return Query(frum, schema=schema)
    else:
        return frum



