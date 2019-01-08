# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

import flask
from flask import Response

from active_data import record_request
from active_data.actions import save_query
from jx_base import container
from jx_elasticsearch.meta import ElasticsearchMetadata
from jx_python.containers.list_usingPythonList import ListContainer
from mo_dots import coalesce, is_data, set_default, split_field
from mo_future import text_type
from mo_json import STRUCT, value2json
from mo_logs import Log, strings
from mo_logs.strings import expand_template, unicode2utf8
from mo_threads import Till
from mo_times.dates import Date
from mo_times.durations import MINUTE

DEBUG = True
QUERY_TOO_LARGE = "Query is too large"


def send_error(active_data_timer, body, e):
    status = 400

    if QUERY_TOO_LARGE in e:
        status = 413

    record_request(flask.request, None, body, e)
    Log.warning("Could not process\n{{body}}", body=body.decode("latin1"), cause=e)
    e = e.__data__()
    e.meta.timing.total = active_data_timer.duration.seconds

    # REMOVE TRACES, BECAUSE NICER TO HUMANS
    # def remove_trace(e):
    #     e.trace = e.trace[0:1:]
    #     for c in listwrap(e.cause):
    #         remove_trace(c)
    # remove_trace(e)

    return Response(
        unicode2utf8(value2json(e)),
        status=status
    )


def replace_vars(text, params=None):
    """
    REPLACE {{vars}} WITH ENVIRONMENTAL VALUES
    """
    start = 0
    var = strings.between(text, "{{", "}}", start)
    while var:
        replace = "{{" + var + "}}"
        index = text.find(replace, 0)
        if index==-1:
            Log.error("could not find {{var}} (including quotes)", var=replace)
        end = index + len(replace)

        try:
            replacement = text_type(Date(var).unix)
            text = text[:index] + replacement + text[end:]
            start = index + len(replacement)
        except Exception as _:
            start += 1

        var = strings.between(text, "{{", "}}", start)

    text = expand_template(text, coalesce(params, {}))
    return text


def test_mode_wait(query):
    """
    WAIT FOR METADATA TO ARRIVE ON INDEX
    :param query: dict() OF REQUEST BODY
    :return: nothing
    """

    if not query["from"]:
        return

    try:
        if query["from"].startswith("meta."):
            return

        now = Date.now()
        alias = split_field(query["from"])[0]
        metadata_manager = find_container(alias).namespace
        metadata_manager.meta.tables[alias].timestamp = now  # TRIGGER A METADATA RELOAD AFTER THIS TIME

        timeout = Till(seconds=MINUTE.seconds)
        while not timeout:
            # GET FRESH VERSIONS
            cols = [c for c in metadata_manager.get_columns(table_name=alias, after=now, timeout=timeout) if c.jx_type not in STRUCT]
            for c in cols:
                if now >= c.last_updated:
                    Log.note(
                        "wait for column (table={{col.es_index}}, name={{col.es_column}}) metadata to arrive",
                        col=c
                    )
                    break
            else:
                break
            Till(seconds=1).wait()
    except Exception as e:
        Log.warning("could not pickup columns", cause=e)


namespace = None

# TODO: The container cache is a hack until a global namespace/container is built
container_cache = {}  # MAP NAME TO Container OBJECT


def find_container(frum):
    """
    :param frum:
    :return:
    """
    global namespace
    if not namespace:
        if not container.config.default.settings:
            Log.error("expecting jx_base.container.config.default.settings to contain default elasticsearch connection info")
        namespace = ElasticsearchMetadata(container.config.default.settings)

    if isinstance(frum, text_type):
        if frum in container_cache:
            return container_cache[frum]

        path = split_field(frum)
        if path[0] == "meta":
            if path[1] == "columns":
                return namespace.meta.columns.denormalized()
            elif path[1] == "tables":
                return namespace.meta.tables
            else:
                Log.error("{{name}} not a recognized table", name=frum)

        type_ = container.config.default.type
        fact_table_name = path[0]

        settings = set_default(
            {
                "index": fact_table_name,
                "name": frum,
                "exists": True,
            },
            container.config.default.settings
        )
        settings.type = None
        output = container.type2container[type_](settings)
        container_cache[frum] = output
        return output
    elif is_data(frum) and frum.type and container.type2container[frum.type]:
        # TODO: Ensure the frum.name is set, so we capture the deep queries
        if not frum.type:
            Log.error("Expecting from clause to have a 'type' property")
        return container.type2container[frum.type](frum.settings)
    elif is_data(frum) and (frum["from"] or isinstance(frum["from"], (list, set))):
        from jx_base.query import QueryOp
        return QueryOp.wrap(frum)
    elif isinstance(frum, (list, set)):
        return ListContainer("test_list", frum)
    else:
        return frum


