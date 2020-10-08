# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

import flask
from flask import Response

from active_data import record_request
from active_data.actions import save_query
from jx_base import container
from jx_elasticsearch import meta
from jx_elasticsearch.meta import ElasticsearchMetadata
from jx_python.containers.list import ListContainer
from mo_dots import is_container, join_field
from mo_dots import is_data, set_default, split_field
from mo_future import is_text, first
from mo_json import INTERNAL, value2json
from mo_logs import Log
from mo_threads import Till
from mo_times import Timer
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

    return Response(value2json(e).encode("utf8"), status=status)


def test_mode_wait(query, please_stop):
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

        alias = split_field(query["from"])[0]
        after = Date.now()
        require_cardinality = meta.ENABLE_META_SCAN
        with Timer(
            "Get columns for {{table}} after {{after}}",
            {"table": alias, "after": after},
            verbose=DEBUG,
        ):
            metadata_manager = find_container(alias, after=after).namespace

            timeout = Till(seconds=MINUTE.seconds) | please_stop
            while not timeout:
                # GET FRESH VERSIONS
                cols = metadata_manager.get_columns(
                    table_name=alias, after=after, timeout=timeout
                )
                not_ready = [
                    c
                    for c in cols
                    if c.jx_type not in INTERNAL
                    and (
                        after >= c.last_updated
                        or (require_cardinality and c.cardinality == None)
                    )
                ]
                if not_ready:
                    Log.note(
                        "wait for column (table={{col.es_index}},"
                        " name={{col.es_column}}, cardinality={{col.cardinality|json}},"
                        " last_updated={{col.last_updated|datetime}}) metadata to"
                        " arrive",
                        col=first(not_ready),
                    )
                else:
                    break
                Till(seconds=1).wait()
    except Exception as e:
        Log.warning("could not pickup columns", cause=e)


namespace = None

# TODO: The container cache is a hack until a global namespace/container is built
container_cache = {}  # MAP NAME TO Container OBJECT


def find_container(frum, after):
    """
    :param frum:
    :return:
    """
    global namespace
    if not namespace:
        if not container.config.default.settings:
            Log.error(
                "expecting jx_base.container.config.default.settings to contain default"
                " elasticsearch connection info"
            )
        namespace = ElasticsearchMetadata(container.config.default.settings)
    if not frum:
        Log.error("expecting json query expression with from clause")

    # FORCE A RELOAD
    namespace.get_columns(frum, after=after)

    if is_text(frum):
        if frum in container_cache:
            return container_cache[frum]

        path = split_field(frum)
        if path[0] == "meta":
            if path[1] == "columns":
                return namespace.meta.columns.denormalized()
            elif path[1] == "tables":
                return namespace.meta.tables
            else:
                fact_table_name = join_field(path[:2])
        else:
            fact_table_name = path[0]

        type_ = container.config.default.type

        settings = set_default(
            {"alias": fact_table_name, "name": frum, "exists": True},
            container.config.default.settings,
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
    elif is_data(frum) and (frum["from"] or is_container(frum["from"])):
        from jx_base.expressions import QueryOp

        return QueryOp.wrap(frum)
    elif is_container(frum):
        return ListContainer("test_list", frum)
    else:
        return frum
