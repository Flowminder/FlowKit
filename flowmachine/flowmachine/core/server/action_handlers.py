# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

#
# This module contains the handler functions for the actions which are
# supported by the flowmachine server. In order to implement a new action
# you need to provide a handler function (see below for some examples) and
# register it in the ACTION_HANDLERS lookup. The return value of the action
# handler must be of type `ZMQReply`.
#
# In order to actually perform an action, call the `perform_action()`
# function with the action name and parameters. This selects the correct
# action handler and also gracefully handles any potential errors.
#
import asyncio
from contextvars import copy_context
from functools import partial
import json
import textwrap
from typing import Callable, Union

from marshmallow import ValidationError

from flowmachine.core.context import get_db, get_redis
from flowmachine.core.cache import get_query_object_by_id
from flowmachine.core.query_info_lookup import (
    QueryInfoLookup,
    UnkownQueryIdError,
    QueryInfoLookupError,
)
from flowmachine.core.query_state import QueryStateMachine, QueryState
from flowmachine.utils import convert_dict_keys_to_strings
from .exceptions import FlowmachineServerError
from .query_schemas import FlowmachineQuerySchema, GeographySchema
from .query_schemas.flowmachine_query import get_query_schema
from .zmq_helpers import ZMQReply

__all__ = ["perform_action"]

from ..dependency_graph import query_progress


async def action_handler__ping(config: "FlowmachineServerConfig") -> ZMQReply:
    """
    Handler for 'ping' action.

    Returns the message 'pong'.
    """
    return ZMQReply(status="success", msg="pong")


async def action_handler__get_available_queries(
    config: "FlowmachineServerConfig",
) -> ZMQReply:
    """
    Handler for 'get_available_queries' action.

    Returns a list of available flowmachine queries.
    """
    available_queries = list(FlowmachineQuerySchema.type_schemas.keys())
    return ZMQReply(status="success", payload={"available_queries": available_queries})


async def action_handler__get_query_schemas(
    config: "FlowmachineServerConfig",
) -> ZMQReply:
    """
    Handler for the 'get_query_schemas' action.

    Returns a dict with all supported flowmachine queries as keys
    and the associated schema for the query parameters as values.
    """

    return ZMQReply(status="success", payload={"query_schemas": get_query_schema()})


async def action_handler__run_query(
    config: "FlowmachineServerConfig", **action_params: dict
) -> ZMQReply:
    """
    Handler for the 'run_query' action.

    Constructs a flowmachine query object, sets it running and returns the query_id.
    For this action handler the `action_params` are exactly the query kind plus the
    parameters needed to construct the query.
    """
    try:
        query_obj = FlowmachineQuerySchema().load(action_params)
    except TypeError as exc:
        # We need to catch TypeError here, otherwise they propagate up to
        # perform_action() and result in a very misleading error message.
        orig_error_msg = exc.args[0]
        error_msg = (
            f"Internal flowmachine server error: could not create query object using query schema. "
            f"The original error was: '{orig_error_msg}'"
        )
        return ZMQReply(
            status="error",
            msg=error_msg,
            payload={"params": action_params, "orig_error_msg": orig_error_msg},
        )
    except ValidationError as exc:
        # The dictionary of marshmallow errors can contain integers as keys,
        # which will raise an error when converting to JSON (where the keys
        # must be strings). Therefore we transform the keys to strings here.
        validation_error_messages = convert_dict_keys_to_strings(exc.messages)
        action_params_as_text = textwrap.indent(
            json.dumps(action_params, indent=2), "   "
        )
        validation_errors_as_text = textwrap.indent(
            json.dumps(validation_error_messages, indent=2), "   "
        )
        error_msg = (
            "Parameter validation failed.\n\n"
            f"The action parameters were:\n{action_params_as_text}.\n\n"
            f"Validation error messages:\n{validation_errors_as_text}.\n\n"
        )
        payload = {"validation_error_messages": validation_error_messages}
        return ZMQReply(status="error", msg=error_msg, payload=payload)

    q_info_lookup = QueryInfoLookup(get_redis())
    try:
        query_id = q_info_lookup.get_query_id(action_params)
        qsm = QueryStateMachine(
            query_id=query_id, redis_client=get_redis(), db_id=get_db().conn_id
        )
        if qsm.current_query_state in [
            QueryState.CANCELLED,
            QueryState.KNOWN,
        ]:  # Start queries running even if they've been cancelled or reset
            if qsm.is_cancelled:
                reset = qsm.reset()
                finish = qsm.finish_resetting()
            raise QueryInfoLookupError
    except QueryInfoLookupError:
        try:
            # Set the query running (it's safe to call this even if the query was set running before)
            query_id = await asyncio.get_running_loop().run_in_executor(
                executor=config.server_thread_pool,
                func=partial(
                    copy_context().run,
                    partial(
                        query_obj.store_async,
                        store_dependencies=config.store_dependencies,
                    ),
                ),
            )
        except Exception as e:
            return ZMQReply(
                status="error",
                msg="Unable to create query object.",
                payload={"exception": str(e)},
            )

        # Register the query as "known" (so that we can later look up the query kind
        # and its parameters from the query_id).

        q_info_lookup.register_query(query_id, action_params)

    return ZMQReply(
        status="success",
        payload={
            "query_id": query_id,
            "progress": query_progress(query_obj._flowmachine_query_obj),
        },
    )


def _get_query_kind_for_query_id(query_id: str) -> Union[None, str]:
    """
    Helper function to look up the query kind corresponding to the
    given query id. Returns `None` if the query_id does not exist.

    Parameters
    ----------
    query_id : str
        Identifier of the query.

    Returns
    -------
    str or None
        The query kind associated with this query_id (or None
        if no query with this query_id exists).
    """
    q_info_lookup = QueryInfoLookup(get_redis())
    try:
        return q_info_lookup.get_query_kind(query_id)
    except UnkownQueryIdError:
        return None


async def action_handler__poll_query(
    config: "FlowmachineServerConfig", query_id: str
) -> ZMQReply:
    """
    Handler for the 'poll_query' action.

    Returns the status of the query with the given `query_id`.
    """
    query_kind = _get_query_kind_for_query_id(query_id)
    # TODO: we should probably be able to use the QueryStateMachine to determine
    # whether the query already exists.
    if query_kind is None:
        payload = {"query_id": query_id, "query_state": "awol"}
        return ZMQReply(
            status="error", msg=f"Unknown query id: '{query_id}'", payload=payload
        )
    else:
        q_state_machine = QueryStateMachine(get_redis(), query_id, get_db().conn_id)
        payload = {
            "query_id": query_id,
            "query_kind": query_kind,
            "query_state": q_state_machine.current_query_state,
            "progress": query_progress(
                FlowmachineQuerySchema()
                .load(QueryInfoLookup(get_redis()).get_query_params(query_id))
                ._flowmachine_query_obj
            ),
        }
        return ZMQReply(status="success", payload=payload)


async def action_handler__get_query_kind(
    config: "FlowmachineServerConfig", query_id: str
) -> ZMQReply:
    """
    Handler for the 'get_query_kind' action.

    Returns query kind of the query with the given `query_id`.
    """
    query_kind = _get_query_kind_for_query_id(query_id)
    if query_kind is None:
        error_msg = f"Unknown query id: '{query_id}'"
        payload = {"query_id": query_id, "query_state": "awol"}
        return ZMQReply(status="error", msg=error_msg, payload=payload)
    else:
        payload = {"query_id": query_id, "query_kind": query_kind}
        return ZMQReply(status="success", payload=payload)


async def action_handler__get_query_params(
    config: "FlowmachineServerConfig", query_id: str
) -> ZMQReply:
    """
    Handler for the 'get_query_params' action.

    Returns query parameters of the query with the given `query_id`.
    """
    q_info_lookup = QueryInfoLookup(get_redis())
    try:
        query_params = q_info_lookup.get_query_params(query_id)
    except UnkownQueryIdError:
        payload = {"query_id": query_id, "query_state": "awol"}
        return ZMQReply(
            status="error", msg=f"Unknown query id: '{query_id}'", payload=payload
        )

    payload = {"query_id": query_id, "query_params": query_params}
    return ZMQReply(status="success", payload=payload)


async def action_handler__get_sql(
    config: "FlowmachineServerConfig", query_id: str
) -> ZMQReply:
    """
    Handler for the 'get_sql' action.

    Returns a SQL string which can be run against flowdb to obtain
    the result of the query with given `query_id`.
    """
    # TODO: currently we can't use QueryStateMachine to determine whether
    # the query_id belongs to a valid query object, so we need to check it
    # manually. Would be good to add a QueryState.UNKNOWN so that we can
    # avoid this separate treatment.
    q_info_lookup = QueryInfoLookup(get_redis())
    if not q_info_lookup.query_is_known(query_id):
        msg = f"Unknown query id: '{query_id}'"
        payload = {"query_id": query_id, "query_state": "awol"}
        return ZMQReply(status="error", msg=msg, payload=payload)

    query_state = QueryStateMachine(
        get_redis(), query_id, get_db().conn_id
    ).current_query_state

    if query_state == QueryState.COMPLETED:
        q = get_query_object_by_id(get_db(), query_id)
        sql = q.get_query()
        payload = {"query_id": query_id, "query_state": query_state, "sql": sql}
        return ZMQReply(status="success", payload=payload)
    else:
        msg = f"Query with id '{query_id}' {query_state.description}."
        payload = {"query_id": query_id, "query_state": query_state}
        return ZMQReply(status="error", msg=msg, payload=payload)


async def action_handler__get_geo_sql(
    config: "FlowmachineServerConfig", query_id: str
) -> ZMQReply:
    """
    Handler for the 'get_sql' action.

    Returns a SQL string which can be run against flowdb to obtain
    the result of the query with given `query_id`.
    """
    # TODO: currently we can't use QueryStateMachine to determine whether
    # the query_id belongs to a valid query object, so we need to check it
    # manually. Would be good to add a QueryState.UNKNOWN so that we can
    # avoid this separate treatment.
    q_info_lookup = QueryInfoLookup(get_redis())
    if not q_info_lookup.query_is_known(query_id):
        msg = f"Unknown query id: '{query_id}'"
        payload = {"query_id": query_id, "query_state": "awol"}
        return ZMQReply(status="error", msg=msg, payload=payload)

    query_state = QueryStateMachine(
        get_redis(), query_id, get_db().conn_id
    ).current_query_state

    if query_state == QueryState.COMPLETED:
        q = get_query_object_by_id(get_db(), query_id)
        try:
            sql = q.geojson_query()
            payload = {
                "query_id": query_id,
                "query_state": query_state,
                "sql": sql,
                "aggregation_unit": q.spatial_unit.canonical_name,
            }
            return ZMQReply(status="success", payload=payload)
        except AttributeError:
            msg = f"Query with id '{query_id}' has no geojson compatible representation."  # TODO: This codepath is untested because all queries right now have geography
            payload = {"query_id": query_id, "query_state": "errored"}
            return ZMQReply(status="error", msg=msg, payload=payload)
    else:
        msg = f"Query with id '{query_id}' {query_state.description}."
        payload = {"query_id": query_id, "query_state": query_state}
        return ZMQReply(status="error", msg=msg, payload=payload)


async def action_handler__get_geography(
    config: "FlowmachineServerConfig", aggregation_unit: str
) -> ZMQReply:
    """
    Handler for the 'get_query_geography' action.

    Returns SQL to get geography for the given `aggregation_unit` as GeoJSON.
    """
    try:
        query_obj = GeographySchema().load({"aggregation_unit": aggregation_unit})
    except TypeError as exc:
        # We need to catch TypeError here, otherwise they propagate up to
        # perform_action() and result in a very misleading error message.
        orig_error_msg = exc.args[0]
        error_msg = (
            f"Internal flowmachine server error: could not create query object using query schema. "
            f"The original error was: '{orig_error_msg}'"
        )
        return ZMQReply(
            status="error",
            msg=error_msg,
            payload={
                "params": {"aggregation_unit": aggregation_unit},
                "orig_error_msg": orig_error_msg,
            },
        )
    except ValidationError as exc:
        # The dictionary of marshmallow errors can contain integers as keys,
        # which will raise an error when converting to JSON (where the keys
        # must be strings). Therefore we transform the keys to strings here.
        error_msg = "Parameter validation failed."
        validation_error_messages = convert_dict_keys_to_strings(exc.messages)
        return ZMQReply(
            status="error", msg=error_msg, payload=validation_error_messages
        )

    # We don't cache the query, because it just selects columns from a
    # geography table. If we expose an aggregation unit which relies on another
    # query to create the geometry (e.g. grid), we may want to reconsider this
    # decision.

    sql = query_obj.geojson_sql
    # TODO: put query_run_log back in!
    # query_run_log.info("get_geography", **run_log_dict)
    payload = {"query_state": QueryState.COMPLETED, "sql": sql}
    return ZMQReply(status="success", payload=payload)


async def action_handler__get_available_dates(
    config: "FlowmachineServerConfig",
) -> ZMQReply:
    """
    Handler for the 'get_available_dates' action.

    Returns a dict of the form {"calls": [...], "sms": [...], ...}.

    Returns
    -------
    ZMQReply
        The reply from the action handler.
    """
    conn = get_db()

    available_dates = {
        event_type: [date.strftime("%Y-%m-%d") for date in dates]
        for (event_type, dates) in conn.available_dates.items()
    }
    return ZMQReply(status="success", payload=available_dates)


def get_action_handler(action: str) -> Callable:
    """Exception should be raised for handlers that don't exist."""
    try:
        return ACTION_HANDLERS[action]
    except KeyError:
        raise FlowmachineServerError(f"Unknown action: '{action}'")


async def perform_action(
    action_name: str, action_params: dict, *, config: "FlowmachineServerConfig"
) -> ZMQReply:
    """
    Perform action with the given action parameters.

    Parameters
    ----------
    action_name : str
        The action to be performed.
    action_params : dict
        Parameters for the action handler.
    config : FlowmachineServerConfig
        Server config options

    Returns
    -------
    ZMQReply
        The reply from the action handler.
    """

    # Determine the handler function associated with this action
    action_handler_func = get_action_handler(action_name)

    # Run the action handler to obtain the reply
    try:
        reply = await action_handler_func(config=config, **action_params)
    except TypeError:
        error_msg = f"Internal flowmachine server error: wrong arguments passed to handler for action '{action_name}'."
        raise FlowmachineServerError(error_msg)

    # Safety check to ensure the handler function returned an instance of ZMQReply
    if not isinstance(reply, ZMQReply):
        error_msg = f"Internal flowmachine server error: handler for action '{action_name}' returned an invalid reply."
        raise FlowmachineServerError(error_msg)

    return reply


ACTION_HANDLERS = {
    "ping": action_handler__ping,
    "get_available_queries": action_handler__get_available_queries,
    "get_query_schemas": action_handler__get_query_schemas,
    "run_query": action_handler__run_query,
    "poll_query": action_handler__poll_query,
    "get_query_kind": action_handler__get_query_kind,
    "get_query_params": action_handler__get_query_params,
    "get_sql_for_query_result": action_handler__get_sql,
    "get_geo_sql_for_query_result": action_handler__get_geo_sql,
    "get_geography": action_handler__get_geography,
    "get_available_dates": action_handler__get_available_dates,
}
