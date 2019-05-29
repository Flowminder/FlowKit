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

import functools
from typing import Callable, List, Optional, Union

from apispec import APISpec
from apispec_oneofschema import MarshmallowPlugin
from marshmallow import ValidationError

from flowmachine.core import Query, GeoTable
from flowmachine.core.cache import get_query_object_by_id
from flowmachine.core.query_info_lookup import (
    QueryInfoLookup,
    UnkownQueryIdError,
    QueryInfoLookupError,
)
from flowmachine.core.query_state import QueryStateMachine, QueryState
from flowmachine.utils import convert_dict_keys_to_strings
from .exceptions import FlowmachineServerError
from .query_schemas import FlowmachineQuerySchema
from .zmq_helpers import ZMQReply

__all__ = ["perform_action"]


def action_handler__ping() -> ZMQReply:
    """
    Handler for 'ping' action.

    Returns the message 'pong'.
    """
    return ZMQReply(status="success", msg="pong")


def action_handler__get_available_queries() -> ZMQReply:
    """
    Handler for 'get_available_queries' action.

    Returns a list of available flowmachine queries.
    """
    available_queries = list(FlowmachineQuerySchema.type_schemas.keys())
    return ZMQReply(status="success", payload={"available_queries": available_queries})


@functools.lru_cache(maxsize=1)
def action_handler__get_query_schemas() -> ZMQReply:
    """
    Handler for the 'get_query_schemas' action.

    Returns a dict with all supported flowmachine queries as keys
    and the associated schema for the query parameters as values.
    """
    spec = APISpec(
        title="FlowAPI",
        version="1.0.0",
        openapi_version="3.0.2",
        plugins=[MarshmallowPlugin()],
    )
    spec.components.schema("FlowmachineQuerySchema", schema=FlowmachineQuerySchema)
    schemas_spec = spec.to_dict()["components"]["schemas"]
    return ZMQReply(status="success", payload={"query_schemas": schemas_spec})


def action_handler__run_query(**action_params: dict) -> ZMQReply:
    """
    Handler for the 'run_query' action.

    Constructs a flowmachine query object, sets it running and returns the query_id.
    For this action handler the `action_params` are exactly the query kind plus the
    parameters needed to construct the query.
    """
    try:
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
        error_msg = "Parameter validation failed."
        validation_error_messages = convert_dict_keys_to_strings(exc.messages)
        return ZMQReply(
            status="error", msg=error_msg, payload=validation_error_messages
        )

    q_info_lookup = QueryInfoLookup(Query.redis)
    try:
        query_id = q_info_lookup.get_query_id(action_params)
    except QueryInfoLookupError:
        # Set the query running (it's safe to call this even if the query was set running before)
        try:
            query_id = query_obj.store_async()
        except Exception as e:
            return ZMQReply(
                status="error",
                msg="Unable to create query object.",
                payload={"exception": str(e)},
            )

        # Register the query as "known" (so that we can later look up the query kind
        # and its parameters from the query_id).

        q_info_lookup.register_query(query_id, action_params)

    return ZMQReply(status="success", payload={"query_id": query_id})


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
    q_info_lookup = QueryInfoLookup(Query.redis)
    try:
        return q_info_lookup.get_query_kind(query_id)
    except UnkownQueryIdError:
        return None


def action_handler__poll_query(query_id: str) -> ZMQReply:
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
        q_state_machine = QueryStateMachine(Query.redis, query_id)
        payload = {
            "query_id": query_id,
            "query_kind": query_kind,
            "query_state": q_state_machine.current_query_state,
        }
        return ZMQReply(status="success", payload=payload)


def action_handler__get_query_kind(query_id: str) -> ZMQReply:
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


def action_handler__get_query_params(query_id: str) -> ZMQReply:
    """
    Handler for the 'get_query_params' action.

    Returns query parameters of the query with the given `query_id`.
    """
    q_info_lookup = QueryInfoLookup(Query.redis)
    try:
        query_params = q_info_lookup.get_query_params(query_id)
    except UnkownQueryIdError:
        payload = {"query_id": query_id, "query_state": "awol"}
        return ZMQReply(
            status="error", msg=f"Unknown query id: '{query_id}'", payload=payload
        )

    payload = {"query_id": query_id, "query_params": query_params}
    return ZMQReply(status="success", payload=payload)


def action_handler__get_sql(query_id: str) -> ZMQReply:
    """
    Handler for the 'get_sql' action.

    Returns a SQL string which can be run against flowdb to obtain
    the result of the query with given `query_id`.
    """
    # TODO: currently we can't use QueryStateMachine to determine whether
    # the query_id belongs to a valid query object, so we need to check it
    # manually. Would be good to add a QueryState.UNKNOWN so that we can
    # avoid this separate treatment.
    q_info_lookup = QueryInfoLookup(Query.redis)
    if not q_info_lookup.query_is_known(query_id):
        msg = f"Unknown query id: '{query_id}'"
        payload = {"query_id": query_id, "query_state": "awol"}
        return ZMQReply(status="error", msg=msg, payload=payload)

    query_state = QueryStateMachine(Query.redis, query_id).current_query_state

    if query_state == QueryState.COMPLETED:
        q = get_query_object_by_id(Query.connection, query_id)
        sql = q.get_query()
        payload = {"query_id": query_id, "query_state": query_state, "sql": sql}
        return ZMQReply(status="success", payload=payload)
    else:
        msg = f"Query with id '{query_id}' {query_state.description}."
        payload = {"query_id": query_id, "query_state": query_state}
        return ZMQReply(status="error", msg=msg, payload=payload)


def action_handler__get_geography(aggregation_unit: str) -> ZMQReply:
    """
    Handler for the 'get_query_geography' action.

    Returns query parameters of the query with the given `query_id`.
    """

    # TODO: do we still need to validate the aggregation unit or does this happen
    # before (e.g. through marshmallow?)
    allowed_aggregation_units = ["admin0", "admin1", "admin2", "admin3", "admin4"]
    if aggregation_unit not in allowed_aggregation_units:
        error_msg = (
            f"Invalid aggregation unit. Must be one of: {allowed_aggregation_units}'"
        )
        return ZMQReply(status="error", msg=error_msg)

    try:
        q = GeoTable(
            name=aggregation_unit,
            schema="geography",
            columns=[f"{aggregation_unit}name", f"{aggregation_unit}pcod", "geom"],
        )
    except Exception as e:
        return ZMQReply(status="error", msg=f"{e}")

    # Explicitly project to WGS84 (SRID=4326) to conform with GeoJSON standard
    sql = q.geojson_query(crs=4326)
    # TODO: put query_run_log back in!
    # query_run_log.info("get_geography", **run_log_dict)
    payload = {"query_state": QueryState.COMPLETED, "sql": sql}
    return ZMQReply(status="success", payload=payload)


def action_handler__get_available_dates() -> ZMQReply:
    """
    Handler for the 'get_available_dates' action.

    Returns a dict of the form {"calls": [...], "sms": [...], ...}.

    Returns
    -------
    ZMQReply
        The reply from the action handler.
    """
    conn = Query.connection
    event_types = tuple(
        sorted([table_name for table_name, _, _, _ in conn.available_tables])
    )

    available_dates = {
        event_type: [date.strftime("%Y-%m-%d") for date in dates]
        for (event_type, dates) in conn.available_dates(
            table=event_types, strictness=2
        ).items()
    }
    return ZMQReply(status="success", payload=available_dates)


def get_action_handler(action: str) -> Callable:
    """Exception should be raised for handlers that don't exist."""
    try:
        return ACTION_HANDLERS[action]
    except KeyError:
        raise FlowmachineServerError(f"Unknown action: '{action}'")


def perform_action(action_name: str, action_params: dict) -> ZMQReply:
    """
    Perform action with the given action parameters.

    Parameters
    ----------
    action_name : str
        The action to be performed.
    action_params : dict
        Parameters for the action handler.

    Returns
    -------
    ZMQReply
        The reply from the action handler.
    """

    # Determine the handler function associated with this action
    action_handler_func = get_action_handler(action_name)

    # Run the action handler to obtain the reply
    try:
        reply = action_handler_func(**action_params)
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
    "get_geography": action_handler__get_geography,
    "get_available_dates": action_handler__get_available_dates,
}
