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

from apispec import APISpec
from apispec_oneofschema import MarshmallowPlugin
from marshmallow import ValidationError

from flowmachine.core import Query
from flowmachine.core.query_info_lookup import QueryInfoLookup, UnkownQueryIdError
from flowmachine.core.query_state import QueryStateMachine
from flowmachine.utils import convert_dict_keys_to_strings
from .exceptions import FlowmachineServerError
from .query_schemas import FlowmachineQuerySchema
from .zmq_helpers import ZMQReply

__all__ = ["perform_action"]


def action_handler__ping():
    """
    Handler for 'ping' action.

    Returns the message 'pong'.
    """
    return ZMQReply(status="done", msg="pong")


def action_handler__get_available_queries():
    """
    Handler for 'get_available_queries' action.

    Returns a list of available flowmachine queries.
    """
    available_queries = list(FlowmachineQuerySchema.type_schemas.keys())
    return ZMQReply(status="done", data={"available_queries": available_queries})


def action_handler__get_query_schemas():
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
    return ZMQReply(status="done", data={"query_schemas": schemas_spec})


def action_handler__run_query(**action_params):
    """
    Handler for the 'run_query' action.

    Constructs a flowmachine query object, sets it running and returns the query_id.
    """
    try:
        query_obj = FlowmachineQuerySchema().load(action_params)
    except ValidationError as exc:
        # The dictionary of marshmallow errors can contain integers as keys,
        # which will raise an error when converting to JSON (where the keys
        # must be strings). Therefore we transform the keys to strings here.
        error_messages = convert_dict_keys_to_strings(exc.messages)
        return ZMQReply(status="error", msg="", data=error_messages)

    # FIXME: Sanity check: when query_obj above was created it should have automatically
    # registered the query info lookup. However, this is contingent on the fact
    # that any subclass of BaseExposedQuery calls super().__init__() at the end
    # of its own __init__() method (see comment in BaseExposedQuery.__init__()).
    # We should add a metaclass which does this automatically, but until then it
    # is safer to verify here that the query info lookup really exists.
    q_info_lookup = QueryInfoLookup(Query.redis)
    if not q_info_lookup.query_is_known(query_obj.query_id):
        error_msg = f"Internal flowmachine server error: query info is missing for query_id '{query_obj.query_id}'"
        return ZMQReply(status="error", msg=error_msg)

    # Set the query running (it's safe to call this even if the query was set running before)
    query_id = query_obj.store_async()

    return ZMQReply(status="accepted", data={"query_id": query_id})


def action_handler__poll_query(query_id):
    """
    Handler for the 'poll_query' action.

    Returns the status of the query with the given `query_id`.
    """
    q_state_machine = QueryStateMachine(Query.redis, query_id)
    reply_data = {
        "query_id": query_id,
        "query_state": q_state_machine.current_query_state,
    }
    return ZMQReply(status="done", data=reply_data)


def action_handler__get_query_kind(query_id):
    """
    Handler for the 'poll_kind' action.

    Returns query kind of the query with the given `query_id`.
    """
    redis = Query.redis
    q_info_lookup = QueryInfoLookup(redis)
    try:
        query_kind = q_info_lookup.get_query_kind(query_id)
    except UnkownQueryIdError:
        reply_data = {"query_id": query_id, "query_state": "awol"}
        return ZMQReply(
            status="error", msg=f"Unknown query id: '{query_id}'", data=reply_data
        )

    reply_data = {"query_id": query_id, "query_kind": query_kind}
    return ZMQReply(status="done", data=reply_data)


def get_action_handler(action):
    try:
        return ACTION_HANDLERS[action]
    except KeyError:
        raise FlowmachineServerError(f"Unknown action: '{action}'")


def perform_action(action_name, action_params):
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
}
