# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# server.py has no unit-test coverage, but is substantially exercised through integration
# tests. Hence, we exclude it from coverage.

import asyncio
import logging
import os
import zmq
from zmq.asyncio import Context
from flowmachine.core import connect
from .query_proxy import QueryProxy, MissingQueryError, QueryProxyError
from .zmq_interface import ZMQMultipartMessage, ZMQInterfaceError

logger = logging.getLogger("flowmachine").getChild(__name__)


async def get_reply_for_message(  # pragma: no cover
    zmq_msg: ZMQMultipartMessage  # pragma: no cover
) -> dict:  # pragma: no cover
    """
    Dispatches the message to the appropriate handling function
    based on the specified action and returns the reply.

    Parameters
    ----------
    message : dict
        The message received via zeromq.

    Returns
    -------
    dict
        The reply received from one of the action handlers.
    """

    try:
        action = zmq_msg.action

        if "run_query" == action:
            logger.debug(f"Trying to run query.  Message: {zmq_msg.msg_str}")
            query_proxy = QueryProxy(
                zmq_msg.action_params["query_kind"], zmq_msg.action_params["params"]
            )
            query_id = query_proxy.run_query_async()
            reply = {"status": "accepted", "id": query_id}

        elif "poll" == action:
            logger.debug(f"Trying to poll query.  Message: {zmq_msg.msg_str}")
            query_id = zmq_msg.action_params["query_id"]
            query_proxy = QueryProxy.from_query_id(query_id)
            status = query_proxy.poll()
            reply = {"status": status, "id": query_id}

        elif "get_sql" == action:
            logger.debug(f"Trying to get query result. Message: {zmq_msg.msg_str}")
            query_id = zmq_msg.action_params["query_id"]
            query_proxy = QueryProxy.from_query_id(query_id)
            sql = query_proxy.get_sql()
            reply = {"status": "done", "sql": sql}

        elif "get_params" == action:
            logger.debug(f"Trying to get query parameters. Message: {zmq_msg.msg_str}")
            query_id = zmq_msg.action_params["query_id"]
            query_proxy = QueryProxy.from_query_id(query_id)
            reply = {"id": query_id, "params": query_proxy.params}

        elif "get_query_kind" == action:
            logger.debug(f"Trying to get query kind. Message: {zmq_msg.msg_str}")
            query_id = zmq_msg.action_params["query_id"]
            query_proxy = QueryProxy.from_query_id(query_id)
            reply = {"id": query_id, "query_kind": query_proxy.query_kind}

        else:
            logger.debug(f"Unknown action: '{action}'")
            reply = {"status": "rejected", "error": f"Unknown action: '{action}'"}

    except KeyError as e:
        reply = {"status": "rejected", "error": f"Missing key {e}"}
    except QueryProxyError as e:
        reply = {"status": "rejected", "reason": f"{e}"}
    except MissingQueryError as e:
        reply = {"status": "awol", "id": e.missing_query_id}

    logger.debug(f"Received reply {reply} to message: {zmq_msg.msg_str}")
    return reply


async def recv(port):  # pragma: no cover
    """
    Listen for messages coming in via zeromq on the given port, and dispatch them.
    """
    ctx = Context.instance()
    socket = ctx.socket(zmq.ROUTER)
    socket.bind(f"tcp://*:{port}")
    while True:
        try:
            zmq_msg = await get_next_zmq_message(socket)
        except ZMQInterfaceError as e:
            logger.error(
                f"Cannot process message due to unknown return address. Original error: {e}."
            )
            continue

        reply_coroutine = get_reply_for_message(zmq_msg)
        zmq_msg.send_reply_async(socket, reply_coroutine)

    s.close()


async def get_next_zmq_message(socket):  # pragma: no cover
    """
    Listen on the given zmq socket and return the next multipart message received.

    Parameters
    ----------
    socket : zmq.asyncio.Socket
        zmq socket to use for sending the message

    Returns
    -------
    flowmachine.core.server.zmq_interface.ZMQMultipartMessage
        The message received over the socket
    """
    logger.debug("Waiting for messages.")
    multipart_msg = await socket.recv_multipart()
    logger.debug(f"Received multipart msg {multipart_msg}")
    return ZMQMultipartMessage(multipart_msg)


def main():  # pragma: no cover
    port = os.getenv("FLOWMACHINE_PORT", 5555)
    connect()
    debug_mode = "True" == os.getenv("DEBUG", "False")
    if debug_mode:
        logger.info("Enabling asyncio's debugging mode.")
    try:
        asyncio.run(recv(port), debug=debug_mode)
    except AttributeError:
        main_loop = asyncio.get_event_loop()
        if debug_mode:
            main_loop.set_debug(True)
        main_loop.run_until_complete(recv(port))
