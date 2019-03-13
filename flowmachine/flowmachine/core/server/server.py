# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# server.py has no unit-test coverage, but is substantially exercised through integration
# tests. Hence, we exclude it from coverage.

import asyncio
import logging

import os
import signal
from logging.handlers import TimedRotatingFileHandler

import structlog

import zmq
from zmq.asyncio import Context
from flowmachine.core import connect
from flowmachine.core.query_state import QueryState
from .query_proxy import (
    QueryProxy,
    MissingQueryError,
    QueryProxyError,
    construct_query_object,
    InvalidGeographyError,
)
from .query_schemas import FlowmachineQuerySchema
from .zmq_interface import ZMQMultipartMessage, ZMQInterfaceError


logger = structlog.get_logger(__name__)
# Logger for all queries run or accessed
query_run_log = logging.getLogger("flowmachine-server")
query_run_log.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
query_run_log.addHandler(ch)
log_root = os.getenv("LOG_DIRECTORY", "/var/log/flowmachine/")
if not os.path.exists(log_root):
    logger.info(f"Creating log_root directory because it does not exist: {log_root}")
    os.makedirs(log_root)
fh = TimedRotatingFileHandler(os.path.join(log_root, "query-runs.log"), when="midnight")
fh.setLevel(logging.INFO)
query_run_log.addHandler(fh)
query_run_log = structlog.wrap_logger(query_run_log)


async def get_reply_for_message(zmq_msg: ZMQMultipartMessage) -> dict:
    """
    Dispatches the message to the appropriate handling function
    based on the specified action and returns the reply.

    Parameters
    ----------
    zmq_msg : ZMQMultipartMessage
        The message received via zeromq.

    Returns
    -------
    dict
        The reply received from one of the action handlers.
    """

    try:
        action = zmq_msg.action
        run_log_dict = dict(
            message=zmq_msg.msg_str,
            request_id=zmq_msg.api_request_id,
            params=zmq_msg.action_params,
        )
        if "ping" == action:
            logger.debug(f"Received 'ping'. Message: {zmq_msg.msg_str}")
            query_run_log.info("ping", **run_log_dict)
            reply = {"status": "accepted", "msg": "pong", "data": {}}
        elif "run_query" == action:
            logger.debug(f"Trying to run query.  Message: {zmq_msg.msg_str}")

            query_proxy = QueryProxy(
                zmq_msg.action_params["query_kind"], zmq_msg.action_params["params"]
            )
            query_id = query_proxy.run_query_async()
            query_run_log.info("run_query", query_id=query_id, **run_log_dict)
            reply = {"status": query_proxy.poll(), "id": query_id}

        elif "poll" == action:
            logger.debug(f"Trying to poll query.  Message: {zmq_msg.msg_str}")
            query_id = zmq_msg.action_params["query_id"]
            query_proxy = QueryProxy.from_query_id(query_id)
            status = query_proxy.poll()
            query_run_log.info("poll", query_id=query_id, status=status, **run_log_dict)
            reply = {"status": status, "id": query_id}

        elif "get_sql" == action:
            logger.debug(f"Trying to get query result. Message: {zmq_msg.msg_str}")
            query_id = zmq_msg.action_params["query_id"]
            query_proxy = QueryProxy.from_query_id(query_id)
            sql = query_proxy.get_sql()
            query_run_log.info("get_sql", query_id=query_id, **run_log_dict)
            reply = {"status": query_proxy.poll(), "sql": sql}

        elif "get_params" == action:
            logger.debug(f"Trying to get query parameters. Message: {zmq_msg.msg_str}")
            query_id = zmq_msg.action_params["query_id"]
            query_proxy = QueryProxy.from_query_id(query_id)
            query_run_log.info(
                "get_params",
                query_id=query_id,
                retrieved_params=query_proxy.params,
                **run_log_dict,
            )
            reply = {"id": query_id, "params": query_proxy.params}

        elif "get_query_kind" == action:
            logger.debug(f"Trying to get query kind. Message: {zmq_msg.msg_str}")
            query_id = zmq_msg.action_params["query_id"]
            query_proxy = QueryProxy.from_query_id(query_id)
            query_run_log.info(
                "get_query_kind",
                query_id=query_id,
                query_kind=query_proxy.query_kind,
                **run_log_dict,
            )
            reply = {"id": query_id, "query_kind": query_proxy.query_kind}

        elif "get_geography" == action:
            logger.debug(f"Trying to get geography. Message: {zmq_msg.msg_str}")
            # TODO: Once we have refactored QueryProxy, we won't want to
            # directly import 'construct_query_object' here.
            q = construct_query_object("geography", zmq_msg.action_params["params"])
            # Explicitly project to WGS84 (SRID=4326) to conform with GeoJSON standard
            sql = q.geojson_query(crs=4326)
            query_run_log.info("get_geography", **run_log_dict)
            reply = {"status": QueryState.COMPLETED, "sql": sql}

        elif "get_available_queries" == action:
            logger.debug(f"Trying to get available queries. Message: {zmq_msg.msg_str}")
            query_run_log.info("get_available_queries", **run_log_dict)
            available_queries = list(FlowmachineQuerySchema.type_schemas.keys())
            reply = {
                "status": "accepted",
                "msg": "",
                # TODO: don't hard-code this!
                "data": {"available_queries": available_queries},
            }

        else:
            logger.debug(f"Unknown action: '{action}'")
            reply = {
                "status": "error",
                "msg": f"Unknown action: '{action}'",
                "data": {},
            }

    except KeyError as e:
        reply = {"status": "error", "error": f"Missing key {e}"}
    except QueryProxyError as e:
        reply = {"status": "error", "error": f"{e}"}
    except MissingQueryError as e:
        reply = {"status": "awol", "id": e.missing_query_id, "error": f"{e}"}
    except InvalidGeographyError as e:
        reply = {"status": "awol", "error": f"{e}"}
    logger.debug(f"Received reply {reply} to message: {zmq_msg.msg_str}")
    return reply


async def recv(port):
    """
    Listen for messages coming in via zeromq on the given port, and dispatch them.
    """

    ctx = Context.instance()
    socket = ctx.socket(zmq.ROUTER)
    socket.bind(f"tcp://*:{port}")

    def shutdown():
        """
        Handler for SIGTERM to allow coverage data to be written during integration tests.
        """
        logger.debug("Caught SIGTERM. Shutting down.")
        socket.close()
        logger.debug("Closed ZMQ socket,")
        tasks = [
            task
            for task in asyncio.Task.all_tasks()
            if task is not asyncio.tasks.Task.current_task()
        ]
        list(map(lambda task: task.cancel(), tasks))
        logger.debug("Cancelled all remaining tasks.")

    # Get the loop and attach a sigterm handler to allow coverage data to be written
    main_loop = asyncio.get_event_loop()
    main_loop.add_signal_handler(signal.SIGTERM, shutdown)

    while True:
        try:
            zmq_msg = await get_next_zmq_message(socket)
        except ZMQInterfaceError as e:
            logger.error(
                f"Cannot process message due to unknown return address. Original error: {e}."
            )
            continue

        except asyncio.CancelledError:
            logger.error("Task cancelled. Shutting down.")
            break

        reply_coroutine = get_reply_for_message(zmq_msg)
        zmq_msg.send_reply_async(socket, reply_coroutine)

    socket.close()


async def get_next_zmq_message(socket):
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


def main():
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
