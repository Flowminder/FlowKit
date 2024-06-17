# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import asyncio
from json import JSONDecodeError
import traceback

import rapidjson


import signal
import structlog
import zmq
from functools import partial
from typing import NoReturn

from marshmallow import ValidationError
from zmq.asyncio import Context

import flowmachine
from flowmachine.core.context import action_request_context
from flowmachine.utils import convert_dict_keys_to_strings
from .exceptions import FlowmachineServerError
from .zmq_helpers import ZMQReply
from flowmachine.core.server.action_request_schema import ActionRequest
from .action_handlers import perform_action
from .server_config import get_server_config

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)
query_run_log = structlog.get_logger("flowmachine.query_run_log")


async def get_reply_for_message(
    *, msg_str: str, config: "FlowmachineServerConfig"
) -> ZMQReply:
    """
    Parse the zmq message string, perform the desired action and return the result in JSON format.

    Parameters
    ----------
    msg_str : str
        The message string as received from zmq. See the docstring of `parse_zmq_message` for valid structure.
    config : FlowmachineServerConfig
        Server config options

    Returns
    -------
    dict
        The reply in JSON format.
    """

    try:
        try:
            action_request = ActionRequest().loads(msg_str)
        except ValidationError as exc:
            # The dictionary of marshmallow errors can contain integers as keys,
            # which will raise an error when converting to JSON (where the keys
            # must be strings). Therefore we transform the keys to strings here.
            error_msg = "Invalid action request."
            validation_error_messages = convert_dict_keys_to_strings(exc.messages)
            logger.error(
                "Invalid action request while getting reply for ZMQ message.",
                **validation_error_messages,
            )
            return ZMQReply(
                status="error", msg=error_msg, payload=validation_error_messages
            )

        with action_request_context(action_request):
            query_run_log.info(
                f"Attempting to perform action: '{action_request.action}'",
                params=action_request.params,
            )

            reply = await perform_action(
                action_request.action, action_request.params, config=config
            )

            query_run_log.info(
                f"Action completed with status: '{reply.status}'",
                params=action_request.params,
                reply_status=reply.status,
                reply_msg=reply.msg,
                reply_payload=reply.payload,
            )
    except FlowmachineServerError as exc:
        logger.error(
            f"Caught Flowmachine server error while getting reply for ZMQ message: {exc.error_msg}"
        )
        return ZMQReply(status="error", msg=exc.error_msg)
    except JSONDecodeError as exc:
        logger.error(f"Invalid JSON while getting reply for ZMQ message: {exc.msg}")
        return ZMQReply(
            status="error", msg="Invalid JSON.", payload={"decode_error": exc.msg}
        )

    # Return the reply (in JSON format)
    return reply


async def receive_next_zmq_message_and_send_back_reply(
    *, socket: "zmq.asyncio.Socket", config: "FlowmachineServerConfig"
) -> None:
    """
    Listen on the given zmq socket for the next multipart message, .

    Note that the only responsibility of this function is to ensure
    that the incoming zmq message has the expected structure (three
    parts of the form `return_address, empty_delimiter, msg`) and to
    send back the reply. The responsibility for actually processing
    the message and calculating the reply lies with other functions.

    Parameters
    ----------
    socket : zmq.asyncio.Socket
        zmq socket to use for sending the message
    config : FlowmachineServerConfig
        Server config options
    """
    logger.debug("Waiting for messages.")
    multipart_msg = await socket.recv_multipart()
    logger.debug(f"Received multipart msg {multipart_msg}")

    #
    # Check structural integrity of the zmq multipart message.
    # Ignore it if it doesn't have the expected structure.
    #
    if len(multipart_msg) != 3:
        logger.error(
            "Multipart message did not contain the expected three parts. Ignoring this message "
            "as it cannot have come from FlowAPI and we cannot determine a return address."
        )
        return

    return_address, empty_delimiter, msg_contents = multipart_msg

    if empty_delimiter != b"":
        logger.error(
            "Multipart message did not have the expected structure. "
            "Ignoring this message as it cannot have come from FlowAPI."
        )
        return

    #
    # We have received a correctly formatted message. Create a background
    # task which calculates the reply and returns it back to the sender.
    logger.debug(
        f"Creating background task to calculate reply and return it to the sender."
    )
    asyncio.create_task(
        calculate_and_send_reply_for_message(
            socket=socket,
            return_address=return_address,
            msg_contents=msg_contents,
            config=config,
        )
    )


async def calculate_and_send_reply_for_message(
    *,
    socket: "zmq.asyncio.Socket",
    return_address: bytes,
    msg_contents: str,
    config: "FlowmachineServerConfig",
) -> None:
    """
    Calculate the reply to a zmq message and return the result to the sender.
    This function is a small wrapper around `get_reply_for_message` which can
    be executed as an asyncio background task.

    Parameters
    ----------
    socket : zmq.asyncio.Socket
        The zmq socket to use for sending the reply.
    return_address : bytes
        The zmq return address to which to send the reply.
    msg_contents : str
        JSON string with the message contents.
    config : FlowmachineServerConfig
        Server config options
    """
    try:
        reply_json = await get_reply_for_message(msg_str=msg_contents, config=config)
    except Exception as exc:
        # Catch and log any unhandled errors, and send a generic error response to the API
        # TODO: Ensure that FlowAPI always returns the correct error code when receiving an error reply
        logger.error(
            f"Unexpected error while getting reply for ZMQ message. Received exception: {type(exc).__name__}: {exc}",
            traceback=traceback.format_list(traceback.extract_tb(exc.__traceback__)),
        )
        reply_json = ZMQReply(status="error", msg="Could not get reply for message")
    await socket.send_multipart(
        [return_address, b"", rapidjson.dumps(reply_json).encode()]
    )
    logger.debug("Sent reply", reply=reply_json, msg=msg_contents)


def shutdown(socket: "zmq.asyncio.Socket") -> None:
    """
    Handler for SIGTERM to allow test coverage data to be written during integration tests.
    """
    logger.debug("Caught SIGTERM. Shutting down.")
    socket.close()
    logger.debug("Closed ZMQ socket,")
    tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
    list(map(lambda task: task.cancel(), tasks))
    logger.debug("Cancelled all remaining tasks.")


async def recv(*, config: "FlowmachineServerConfig") -> NoReturn:
    """
    Main receive-and-reply loop. Listens to zmq messages on the given port,
    processes them and sends back a reply with the result or an error message.

    Parameters
    ----------
    config : FlowmachineServerConfig
        Server config options
    """
    logger.info(f"Flowmachine server is listening on port {config.port}")

    ctx = Context.instance()
    socket = ctx.socket(zmq.ROUTER)
    socket.bind(f"tcp://*:{config.port}")

    # Get the loop and attach a sigterm handler to allow coverage data to be written
    main_loop = asyncio.get_event_loop()
    main_loop.add_signal_handler(signal.SIGTERM, partial(shutdown, socket=socket))

    try:
        while True:
            await receive_next_zmq_message_and_send_back_reply(
                socket=socket, config=config
            )
    except Exception as exc:
        logger.error(
            f"Received exception: {type(exc).__name__}: {exc}",
            traceback=traceback.format_list(traceback.extract_tb(exc.__traceback__)),
        )
        logger.error("Flowmachine server died unexpectedly.")
        socket.close()


def main():
    # Read config options from environment variables
    config = get_server_config()
    # Connect to flowdb
    flowmachine.connect()

    if not config.store_dependencies:
        logger.info("Dependency caching is disabled.")
    if config.debug_mode:
        logger.info("Enabling asyncio's debugging mode.")

    # Run receive loop which receives zmq messages and sends back replies
    asyncio.run(
        recv(config=config),
        debug=config.debug_mode,
    )  # note: asyncio.run() requires Python 3.7+


if __name__ == "__main__":
    main()
