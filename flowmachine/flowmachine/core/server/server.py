# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import asyncio
import os
from json import JSONDecodeError

import rapidjson


import signal
import structlog
import zmq
from functools import partial

from marshmallow import ValidationError
from zmq.asyncio import Context

import flowmachine
from flowmachine.utils import convert_dict_keys_to_strings
from .exceptions import FlowmachineServerError
from .zmq_helpers import ZMQReply
from flowmachine.core.server.action_request_schema import ActionRequest
from .action_handlers import perform_action

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)
query_run_log = structlog.get_logger("flowmachine.query_run_log")


def get_reply_for_message(msg_str: str) -> ZMQReply:
    """
    Parse the zmq message string, perform the desired action and return the result in JSON format.

    Parameters
    ----------
    msg_str : str
        The message string as received from zmq. See the docstring of `parse_zmq_message` for valid structure.

    Returns
    -------
    dict
        The reply in JSON format.
    """

    try:
        action_request = ActionRequest().loads(msg_str)

        query_run_log.info(
            f"Attempting to perform action: '{action_request.action}'",
            request_id=action_request.request_id,
            action=action_request.action,
            params=action_request.params,
        )

        reply = perform_action(action_request.action, action_request.params)

        query_run_log.info(
            f"Action completed with status: '{reply.status}'",
            request_id=action_request.request_id,
            action=action_request.action,
            params=action_request.params,
            reply_status=reply.status,
            reply_msg=reply.msg,
            reply_payload=reply.payload,
        )
    except FlowmachineServerError as exc:
        return ZMQReply(status="error", msg=exc.error_msg)
    except ValidationError as exc:
        # The dictionary of marshmallow errors can contain integers as keys,
        # which will raise an error when converting to JSON (where the keys
        # must be strings). Therefore we transform the keys to strings here.
        error_msg = "Invalid action request."
        validation_error_messages = convert_dict_keys_to_strings(exc.messages)
        return ZMQReply(
            status="error", msg=error_msg, payload=validation_error_messages
        )
    except JSONDecodeError as exc:
        return ZMQReply(
            status="error", msg="Invalid JSON.", payload={"decode_error": exc.msg}
        )

    # Return the reply (in JSON format)
    return reply


async def receive_next_zmq_message_and_send_back_reply(socket):
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

    Returns
    -------
    flowmachine.core.server.zmq_interface.ZMQMultipartMessage
        The message received over the socket
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
        calculate_and_send_reply_for_message(socket, return_address, msg_contents)
    )


async def calculate_and_send_reply_for_message(socket, return_address, msg_contents):
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
    """
    reply_json = get_reply_for_message(msg_contents)
    socket.send_multipart([return_address, b"", rapidjson.dumps(reply_json).encode()])


def shutdown(socket):
    """
    Handler for SIGTERM to allow test coverage data to be written during integration tests.
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


async def recv(port):
    """
    Main receive-and-reply loop. Listens to zmq messages on the given port,
    processes them and sends back a reply with the result or an error message.
    """
    logger.info(f"Flowmachine server is listening on port {port}")

    ctx = Context.instance()
    socket = ctx.socket(zmq.ROUTER)
    socket.bind(f"tcp://*:{port}")

    # Get the loop and attach a sigterm handler to allow coverage data to be written
    main_loop = asyncio.get_event_loop()
    main_loop.add_signal_handler(signal.SIGTERM, partial(shutdown, socket=socket))

    try:
        while True:
            await receive_next_zmq_message_and_send_back_reply(socket)
    except Exception as exc:
        logger.debug(f"Received exception: {exc}")
        logger.error("Flowmachine server died unexpectedly.")
        socket.close()


def main():
    # Set up internals and connect to flowdb
    flowmachine.connect()

    # Set debug mode if required
    debug_mode = "true" == os.getenv("FLOWMACHINE_SERVER_DEBUG_MODE", "false").lower()
    if debug_mode:
        logger.info("Enabling asyncio's debugging mode.")

    # Run receive loop which receives zmq messages and sends back replies
    port = os.getenv("FLOWMACHINE_PORT", 5555)
    asyncio.run(
        recv(port), debug=debug_mode
    )  # note: asyncio.run() requires Python 3.7+


if __name__ == "__main__":
    main()
