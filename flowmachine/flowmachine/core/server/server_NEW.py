# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import asyncio
import os
import rapidjson
import structlog
import zmq

import flowmachine
from .action_handlers import perform_action
from .exceptions import FlowmachineServerError
from .zmq_helpers import ZMQReply, parse_zmq_message

logger = structlog.get_logger(__name__)


def get_reply_for_message(msg_str):
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
        action, action_params = parse_zmq_message(msg_str)
        reply = perform_action(action, action_params)
    except FlowmachineServerError as exc:
        return ZMQReply(status="error", msg=exc.error_msg).as_json()

    # Return the reply (in JSON format)
    return reply.as_json()


def receive_next_zmq_message_and_send_back_reply(socket):
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
    multipart_msg = socket.recv_multipart()
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

    return_address, empty_delimiter, msg_str = multipart_msg

    if empty_delimiter != b"":
        logger.error(
            "Multipart message did not have the expected structure. "
            "Ignoring this message as it cannot have come from FlowAPI."
        )
        return

    #
    # We have received a correctly formatted message. Calculate the reply ...
    #
    reply_json = get_reply_for_message(msg_str)

    #
    # ... and return the reply back to the sender.
    #
    logger.debug(f"Sending reply to recipient {return_address}: {reply_json}")
    socket.send_multipart([return_address, b"", rapidjson.dumps(reply_json).encode()])


def recv(port):
    """
    Main receive-and-reply loop. Listens to zmq messages on the given port,
    processes them and sends back a reply with the result or an error message.
    """
    logger.info(f"Flowmachine server is listening on port {port}")

    ctx = zmq.Context.instance()
    socket = ctx.socket(zmq.ROUTER)
    socket.bind(f"tcp://*:{port}")

    try:
        while True:
            receive_next_zmq_message_and_send_back_reply(socket)
    except Exception as exc:
        logger.debug(f"Received exception: {exc}")
        logger.error("Flowmachine server died unexpectedly.")
        socket.close()


def main():
    # Set up internals and connect to flowdb
    flowmachine.connect()

    # Set debug mode if required
    debug_mode = "true" == os.getenv("DEBUG", "false").lower()
    if debug_mode:
        logger.info("Enabling asyncio's debugging mode.")

    # Run receive loop which receives zmq messages and sends back replies
    port = os.getenv("FLOWMACHINE_PORT", 5555)
    asyncio.run(
        recv(port), debug=debug_mode
    )  # note: asyncio.run() requires Python 3.7+
