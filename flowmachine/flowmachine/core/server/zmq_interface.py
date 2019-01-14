import asyncio
import logging
from json import dumps, loads, JSONDecodeError

logger = logging.getLogger("flowmachine").getChild(__name__)


class ZMQInterfaceError(Exception):
    """
    Custom exception to indicate an error when receiving or sending
    flowmachine-related messages via zmq.
    """


class ZMQMultipartMessage:
    """
    Helper class which encapsulates the receiving and sending of multipart messages via zeromq.
    This serves as the external connection point between flowmachine and the external zmq loop.

    Its responsibility is to receive a multipart message obtained from ZMQ,
    deconstruct it into the return address and the actual message, and to
    send back replies over the socket.
    """

    def __init__(self, multipart_msg):
        # Deconstruct multipart message into return address and the actual message
        self.return_address, self.msg_str = self._get_parts(multipart_msg)
        self.action, self.action_params, self.api_request_id = self._deconstruct_message_string(
            self.msg_str
        )

    def send_reply_async(self, socket, reply_coroutine):
        asyncio.create_task(send_reply(socket, self.return_address, reply_coroutine))

    def _get_parts(self, multipart_msg):
        """
        Validate that the incoming multipart_msg contains three parts (and that the middle
        part is an empty delimiter) and return the first and third part which represent
        the return address and the message contents, respectively.

        Returns
        -------
        pair of str
            The pair `(return_address, msg_contents)`.
        """
        if len(multipart_msg) != 3:
            error_msg = f"Multipart message is not of the form (<return_address>, <empty_delimiter>, <message>): {multipart_msg}"
            logger.error(error_msg)
            raise ZMQInterfaceError(error_msg)

        return_address, empty_delimiter, msg_str = multipart_msg

        if empty_delimiter != b"":
            error_msg = (
                f"Expected empty delimiter in multipart message, got: {empty_delimiter}"
            )
            logger.error(error_msg)
            raise ZMQInterfaceError(error_msg)

        return return_address, msg_str

    def _deconstruct_message_string(self, msg_str):
        try:
            msg = loads(msg_str)
        except (TypeError, JSONDecodeError):
            error_msg = f"Message does not contain valid JSON: {msg_str}"
            logger.debug(error_msg)
            raise ZMQInterfaceError(error_msg)

        try:
            key = "action"
            action = msg.pop(key)
            key = "request_id"
            api_request_id = msg.pop(key)
            action_params = (
                msg
            )  # everything except "action" and "request_id" is considered a parameter
        except KeyError:
            error_msg = f"Message does not contain expected key '{key}': {msg_str}"
            logger.debug(error_msg)
            raise ZMQInterfaceError(error_msg)

        return action, action_params, api_request_id


async def send_reply(socket, return_address, reply_coroutine):
    """

    Parameters
    ----------
    socket : zmq.asyncio.Socket
        zmq socket to use for sending the message
    reply_coroutine : awaitable
        Coroutine which will eventually return a dict

    Returns
    -------
    None
    """
    logger.debug(f"Awaiting {reply_coroutine}")
    reply = await reply_coroutine
    logger.debug(f"Returning message {reply} to {return_address}")
    socket.send_multipart([return_address, b"", dumps(reply).encode()])
    logger.debug(f"Sent {[return_address, b'', dumps(reply).encode()]}")
