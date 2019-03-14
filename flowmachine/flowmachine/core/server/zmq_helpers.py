# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import rapidjson
from enum import Enum

from .exceptions import FlowmachineServerError

__all__ = ["ZMQReplyStatus", "ZMQReplyMessage", "ZMQReplyData", "ZMQReply"]


class ZMQReplyStatus(str, Enum):
    """
    Valid status values for a zmq reply.
    """

    ACCEPTED = "accepted"
    DONE = "done"
    ERROR = "error"


class ZMQReplyMessage(str):
    """
    Class representing a zmq reply message. The input
    is automatically converted to a string if needed.
    """


class ZMQReplyData(dict):
    """
    Class representing data included in a zmq reply.
    The input is automatically converted to a dict.
    """

    def __init__(self, data):
        if data is None:
            data = {}

        super().__init__(data)


class ZMQReply:
    """
    Class representing a zmq reply.

    It has the following responsibilities:

      - Ensure that the reply status can only be one of the valid values defined in ZMQReplyStatus.
      - Ensure the JSON structure of the reply (as returned by the as_json() method) is consistent.
    """

    def __init__(self, status, msg="", data=None):
        """
        Parameters
        ----------
        status : str or
        """
        if msg == "" and data is None:
            raise ValueError(
                "At least one of the arguments 'msg', 'data' must be provided."
            )
        self.status = ZMQReplyStatus(status)
        self.msg = ZMQReplyMessage(msg)
        self.data = ZMQReplyData(data)

    def as_json(self):
        """
        Return a JSON object
        """
        return {"status": self.status.value, "msg": self.msg, "data": self.data}


def parse_zmq_message(msg_str):
    """
    Parse the message string and return

    Parameters
    ----------
    msg_str : str
        The message string as received from zmq. This must represent a valid
        JSON object containing the keys `action`, `params`, `request_id`.
        The values of `action` and `request_id` must be strings, while the
        value of `params` must be a dictionary. The `params` key may be omitted
        if the action handler does not expect any arguments.

        Example: {"action": "ping", params={}, "request_id": "<some_request_id>"}

    Returns
    -------
    tuple of str, dict
        Returns a pair of values containing a string representing the action
        to be performed and a dict with action parameters (which can be passed
        to the action handler).
    """
    # Load JSON from zmq message string.
    try:
        msg = rapidjson.loads(msg_str)
    except ValueError:
        raise FlowmachineServerError("Zmq message did not contain valid JSON.")

    # Ensure there are no unexpected keys present
    msg_keys = list(sorted(msg.keys()))
    if not set(msg_keys).issubset(["action", "params", "request_id"]):
        unexpected_keys = list(
            sorted(set(msg_keys).difference(["action", "params", "request_id"]))
        )
        raise FlowmachineServerError(
            f"Message contains unexpected key(s): {unexpected_keys}"
        )

    # Determine the action to be performed.
    try:
        action = msg["action"]
    except KeyError:
        raise FlowmachineServerError("Message does not contain expected key: 'action'.")
    if not isinstance(action, str):
        raise FlowmachineServerError("Action must be a string.")

    # Ensure request_id is present. TODO: This should be passed to the logger!
    try:
        request_id = msg["request_id"]
    except KeyError:
        raise FlowmachineServerError(
            "Message does not contain expected key: 'request_id'."
        )
    if not isinstance(request_id, str):
        raise FlowmachineServerError("Request id must be a string.")

    # Extract any params to be passed to the action handler.
    try:
        action_params = msg["params"]
    except KeyError:
        action_params = {}
    if not isinstance(action_params, dict):
        raise FlowmachineServerError("Action params must be a dictionary.")

    return action, action_params
