# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

#
# Note: the class ZMQReply (and its helper classes) in this file mirror the analogous classes
# in flowmachine.core.server.zmq_helpers and flowapi/zmq_helpers.py. The purpose of this
# is to ensure that messages sent back and forth have a consistent structure. If changes are
# made to either file they should also be reflected in the others.
#

from enum import Enum

__all__ = ["ZMQReply"]


class ZMQReplyStatus(str, Enum):
    """
    Valid status values for a zmq reply.
    """

    SUCCESS = "success"
    ERROR = "error"


class ZMQReplyMessage(str):
    """
    Class representing a zmq reply message. The input
    is automatically converted to a string if needed.
    """


class ZMQReplyPayload(dict):
    """
    Class representing payload included in a zmq reply.
    The input is automatically converted to a dict.
    """

    def __init__(self, payload):
        if payload is None:
            payload = {}

        super().__init__(payload)


class ZMQReply:
    """
    Class representing a zmq reply.

    It has the following responsibilities:

      - Ensure that the reply status can only be one of the valid values defined in ZMQReplyStatus.
      - Ensure the JSON structure of the reply (as returned by the as_json() method) is consistent.
    """

    def __init__(self, status, msg="", payload=None):
        """
        Parameters
        ----------
        status : str or
        """
        if msg == "" and payload is None:
            raise ValueError(
                "At least one of the arguments 'msg', 'payload' must be provided."
            )
        self.status = ZMQReplyStatus(status)
        self.msg = ZMQReplyMessage(msg)
        self.payload = ZMQReplyPayload(payload)

    def as_json(self):
        """
        Return a JSON object
        """
        return {"status": self.status.value, "msg": self.msg, "payload": self.payload}
