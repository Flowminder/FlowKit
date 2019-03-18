# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import zmq

__all__ = ["send_zmq_message_and_receive_reply", "FM_EXAMPLE_MESSAGE"]


def send_zmq_message_and_receive_reply(msg, port=5555, host="localhost"):
    """
    Helper function to send JSON messages to the flowmachine server (via zmq) and receive a reply.

    This is mainly useful for interactive testing and debugging.

    Parameters
    ----------
    msg : dict
        Dictionary representing a valid zmq message.

    port : int or str
        Port on which the flowmachine server is running (default: 5555)

    host : str
        The host on which the flowmachine server is running (default: 'localhost')


    Example
    -------

    >>> msg = {
    ...     "action": "run_query_OLD",
    ...     "query_kind": "daily_location",
    ...     "request_id": "DUMMY_ID",
    ...      "params": {"date": "2016-01-01", "method": "last", "aggregation_unit": "admin3", "subscriber_subset": None}
    ... }

    >>> send_zmq_message_and_receive_reply(msg)
    {'status': 'accepted', 'id': 'ddc61a04f608dee16fff0655f91c2057'}

    >>> send_zmq_message_and_receive_reply({"action": "get_sql", "request_id": "DUMMY_ID", "query_id": "ddc61a04f608dee16fff0655f91c2057"})
    {'status': 'done', 'sql': 'SELECT * FROM cache.xddc61a04f608dee16fff0655f91c2057'}
    """
    context = zmq.Context.instance()
    socket = context.socket(zmq.REQ)
    socket.connect(f"tcp://{host}:{port}")
    print(f"Sending message: {msg}")
    socket.send_json(msg)
    reply = socket.recv_json()
    return reply


FM_EXAMPLE_MESSAGE = {
    "action": "run_query",
    "params": {
        "query_kind": "daily_location",
        "date": "2016-01-01",
        "method": "last",
        "aggregation_unit": "admin3",
        "subscriber_subset": None,
    },
    "request_id": "DUMMY_ID",
}
