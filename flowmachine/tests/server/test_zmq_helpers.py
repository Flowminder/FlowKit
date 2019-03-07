# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core.server.zmq_helpers import *


def test_invalid_reply_status_raises_error():
    """
    Initialising ZMQReplyStatus with an invalid status string raises an error.
    """
    with pytest.raises(ValueError, match="'foobar' is not a valid ZMQReplyStatus"):
        ZMQReplyStatus("foobar")


def test_zmq_reply_message_valid_input():
    """
    Input to ZMQReplyMessage is converted to a string.
    """
    msg = ZMQReplyMessage("foobar")
    assert "foobar" == msg

    msg = ZMQReplyMessage(42)
    assert "42" == msg

    msg = ZMQReplyMessage({"a": 1, "b": 2})
    assert "{'a': 1, 'b': 2}" == msg


def test_zmq_reply_data_valid_input():
    """
    Input to ZMQReplyData is converted to a dict.
    """
    zmq_reply_data = ZMQReplyData({"a": 1})
    assert {"a": 1} == zmq_reply_data

    # List of tuples is converted to a dict
    zmq_reply_data = ZMQReplyData([("b", 2), ("c", 3)])
    assert {"b": 2, "c": 3} == zmq_reply_data

    # None is converted to an empty dict
    zmq_reply_data = ZMQReplyData(None)
    assert {} == zmq_reply_data


def test_zmq_reply_data_raises_error_for_invalid_input():
    """
    Initialising ZMQReplyData with invalid input raises an error.
    """
    with pytest.raises(ValueError):
        some_string = "this is not a valid dict"
        ZMQReplyData(some_string)

    with pytest.raises(ValueError):
        some_list_of_dicts = [{"a": 1}, {"b": 2}]
        ZMQReplyData(some_list_of_dicts)


def test_zmq_reply_as_json():
    """
    ZMQReply has the expected structure when converted to JSON.
    """
    reply = ZMQReply("accepted", msg="foobar", data={"a": 1, "b": 2})
    expected_json = {"status": "accepted", "msg": "foobar", "data": {"a": 1, "b": 2}}
    assert expected_json == reply.as_json()
