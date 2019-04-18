# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core.server.server import get_reply_for_message
from flowmachine.core.server.zmq_helpers import *


def test_one_kwarg_required():
    """
    Initialising ZMQReply with both default kwargs is a valueerror.
    """
    with pytest.raises(ValueError):
        ZMQReply("foo")


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


def test_zmq_reply_payload_valid_input():
    """
    Input to ZMQReplyPayload is converted to a dict.
    """
    zmq_reply_payload = ZMQReplyPayload({"a": 1})
    assert {"a": 1} == zmq_reply_payload

    # List of tuples is converted to a dict
    zmq_reply_payload = ZMQReplyPayload([("b", 2), ("c", 3)])
    assert {"b": 2, "c": 3} == zmq_reply_payload

    # None is converted to an empty dict
    zmq_reply_payload = ZMQReplyPayload(None)
    assert {} == zmq_reply_payload


def test_zmq_reply_payload_raises_error_for_invalid_input():
    """
    Initialising ZMQReplypayload with invalid input raises an error.
    """
    with pytest.raises(ValueError):
        some_string = "this is not a valid dict"
        ZMQReplyPayload(some_string)

    with pytest.raises(ValueError):
        some_list_of_dicts = [{"a": 1}, {"b": 2}]
        ZMQReplyPayload(some_list_of_dicts)


def test_zmq_reply_as_json():
    """
    ZMQReply has the expected structure when converted to JSON.
    """
    reply = ZMQReply("success", msg="foobar", payload={"a": 1, "b": 2})
    expected_json = {"status": "success", "msg": "foobar", "payload": {"a": 1, "b": 2}}
    assert expected_json == reply.as_json()


@pytest.mark.parametrize(
    "bad_message, expected_message",
    [
        ("NOT_JSON", "Invalid JSON."),
        (
            '{"action": "DUMMY_ACTION", "params": {}, "request_id": "DUMMY_REQUEST_ID", "EXTRA_KEY": "EXTRA_KEY_VALUE"}',
            "Invalid action request.",
        ),
        ('{"params": {}, "request_id": "DUMMY_REQUEST_ID"}', "Invalid action request."),
        ('{"action": "DUMMY_ACTION", "params": {}}', "Invalid action request."),
        ('{"action": -1, "params": {}}', "Invalid action request."),
        (
            '{"action": "DUMMY_ACTION", "params": "NOT_A_DICT", "request_id": "DUMMY_REQUEST_ID"}',
            "Invalid action request.",
        ),
        (
            '{"action": "DUMMY_ACTION", "params": {}, "request_id": -1}',
            "Invalid action request.",
        ),
    ],
)
def test_zmq_msg_parse_error(bad_message, expected_message):
    """Test errors are raised as expected when failing to parse zmq messages"""
    reply = get_reply_for_message(bad_message)
    assert reply["status"] == "error"
    assert reply["msg"] == expected_message
