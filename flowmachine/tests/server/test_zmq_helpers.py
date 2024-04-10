# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from unittest.mock import Mock, patch

import pytest
import rapidjson
from asynctest import CoroutineMock

from flowmachine.core.server.server import (
    calculate_and_send_reply_for_message,
    get_reply_for_message,
)
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
    assert expected_json == reply


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
        (
            '{"action": "ping", "params": {"DUMMY_PARAM":"DUMMY_PARAM_VALUE"}, "request_id": "DUMMY_REQUEST_ID"}',
            "Internal flowmachine server error: wrong arguments passed to handler for action 'ping'.",
        ),
    ],
)
@pytest.mark.asyncio
async def test_zmq_msg_parse_error(bad_message, expected_message, server_config):
    """Test errors are raised as expected when failing to parse zmq messages"""
    reply = await get_reply_for_message(msg_str=bad_message, config=server_config)
    assert reply["status"] == "error"
    assert reply["msg"] == expected_message


@pytest.mark.asyncio
async def test_generic_error_catch(server_config):
    """
    Test that calculate_and_send_reply_for_message sends a reply if get_reply_for_message raises an unexpected error
    """
    mock_socket = Mock()
    mock_socket.send_multipart = CoroutineMock()
    expected_response = [
        "DUMMY_RETURN_ADDRESS",
        b"",
        rapidjson.dumps(
            ZMQReply(status="error", msg="Could not get reply for message")
        ).encode(),
    ]
    with patch(
        "flowmachine.core.server.server.get_reply_for_message"
    ) as mock_get_reply:
        mock_get_reply.side_effect = Exception("Didn't see this one coming!")
        await calculate_and_send_reply_for_message(
            socket=mock_socket,
            return_address="DUMMY_RETURN_ADDRESS",
            msg_contents="DUMMY_MESSAGE",
            config=server_config,
        )
        mock_get_reply.assert_called_once()
        mock_socket.send_multipart.assert_called_once_with(expected_response)
