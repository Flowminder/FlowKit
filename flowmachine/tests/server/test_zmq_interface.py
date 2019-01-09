import pytest

from flowmachine.core.server.zmq_interface import ZMQMultipartMessage, ZMQInterfaceError


def test_create_zmq_msg_from_multipart_message():
    """
    Can construct a ZMQMultipartMessage and its public attributes contain the expected values.
    """
    msg_contents = b'{"action": "dummy_action", "request_id": "DUMMY_API_REQUEST_ID", "query_kind": "foobar", "param1": "some_value", "param2": "another_value"}'
    multipart_msg = (b"DUMMY_RETURN_ADDRESS", b"", msg_contents)
    zmq_msg = ZMQMultipartMessage(multipart_msg)

    # Check that the resulting zmq_msg has the expected
    assert zmq_msg.action == "dummy_action"
    assert zmq_msg.api_request_id == "DUMMY_API_REQUEST_ID"
    assert zmq_msg.action_params == {
        "query_kind": "foobar",
        "param1": "some_value",
        "param2": "another_value",
    }
    assert zmq_msg.return_address == b"DUMMY_RETURN_ADDRESS"
    assert zmq_msg.msg_str == msg_contents


@pytest.mark.parametrize(
    "multipart_msg",
    [
        (b"DUMMY_RETURN_ADDRESS", b'{"action": "dummy_action"}'),  # missing separator
        (b"", b'{"action": "dummy_action"}'),  # missing return address
        (b"DUMMY_RETURN_ADDRESS", b""),  # missing message contents
        (b"DUMMY_RETURN_ADDRESS",),  # only return address
        (
            b"DUMMY_RETURN_ADDRESS",
            b"",
            b'{"action": "dummy_action"}',
            b'{"param1": "some_value"}',
        ),  # too many parts
    ],
)
def test_wrong_number_of_parts(multipart_msg):
    """
    Constructing a ZMQMultipartMessage from tuple of the wrong length raises an error.
    """
    with pytest.raises(
        ZMQInterfaceError,
        match="Multipart message is not of the form \(<return_address>, <empty_delimiter>, <message>\)",
    ):
        _ = ZMQMultipartMessage(multipart_msg)


def test_nonempty_delimiter():
    """
    Non-empty delimiter in multipart message raises an error.
    """
    multipart_msg = (
        b"DUMMY_RETURN_ADDRESS",
        b"THIS_SHOULD_BE_EMPTY_BUT_IS_NOT",
        b'{"action": "dummy_action"}',
    )
    with pytest.raises(
        ZMQInterfaceError, match="Expected empty delimiter in multipart message"
    ):
        _ = ZMQMultipartMessage(multipart_msg)


def test_invalid_json_in_message_contents():
    """
    Invalid JSON in message contents raises an error.
    """
    multipart_msg = (b"DUMMY_RETURN_ADDRESS", b"", b"THIS_IS_NOT_VALID_JSON")
    with pytest.raises(ZMQInterfaceError, match="Message does not contain valid JSON"):
        _ = ZMQMultipartMessage(multipart_msg)


def test_missing_action_key_in_message_contents():
    """
    Missing 'action' key in message contents raises an error.
    """
    multipart_msg = (b"DUMMY_RETURN_ADDRESS", b"", b'{"param1": "some_value"}')
    with pytest.raises(
        ZMQInterfaceError, match="Message does not contain expected key 'action'"
    ):
        _ = ZMQMultipartMessage(multipart_msg)


def test_missing_request_id_key_in_message_contents():
    """
    Missing 'request_id' key in message contents raises an error.
    """
    multipart_msg = (
        b"DUMMY_RETURN_ADDRESS",
        b"",
        b'{"action": "dummy_action", "query_kind": "foobar", "param1": "some_value"}',
    )
    with pytest.raises(
        ZMQInterfaceError, match="Message does not contain expected key 'request_id'"
    ):
        _ = ZMQMultipartMessage(multipart_msg)
