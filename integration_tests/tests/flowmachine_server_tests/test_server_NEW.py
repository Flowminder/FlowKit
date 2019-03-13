def test_ping_flowmachine_server(send_zmq_message_and_receive_reply):
    """
    Sending the 'ping' action to the flowmachine server evokes a successful 'pong' response.
    """
    msg = {"action": "ping", "request_id": "DUMMY_ID"}
    reply = send_zmq_message_and_receive_reply(msg)
    expected_reply = {"status": "accepted", "msg": "pong", "data": {}}
    assert expected_reply == reply


def test_unknown_action_returns_error(send_zmq_message_and_receive_reply):
    """
    Unknown action returns an error response.
    """
    msg = {"action": "foobar", "request_id": "DUMMY_ID"}
    reply = send_zmq_message_and_receive_reply(msg)
    expected_reply = {"status": "error", "msg": "Unknown action: 'foobar'", "data": {}}
    assert expected_reply == reply
