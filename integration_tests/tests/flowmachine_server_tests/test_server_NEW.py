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


def test_get_available_queries(send_zmq_message_and_receive_reply):
    """
    Action 'get_available_queries' returns list of available queries.
    """
    msg = {"action": "get_available_queries", "request_id": "DUMMY_ID"}
    reply = send_zmq_message_and_receive_reply(msg)
    expected_reply = {
        "status": "accepted",
        "msg": "",
        "data": {"available_queries": ["daily_location", "modal_location"]},
    }
    assert expected_reply == reply
