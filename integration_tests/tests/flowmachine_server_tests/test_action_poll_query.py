import pytest

from flowmachine.core.server.utils import send_zmq_message_and_receive_reply
from .helpers import poll_until_done


def test_poll_existing_query(zmq_port, zmq_host):
    """
    Polling an existing query id returns expected reply.
    """
    expected_query_id = "dummy_query_foobar"
    msg = {
        "action": "run_query",
        "params": {
            "query_kind": "dummy_query",
            "dummy_param": "foobar",
            "aggregation_unit": "admin3",
        },
        "request_id": "DUMMY_ID",
    }
    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)
    expected_reply = {
        "status": "success",
        "msg": "",
        "payload": {"completed": [1, 1], "query_id": expected_query_id},
    }
    assert expected_reply == reply

    # Poll until done to ensure we don't send the poll message until the query state has finished updating.
    poll_until_done(zmq_port, expected_query_id)

    msg = {
        "action": "poll_query",
        "params": {"query_id": expected_query_id},
        "request_id": "DUMMY_ID",
    }
    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)
    expected_reply = {
        "status": "success",
        "msg": "",
        "payload": {
            "query_id": expected_query_id,
            "query_kind": "dummy_query",
            "query_state": "completed",
            "completed": [1, 1],
        },
    }
    assert expected_reply == reply


def test_poll_query_with_nonexistent_query_id_fails(zmq_port, zmq_host):
    """
    Polling a query with non-existent query id returns expected error.
    """
    msg = {
        "action": "poll_query",
        "params": {"query_id": "FOOBAR"},
        "request_id": "DUMMY_ID",
    }

    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)
    assert {
        "status": "error",
        "payload": {"query_id": "FOOBAR", "query_state": "awol"},
        "msg": "Unknown query id: 'FOOBAR'",
    } == reply
