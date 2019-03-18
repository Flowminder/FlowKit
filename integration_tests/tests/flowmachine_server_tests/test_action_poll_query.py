import pytest

from flowmachine.core.server.utils import send_zmq_message_and_receive_reply


@pytest.mark.asyncio
async def test_poll_existing_query(zmq_port, zmq_host):
    """
    Polling a query with non-existent query id returns expected error.
    """
    expected_query_id = "dummy_query_d5d01a68ba6305f24a721b802341335b"
    msg = {
        "action": "run_query",
        "params": {"query_kind": "dummy_query", "dummy_param": "foobar"},
        "request_id": "DUMMY_ID",
    }
    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)
    expected_reply = {
        "status": "accepted",
        "msg": "",
        "data": {"query_id": expected_query_id},
    }
    assert expected_reply == reply

    msg = {
        "action": "poll_query",
        "params": {"query_id": expected_query_id},
        "request_id": "DUMMY_ID",
    }
    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)
    expected_reply = {
        "status": "done",
        "msg": "",
        "data": {"query_id": expected_query_id, "query_state": "completed"},
    }
    assert expected_reply == reply


@pytest.mark.xfail(
    reason="TODO: how to determine a missing query using QueryStateMachine?"
)
@pytest.mark.asyncio
async def test_poll_query_with_nonexistent_query_id_fails(zmq_port, zmq_host):
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
        "data": {"query_id": "FOOBAR", "query_state": "awol"},
        "id": "FOOBAR",
        "msg": "Unknown query id: FOOBAR",
    } == reply
