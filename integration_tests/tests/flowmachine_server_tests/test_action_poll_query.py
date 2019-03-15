import pytest

from .helpers import send_message_and_get_reply


@pytest.mark.asyncio
async def test_poll_existing_query(zmq_url):
    # def test_poll_query_with_nonexistent_query_id_fails(zmq_url):
    """
    Polling a query with non-existent query id returns expected error.
    """
    msg = {
        "action": "run_query",
        "params": {"query_kind": "dummy_query", "dummy_param": "foobar"},
        "request_id": "DUMMY_ID",
    }
    reply = send_message_and_get_reply(zmq_url, msg)
    expected_reply = {
        "status": "accepted",
        "msg": "",
        "data": {"query_id": "7a2553620227ccd7e4b3ef6a1e0aa9aa"},
    }
    assert expected_reply == reply

    msg = {
        "action": "poll_query",
        "params": {
            "query_id": "7a2553620227ccd7e4b3ef6a1e0aa9aa",
            "query_kind": "dummy_query",
        },
        "request_id": "DUMMY_ID",
    }
    reply = send_message_and_get_reply(zmq_url, msg)
    expected_reply = {
        "status": "done",
        "msg": "",
        "data": {"query_id": "7a2553620227ccd7e4b3ef6a1e0aa9aa", "status": "completed"},
    }
    assert expected_reply == reply


@pytest.mark.asyncio
async def test_poll_query_with_nonexistent_query_id_fails(zmq_url):
    """
    Polling a query with non-existent query id returns expected error.
    """
    msg = {"action": "poll", "query_id": "FOOBAR", "request_id": "DUMMY_ID"}

    reply = send_message_and_get_reply(zmq_url, msg)
    assert {
        "status": "awol",
        "id": "FOOBAR",
        "error": "Unknown query id: FOOBAR",
    } == reply
