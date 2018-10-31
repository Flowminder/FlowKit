import pytest

from .helpers import send_message_and_get_reply


@pytest.mark.asyncio
async def test_poll_query_with_nonexistent_query_id_fails(zmq_url):
    """
    Polling a query with non-existent query id returns expected error.
    """
    msg = {"action": "poll", "query_id": "FOOBAR"}

    reply = send_message_and_get_reply(zmq_url, msg)
    assert {"status": "awol", "id": "FOOBAR"} == reply
