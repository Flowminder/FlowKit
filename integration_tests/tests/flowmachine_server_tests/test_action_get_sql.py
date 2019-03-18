import pytest

from flowmachine.core.server.utils import send_zmq_message_and_receive_reply
from .helpers import poll_until_done


# TODO: add test for code path that raises QueryProxyError with the 'get_sql' action


@pytest.mark.asyncio
async def test_get_sql(zmq_port, zmq_host):
    """
    Running 'get_sql' on finished query returns the expected result.
    """
    #
    # Run daily_location query.
    #
    msg = {
        "action": "run_query",
        "params": {
            "query_kind": "daily_location",
            "date": "2016-01-01",
            "method": "last",
            "aggregation_unit": "admin3",
            "subscriber_subset": None,
        },
        "request_id": "DUMMY_ID",
    }
    expected_query_id = "e39b0d45bc6b46b7700c67cd52f00455"

    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)
    # assert reply["status"] in ("executing", "queued", "completed")
    assert reply["status"] in ("accepted")

    #
    # Wait until the query has finished.
    #
    poll_until_done(zmq_port, expected_query_id)

    #
    # Get query result.
    #
    msg = {
        "action": "get_sql_for_query_result",
        "params": {"query_id": expected_query_id},
        "request_id": "DUMMY_ID",
    }
    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)
    assert "done" == reply["status"]
    assert f"SELECT * FROM cache.x{expected_query_id}" == reply["data"]["sql"]


@pytest.mark.asyncio
async def test_get_sql_for_nonexistent_query_id(zmq_port, zmq_host):
    """
    Polling a query with non-existent query id returns expected error.
    """
    #
    # Try getting query result for nonexistent ID.
    #
    msg = {
        "action": "get_sql_for_query_result",
        "params": {"query_id": "FOOBAR"},
        "request_id": "DUMMY_ID",
    }

    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)
    expected_reply = {
        "status": "error",
        "msg": "Unknown query id: 'FOOBAR'",
        "data": {"query_id": "FOOBAR", "query_state": "awol"},
    }
    assert expected_reply == reply
