import pytest

from .helpers import poll_until_done, send_message_and_get_reply


# TODO: add test for code path that raises QueryProxyError with the 'get_sql' action


@pytest.mark.skip(reason="The 'get_sql' action will likely be removed soon.")
@pytest.mark.asyncio
async def test_get_sql(zmq_url):
    """
    Running 'get_sql' on finished query returns the expected result.
    """
    #
    # Run daily_location query.
    #
    msg_run_query = {
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

    reply = send_message_and_get_reply(zmq_url, msg_run_query)
    # assert reply["status"] in ("executing", "queued", "completed")
    assert reply["status"] in ("accepted")

    #
    # Wait until the query has finished.
    #
    poll_until_done(zmq_url, expected_query_id)

    #
    # Get query result.
    #
    msg_get_sql = {
        "action": "get_sql",
        "params": {"query_id": expected_query_id},
        "request_id": "DUMMY_ID",
    }

    reply = send_message_and_get_reply(zmq_url, msg_get_sql)
    assert "done" == reply["status"]
    assert f"SELECT * FROM cache.x{expected_query_id}" == reply["data"]["sql"]


@pytest.mark.skip(reason="The 'get_sql' action will likely be removed soon.")
@pytest.mark.asyncio
async def test_get_sql_for_nonexistent_query_id(zmq_url):
    """
    Polling a query with non-existent query id returns expected error.
    """
    #
    # Try getting query result for nonexistent ID.
    #
    msg_get_sql = {
        "action": "get_sql",
        "params": {"query_id": "FOOBAR"},
        "request_id": "DUMMY_ID",
    }

    reply = send_message_and_get_reply(zmq_url, msg_get_sql)
    assert {
        "status": "awol",
        "id": "FOOBAR",
        "error": "Unknown query id: FOOBAR",
    } == reply
