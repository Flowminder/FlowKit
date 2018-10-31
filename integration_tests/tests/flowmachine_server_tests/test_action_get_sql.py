import pytest

from .helpers import poll_until_done, send_message_and_get_reply


# TODO: add test for code path that raises QueryProxyError with the 'get_sql' action


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
        "query_kind": "daily_location",
        "params": {
            "date": "2016-01-01",
            "daily_location_method": "last",
            "aggregation_unit": "admin3",
            "subscriber_subset": "all",
        },
    }
    expected_query_id = "4a12fae60ce647d01b2bd5902b8a48e7"

    reply = send_message_and_get_reply(zmq_url, msg_run_query)
    assert {"status": "accepted", "id": expected_query_id} == reply

    #
    # Wait until the query has finished.
    #
    poll_until_done(zmq_url, expected_query_id)

    #
    # Get query result.
    #
    msg_get_sql = {"action": "get_sql", "query_id": expected_query_id}

    reply = send_message_and_get_reply(zmq_url, msg_get_sql)
    assert (
        "SELECT name,total FROM cache.x4a12fae60ce647d01b2bd5902b8a48e7" == reply["sql"]
    )


@pytest.mark.asyncio
async def test_get_sql_for_nonexistent_query_id(zmq_url):
    """
    Polling a query with non-existent query id returns expected error.
    """
    #
    # Try getting query result for nonexistent ID.
    #
    msg_get_sql = {"action": "get_sql", "query_id": "FOOBAR"}

    reply = send_message_and_get_reply(zmq_url, msg_get_sql)
    assert {"status": "awol", "id": "FOOBAR"} == reply
