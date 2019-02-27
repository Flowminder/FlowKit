import pytest

from .helpers import poll_until_done, send_message_and_get_reply


# TODO: add test for code path that raises QueryProxyError with the 'get_params' action


@pytest.mark.parametrize(
    "params",
    [
        {
            "date": "2016-01-01",
            "daily_location_method": "last",
            "aggregation_unit": "admin3",
            "subscriber_subset": "all",
        },
        {
            "date": "2016-01-04",
            "daily_location_method": "most-common",
            "aggregation_unit": "admin1",
            "subscriber_subset": "all",
        },
    ],
)
@pytest.mark.asyncio
async def test_get_query_kind(params, zmq_url):
    """
    Running 'get_params' against an existing query_id returns the expected parameters with which the query was run.
    """
    #
    # Run daily_location query.
    #
    msg_run_query = {
        "action": "run_query",
        "query_kind": "daily_location",
        "params": params,
        "request_id": "DUMMY_ID",
    }

    reply = send_message_and_get_reply(zmq_url, msg_run_query)
    query_id = reply["id"]
    assert reply["status"] in ("executing", "queued", "executed")

    #
    # Wait until the query has finished.
    #
    poll_until_done(zmq_url, query_id)

    #
    # Get query result.
    #
    msg_get_params = {
        "action": "get_query_kind",
        "query_id": query_id,
        "request_id": "DUMMY_ID",
    }

    reply = send_message_and_get_reply(zmq_url, msg_get_params)
    assert {"id": query_id, "query_kind": "daily_location"} == reply


@pytest.mark.asyncio
async def test_get_query_kind_for_nonexistent_query_id(zmq_url):
    """
    Running 'get_params' on a non-existent query id returns an error.
    """
    #
    # Try getting query result for nonexistent ID.
    #
    msg_get_sql = {
        "action": "get_query_kind",
        "query_id": "FOOBAR",
        "request_id": "DUMMY_ID",
    }

    reply = send_message_and_get_reply(zmq_url, msg_get_sql)
    assert {
        "status": "awol",
        "id": "FOOBAR",
        "error": "Unknown query id: FOOBAR",
    } == reply
