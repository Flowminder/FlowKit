import asyncio
import logging
import pytest

from .helpers import (
    cache_schema_is_empty,
    get_cache_tables,
    poll_until_done,
    send_message_and_get_reply,
)

logger = logging.getLogger("flowmachine").getChild(__name__)


def test_bail_out_if_pytest_asyncio_is_not_installed(event_loop):
    """
    This is a "smoke test" to check that the integration tests are being run under asyncio.
    We do this by checking for the existence of the 'event_loop' fixture, which only exists
    if pytest-asyncio is installed.
    """
    if not isinstance(event_loop, asyncio.AbstractEventLoop):
        raise RuntimeError(
            "Expecting 'event_loop' fixture from pytest-asyncio. Please ensure that pytest-asyncio is installed."
        )
    else:
        logger.debug("Confirming that pytest-asyncio is installed")


@pytest.mark.asyncio
async def test_run_query(zmq_url, fm_conn, redis):
    """
    Run daily_location query and check the resulting table contains the expected rows.
    """
    msg_run_query = {
        "action": "run_query",
        "query_kind": "daily_location",
        "params": {
            "date": "2016-01-01",
            "daily_location_method": "last",
            "aggregation_unit": "admin3",
            "subscriber_subset": "all",
        },
        "request_id": "DUMMY_ID",
    }
    expected_query_id = "ddc61a04f608dee16fff0655f91c2057"

    #
    # Check that we are starting with a clean slate (no cache tables, empty redis).
    #
    assert cache_schema_is_empty(fm_conn)
    assert not redis.exists(expected_query_id)

    #
    # Send message to run the daily_location query, check it was accepted
    # and a redis lookup was created for the query id.
    #
    reply = send_message_and_get_reply(zmq_url, msg_run_query)
    assert {"status": "accepted", "id": expected_query_id} == reply
    assert redis.exists(expected_query_id)

    #
    # Wait until the query has finished.
    #
    poll_until_done(zmq_url, expected_query_id)

    #
    # Check that a cache table for the query result was created
    # and that it contains the expected number of rows.
    #
    output_cache_table = f"x{expected_query_id}"
    implicit_cache_table = f"x7ea3c788cb7f5829ee2a69494e502765"
    assert [implicit_cache_table, output_cache_table] == get_cache_tables(fm_conn)
    num_rows = fm_conn.engine.execute(
        f"SELECT COUNT(*) FROM cache.{output_cache_table}"
    ).fetchone()[0]
    assert num_rows == 25

    #
    # In addition, check first few rows of the result are as expected.
    #

    first_few_rows_expected = [
        ("524 3 09 50", 18),
        ("524 5 13 67", 17),
        ("524 1 03 13", 20),
    ]
    first_few_rows = fm_conn.engine.execute(
        f"SELECT * FROM cache.{output_cache_table} LIMIT 3"
    ).fetchall()
    assert first_few_rows_expected == first_few_rows


@pytest.mark.parametrize(
    "params, expected_error_msg",
    [
        (
            {
                "date": "2000-88-99",
                "daily_location_method": "last",
                "aggregation_unit": "admin3",
                "subscriber_subset": "all",
            },
            "month must be in 1..12",
        ),
        (
            {
                "date": "2016-01-01",
                "daily_location_method": "FOOBAR",
                "aggregation_unit": "admin3",
                "subscriber_subset": "all",
            },
            "Unrecognised method 'FOOBAR', must be one of: ['last', 'most-common']",
        ),
        (
            {
                "date": "2016-01-01",
                "daily_location_method": "last",
                "aggregation_unit": "admin9999",
                "subscriber_subset": "all",
            },
            "Unrecognised level 'admin9999', must be one of: ['admin0', 'admin1', 'admin2', 'admin3', 'admin4']",
        ),
        (
            {
                "date": "2016-01-01",
                "daily_location_method": "last",
                "aggregation_unit": "admin3",
                "subscriber_subset": None,
            },
            "Cannot construct daily_location subset from given input: None",
        ),
    ],
)
@pytest.mark.asyncio
async def test_run_query_with_wrong_parameters(
    params, expected_error_msg, zmq_url, fm_conn
):
    """
    Run daily_location query and check the resulting table contains the expected rows.
    """
    msg_run_query = {
        "action": "run_query",
        "query_kind": "daily_location",
        "params": params,
        "request_id": "DUMMY_ID",
    }

    reply = send_message_and_get_reply(zmq_url, msg_run_query)
    expected_reason = f"Error when constructing query of kind daily_location with parameters {params}: '{expected_error_msg}'"
    assert "rejected" == reply["status"]
    assert expected_reason == reply["reason"]


@pytest.mark.skip(reason="Cannot currently test this because the sender hangs")
@pytest.mark.asyncio
async def test_wrongly_formatted_zmq_message(zmq_url):
    """
    """
    # msg = {"query_kind": "daily_location", "params": {"date": "2016-01-01", "daily_location_method": "last", "aggregation_unit": "admin3", "subscriber_subset": "all"}}
    msg = {
        "foo": "bar",
        "query_kind": "daily_location",
        "params": {
            "date": "2016-01-01",
            "daily_location_method": "last",
            "aggregation_unit": "admin3",
            "subscriber_subset": "all",
        },
    }

    reply = send_message_and_get_reply(zmq_url, msg)
    assert False
