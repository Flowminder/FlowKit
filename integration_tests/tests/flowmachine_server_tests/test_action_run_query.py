import logging
import pytest

from flowmachine.core.server.utils import send_zmq_message_and_receive_reply
from .helpers import cache_schema_is_empty, get_cache_tables, poll_until_done

logger = logging.getLogger("flowmachine").getChild(__name__)


@pytest.mark.asyncio
async def test_run_query(zmq_port, zmq_host, fm_conn, redis):
    """
    Run daily_location query and check the resulting table contains the expected rows.
    """
    msg_run_query = {
        "action": "run_query",
        "params": {
            "query_kind": "spatial_aggregate",
            "locations": {
                "query_kind": "daily_location",
                "date": "2016-01-01",
                "method": "last",
                "aggregation_unit": "admin3",
                "subscriber_subset": None,
            },
        },
        "request_id": "DUMMY_ID",
    }
    expected_query_id = "a2fb1efca05a42d558e8c613970262de"

    #
    # Check that we are starting with a clean slate (no cache tables, empty redis).
    #
    assert cache_schema_is_empty(fm_conn)
    assert not redis.exists(expected_query_id)

    #
    # Send message to run the daily_location query, check it was accepted
    # and a redis lookup was created for the query id.
    #
    reply = send_zmq_message_and_receive_reply(
        msg_run_query, port=zmq_port, host=zmq_host
    )
    # assert reply["status"] in ("executing", "queued", "completed")
    assert reply["status"] in ("success")
    assert expected_query_id == reply["payload"]["query_id"]
    # assert redis.exists(expected_query_id)

    #
    # Wait until the query has finished.
    #
    poll_until_done(zmq_port, expected_query_id)

    #
    # Check that a cache table for the query result was created
    # and that it contains the expected number of rows.
    #
    output_cache_table = f"x{expected_query_id}"
    assert [output_cache_table] == get_cache_tables(fm_conn)
    num_rows = fm_conn.engine.execute(
        f"SELECT COUNT(*) FROM cache.{output_cache_table}"
    ).fetchone()[0]
    assert num_rows == 25

    #
    # In addition, check first few rows of the result are as expected.
    #

    first_few_rows_expected = [
        ("524 1 01 04", 13),
        ("524 1 02 09", 26),
        ("524 1 03 13", 20),
    ]
    first_few_rows = fm_conn.engine.execute(
        f"SELECT * FROM cache.{output_cache_table} ORDER BY pcod LIMIT 3"
    ).fetchall()
    assert first_few_rows_expected == first_few_rows


@pytest.mark.parametrize(
    "params, expected_error_messages",
    [
        (
            {
                "query_kind": "spatial_aggregate",
                "locations": {
                    "query_kind": "daily_location",
                    "date": "2000-88-99",
                    "method": "last",
                    "aggregation_unit": "admin3",
                    "subscriber_subset": None,
                },
            },
            {"0": {"locations": {"0": {"date": ["Not a valid date."]}}}},
        ),
        (
            {
                "query_kind": "spatial_aggregate",
                "locations": {
                    "query_kind": "daily_location",
                    "date": "2016-01-01",
                    "method": "FOOBAR",
                    "aggregation_unit": "admin3",
                    "subscriber_subset": None,
                },
            },
            {
                "0": {
                    "locations": {
                        "0": {"method": ["Must be one of: last, most-common."]}
                    }
                }
            },
        ),
        (
            {
                "query_kind": "spatial_aggregate",
                "locations": {
                    "query_kind": "daily_location",
                    "date": "2016-01-01",
                    "method": "last",
                    "aggregation_unit": "admin9999",
                    "subscriber_subset": None,
                },
            },
            {
                "0": {
                    "locations": {
                        "0": {
                            "aggregation_unit": [
                                "Must be one of: admin0, admin1, admin2, admin3."
                            ]
                        }
                    }
                }
            },
        ),
        (
            {
                "query_kind": "spatial_aggregate",
                "locations": {
                    "query_kind": "daily_location",
                    "date": "2016-01-01",
                    "method": "last",
                    "aggregation_unit": "admin3",
                    "subscriber_subset": "virtually_all_subscribers",
                },
            },
            {
                "0": {
                    "locations": {"0": {"subscriber_subset": ["Must be one of: None."]}}
                }
            },
        ),
    ],
)
@pytest.mark.asyncio
async def test_run_query_with_wrong_parameters(
    params, expected_error_messages, zmq_port, zmq_host
):
    """
    Run daily_location query and check that the resulting table contains the expected rows.
    """
    msg_run_query = {"action": "run_query", "params": params, "request_id": "DUMMY_ID"}

    reply = send_zmq_message_and_receive_reply(
        msg_run_query, port=zmq_port, host=zmq_host
    )
    # expected_reason = f"Error when constructing query of kind daily_location with parameters {params}: '{expected_error_msg}'"
    # expected_reason = "Message contains unexpected key(s): ['query_kind'], 'data': {}"
    assert "error" == reply["status"]
    assert expected_error_messages == reply["payload"]


@pytest.mark.skip(reason="Cannot currently test this because the sender hangs")
@pytest.mark.asyncio
async def test_wrongly_formatted_zmq_message(zmq_port, zmq_host):
    """
    """
    msg = {
        "foo": "bar",
        "params": {
            "query_kind": "daily_location",
            "date": "2016-01-01",
            "method": "last",
            "aggregation_unit": "admin3",
            "subscriber_subset": None,
        },
        "request_id": "DUMMY_ID",
    }

    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)
    assert False
