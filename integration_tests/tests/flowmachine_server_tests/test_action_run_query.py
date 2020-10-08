# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import asyncio
import logging
import pytest
import os

from flowmachine.core.cache import reset_cache
from flowmachine.core.server.utils import (
    send_zmq_message_and_receive_reply,
    send_zmq_message_and_await_reply,
)
from flowmachine.core.context import get_db
from flowmachine.core import make_spatial_unit
from flowmachine.core.dependency_graph import unstored_dependencies_graph
from flowmachine.features.location.spatial_aggregate import SpatialAggregate
from flowmachine.features.location.redacted_spatial_aggregate import (
    RedactedSpatialAggregate,
)
from flowmachine.features import daily_location
from .helpers import cache_schema_is_empty, get_cache_tables, poll_until_done

logger = logging.getLogger("flowmachine").getChild(__name__)


@pytest.mark.asyncio
async def test_run_query_nonblocking(zmq_port, zmq_host, fm_conn, redis):
    """
    Run two dummy queries to check that that are executed concurrently.
    """
    slow_dummy = {
        "action": "run_query",
        "params": {
            "query_kind": "dummy_query",
            "aggregation_unit": "admin3",
            "dummy_param": "slow_dummy",
            "dummy_delay": 10,
        },
        "request_id": "SLOW_DUMMY_ID",
    }
    fast_dummy = {
        "action": "run_query",
        "params": {
            "query_kind": "dummy_query",
            "aggregation_unit": "admin3",
            "dummy_param": "fast_dummy",
        },
        "request_id": "FAST_DUMMY_ID",
    }

    replies = [
        send_zmq_message_and_await_reply(dummy, port=zmq_port, host=zmq_host)
        for dummy in (slow_dummy, fast_dummy)
    ]
    for reply in asyncio.as_completed(replies):
        assert (await reply)["payload"]["query_id"] == "dummy_query_fast_dummy"
        break


def test_run_query(zmq_port, zmq_host, fm_conn, redis):
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
    q = RedactedSpatialAggregate(
        spatial_aggregate=SpatialAggregate(
            locations=daily_location(
                date="2016-01-01",
                method="last",
                spatial_unit=make_spatial_unit("admin", level=3),
                table=None,
                subscriber_subset=None,
            )
        )
    )
    expected_query_id = q.query_id

    #
    # Check that we are starting with a clean slate (no cache tables, empty redis).
    #
    reset_cache(get_db(), redis, protect_table_objects=False)
    assert cache_schema_is_empty(get_db())
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
    assert output_cache_table in get_cache_tables(get_db())
    num_rows = (
        get_db()
        .engine.execute(f"SELECT COUNT(*) FROM cache.{output_cache_table}")
        .fetchone()[0]
    )
    assert num_rows == 14

    #
    # In addition, check first few rows of the result are as expected.
    #

    first_few_rows_expected = [
        ("524 1 02 09", 26),
        ("524 1 03 13", 20),
        ("524 3 08 43", 35),
    ]
    first_few_rows = (
        get_db()
        .engine.execute(
            f"SELECT * FROM cache.{output_cache_table} ORDER BY pcod LIMIT 3"
        )
        .fetchall()
    )
    assert first_few_rows_expected == first_few_rows


def test_cache_content(
    start_flowmachine_server_with_or_without_dependency_caching, fm_conn, redis
):
    """
    Run a query with dependency caching turned on, and check that its dependencies are cached.
    Run a query with dependency caching turned off, and check that only the query itself is cached.
    """
    # Can't use the zmq_port fixture here as we're running against a different FlowMachine server
    zmq_port = os.getenv("FLOWMACHINE_PORT")

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
    q = RedactedSpatialAggregate(
        spatial_aggregate=SpatialAggregate(
            locations=daily_location(
                date="2016-01-01",
                method="last",
                spatial_unit=make_spatial_unit("admin", level=3),
                table=None,
                subscriber_subset=None,
            )
        )
    )

    # Get list of tables that should be cached
    expected_cache_tables = [q.table_name]
    if "false" == os.getenv("FLOWMACHINE_SERVER_DISABLE_DEPENDENCY_CACHING"):
        dependencies = unstored_dependencies_graph(q)
        for node, query_obj in dependencies.nodes(data="query_object"):
            try:
                schema, table_name = query_obj.fully_qualified_table_name.split(".")
                if schema == "cache":
                    expected_cache_tables.append(table_name)
            except NotImplementedError:
                # Some queries cannot be cached, and don't have table names
                pass

    # Check that we are starting with an empty cache.
    assert cache_schema_is_empty(get_db(), check_internal_tables_are_empty=False)

    # Send message to run the daily_location query, and check it was accepted
    reply = send_zmq_message_and_receive_reply(
        msg_run_query, port=zmq_port, host="localhost"
    )
    assert reply["status"] == "success"
    query_id = reply["payload"]["query_id"]

    # Wait until the query has finished.
    poll_until_done(zmq_port, query_id)

    # Check that the cache contains the correct tables.
    assert sorted(expected_cache_tables) == get_cache_tables(get_db())


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
            {"locations": {"date": ["Not a valid datetime."]}},
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
            {"locations": {"method": ["Must be one of: last, most-common."]}},
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
                "locations": {
                    "aggregation_unit": [
                        "Must be one of: admin0, admin1, admin2, admin3, lon-lat."
                    ]
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
            {"locations": {"subscriber_subset": ["Must be one of: None."]}},
        ),
    ],
)
def test_run_query_with_wrong_parameters(
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
    assert expected_error_messages == reply["payload"]["validation_error_messages"]


def test_wrongly_formatted_zmq_message(zmq_port, zmq_host):
    """"""
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
    assert "error" == reply["status"]
    assert "Invalid action request." == reply["msg"]
