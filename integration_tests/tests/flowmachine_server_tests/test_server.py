# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import json

from flowmachine.core import make_spatial_unit
from flowmachine.core.server.utils import send_zmq_message_and_receive_reply
from flowmachine.features import ModalLocation, daily_location
from flowmachine.features.dfs.total_amount_for_metric import DFSTotalMetricAmount
from flowmachine.features.location.redacted_spatial_aggregate import (
    RedactedSpatialAggregate,
)
from flowmachine.features.location.spatial_aggregate import SpatialAggregate
from flowmachine.utils import sort_recursively

from .helpers import poll_until_done


def test_ping_flowmachine_server(zmq_host, zmq_port):
    """
    Sending the 'ping' action to the flowmachine server evokes a successful 'pong' response.
    """

    msg = {"action": "ping", "request_id": "DUMMY_ID"}
    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)
    expected_reply = {"status": "success", "msg": "pong", "payload": {}}
    assert expected_reply == reply


def test_unknown_action_returns_error(zmq_host, zmq_port):
    """
    Unknown action returns an error response.
    """
    msg = {"action": "foobar", "request_id": "DUMMY_ID"}
    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)
    expected_reply = {
        "status": "error",
        "msg": "Invalid action request.",
        "payload": {
            "action": [
                "Must be one of: ping, get_available_queries, get_query_schemas, run_query, poll_query, get_query_kind, get_query_params, get_sql_for_query_result, get_geo_sql_for_query_result, get_geography, get_available_dates."
            ]
        },
    }
    assert expected_reply == reply


def test_get_available_queries(zmq_host, zmq_port):
    """
    Action 'get_available_queries' returns list of available queries.
    """
    msg = {"action": "get_available_queries", "request_id": "DUMMY_ID"}
    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)
    expected_reply = {
        "status": "success",
        "msg": "",
        "payload": {
            "available_queries": [
                "dummy_query",
                "flows",
                "meaningful_locations_aggregate",
                "meaningful_locations_between_label_od_matrix",
                "meaningful_locations_between_dates_od_matrix",
                "geography",
                "location_event_counts",
                "unique_subscriber_counts",
                "location_introversion",
                "total_network_objects",
                "aggregate_network_objects",
                "dfs_metric_total_amount",
                "spatial_aggregate",
                "joined_spatial_aggregate",
                "histogram_aggregate",
                "active_at_reference_location_counts",
                "unique_visitor_counts",
                "consecutive_trips_od_matrix",
                "unmoving_counts",
                "unmoving_at_reference_location_counts",
                "trips_od_matrix",
                "labelled_query",
            ]
        },
    }
    assert expected_reply == reply


def test_api_spec_of_flowmachine_query_schemas(zmq_host, zmq_port, diff_reporter):
    """
    Verify the API spec for flowmachine queries.
    """
    msg = {"action": "get_query_schemas", "request_id": "DUMMY_ID"}
    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)
    print(reply)
    assert "success" == reply["status"]
    spec_as_json_string = json.dumps(
        sort_recursively(reply["payload"]["query_schemas"]), indent=2
    )
    diff_reporter(spec_as_json_string)


def test_run_daily_location_query(zmq_host, zmq_port):
    """
    Can run daily location query and receive successful response including the query_id.
    """
    msg = {
        "action": "run_query",
        "params": {
            "query_kind": "spatial_aggregate",
            "locations": {
                "query_kind": "daily_location",
                "date": "2016-01-01",
                "method": "most-common",
                "aggregation_unit": "admin3",
                "subscriber_subset": None,
            },
        },
        "request_id": "DUMMY_ID",
    }
    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)

    q = RedactedSpatialAggregate(
        spatial_aggregate=SpatialAggregate(
            locations=daily_location(
                date="2016-01-01",
                method="most-common",
                spatial_unit=make_spatial_unit("admin", level=3),
                table=None,
                subscriber_subset=None,
                hours=None,
            )
        )
    )
    expected_query_id = q.query_id

    assert "success" == reply["status"]
    assert expected_query_id == reply["payload"]["query_id"]
    assert ["query_id", "progress"] == list(reply["payload"].keys())

    # FIXME: At the moment we have to explicitly wait for all running queries
    # to finish before finishing the test, otherwise unexpected behaviour may
    # occur when we reset the cache before the next test
    # (see https://github.com/Flowminder/FlowKit/issues/1245).
    poll_until_done(zmq_port, expected_query_id)


def test_run_modal_location_query(zmq_host, zmq_port):
    """
    Can run modal location query and receive successful response including the query_id.
    """
    msg = {
        "action": "run_query",
        "params": {
            "query_kind": "spatial_aggregate",
            "locations": {
                "query_kind": "modal_location",
                "locations": [
                    {
                        "query_kind": "daily_location",
                        "date": "2016-01-01",
                        "method": "most-common",
                        "aggregation_unit": "admin3",
                        "subscriber_subset": None,
                    },
                    {
                        "query_kind": "daily_location",
                        "date": "2016-01-02",
                        "method": "most-common",
                        "aggregation_unit": "admin3",
                        "subscriber_subset": None,
                    },
                ],
            },
        },
        "request_id": "DUMMY_ID",
    }
    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)

    q = RedactedSpatialAggregate(
        spatial_aggregate=SpatialAggregate(
            locations=ModalLocation(
                daily_location(
                    date="2016-01-01",
                    method="most-common",
                    spatial_unit=make_spatial_unit("admin", level=3),
                    table=None,
                    subscriber_subset=None,
                    hours=None,
                ),
                daily_location(
                    date="2016-01-02",
                    method="most-common",
                    spatial_unit=make_spatial_unit("admin", level=3),
                    table=None,
                    subscriber_subset=None,
                    hours=None,
                ),
            )
        )
    )
    expected_query_id = q.query_id

    assert "success" == reply["status"]
    assert expected_query_id == reply["payload"]["query_id"]
    assert ["query_id", "progress"] == list(reply["payload"].keys())

    # FIXME: At the moment we have to explicitly wait for all running queries
    # to finish before finishing the test, otherwise unexpected behaviour may
    # occur when we reset the cache before the next test
    # (see https://github.com/Flowminder/FlowKit/issues/1245).
    poll_until_done(zmq_port, expected_query_id)


def test_run_dfs_metric_total_amount_query(zmq_host, zmq_port):
    """
    Can run dfs metric query and receive successful response including the query_id.
    """
    msg = {
        "action": "run_query",
        "params": {
            "query_kind": "dfs_metric_total_amount",
            "metric": "commission",
            "start_date": "2016-01-02",
            "end_date": "2016-01-05",
            "aggregation_unit": "admin2",
        },
        "request_id": "DUMMY_ID",
    }
    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)

    q = DFSTotalMetricAmount(
        metric="commission",
        start_date="2016-01-02",
        end_date="2016-01-05",
        aggregation_unit="admin2",
    )
    expected_query_id = q.query_id

    assert "success" == reply["status"]
    assert expected_query_id == reply["payload"]["query_id"]
    assert ["query_id", "progress"] == list(reply["payload"].keys())

    # FIXME: At the moment we have to explicitly wait for all running queries
    # to finish before finishing the test, otherwise unexpected behaviour may
    # occur when we reset the cache before the next test
    # (see https://github.com/Flowminder/FlowKit/issues/1245).
    poll_until_done(zmq_port, expected_query_id)
