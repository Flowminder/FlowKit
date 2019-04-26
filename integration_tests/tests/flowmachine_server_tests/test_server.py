# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import json

from flowmachine.utils import sort_recursively
from approvaltests.approvals import verify


def test_ping_flowmachine_server(send_zmq_message_and_receive_reply):
    """
    Sending the 'ping' action to the flowmachine server evokes a successful 'pong' response.
    """
    msg = {"action": "ping", "request_id": "DUMMY_ID"}
    reply = send_zmq_message_and_receive_reply(msg)
    expected_reply = {"status": "success", "msg": "pong", "payload": {}}
    assert expected_reply == reply


def test_unknown_action_returns_error(send_zmq_message_and_receive_reply):
    """
    Unknown action returns an error response.
    """
    msg = {"action": "foobar", "request_id": "DUMMY_ID"}
    reply = send_zmq_message_and_receive_reply(msg)
    expected_reply = {
        "status": "error",
        "msg": "Invalid action request.",
        "payload": {
            "action": [
                "Must be one of: ping, get_available_queries, get_query_schemas, run_query, poll_query, get_query_kind, get_query_params, get_sql_for_query_result, get_geography, get_available_dates."
            ]
        },
    }
    assert expected_reply == reply


def test_get_available_queries(send_zmq_message_and_receive_reply):
    """
    Action 'get_available_queries' returns list of available queries.
    """
    msg = {"action": "get_available_queries", "request_id": "DUMMY_ID"}
    reply = send_zmq_message_and_receive_reply(msg)
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
            ]
        },
    }
    assert expected_reply == reply


def test_api_spec_of_flowmachine_query_schemas(
    send_zmq_message_and_receive_reply, diff_reporter
):
    """
    Verify the API spec for flowmachine queries.
    """
    msg = {"action": "get_query_schemas", "request_id": "DUMMY_ID"}
    reply = send_zmq_message_and_receive_reply(msg)
    print(reply)
    assert "success" == reply["status"]
    spec_as_json_string = json.dumps(
        sort_recursively(reply["payload"]["query_schemas"]), indent=2
    )
    verify(spec_as_json_string, diff_reporter)


def test_run_daily_location_query(send_zmq_message_and_receive_reply):
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
    reply = send_zmq_message_and_receive_reply(msg)

    assert "success" == reply["status"]
    assert "d426cec2c2a0881cbb25d6620b45db94" == reply["payload"]["query_id"]
    assert ["query_id"] == list(reply["payload"].keys())


def test_run_modal_location_query(send_zmq_message_and_receive_reply):
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
                "aggregation_unit": "admin3",
                "subscriber_subset": None,
            },
        },
        "request_id": "DUMMY_ID",
    }
    reply = send_zmq_message_and_receive_reply(msg)

    assert "success" == reply["status"]
    assert "6fb04af28b1475be4fd564605acbad5b" == reply["payload"]["query_id"]
    assert ["query_id"] == list(reply["payload"].keys())


def test_run_dfs_metric_total_amount_query(send_zmq_message_and_receive_reply):
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
    reply = send_zmq_message_and_receive_reply(msg)

    assert "success" == reply["status"]
    assert "7070dcedf6633d2b6f263b83ea27b9e4" == reply["payload"]["query_id"]
    assert ["query_id"] == list(reply["payload"].keys())
