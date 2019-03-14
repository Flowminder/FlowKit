# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import json

from approvaltests.approvals import verify


def test_ping_flowmachine_server(send_zmq_message_and_receive_reply):
    """
    Sending the 'ping' action to the flowmachine server evokes a successful 'pong' response.
    """
    msg = {"action": "ping", "request_id": "DUMMY_ID"}
    reply = send_zmq_message_and_receive_reply(msg)
    expected_reply = {"status": "done", "msg": "pong", "data": {}}
    assert expected_reply == reply


def test_unknown_action_returns_error(send_zmq_message_and_receive_reply):
    """
    Unknown action returns an error response.
    """
    msg = {"action": "foobar", "request_id": "DUMMY_ID"}
    reply = send_zmq_message_and_receive_reply(msg)
    expected_reply = {"status": "error", "msg": "Unknown action: 'foobar'", "data": {}}
    assert expected_reply == reply


def test_get_available_queries(send_zmq_message_and_receive_reply):
    """
    Action 'get_available_queries' returns list of available queries.
    """
    msg = {"action": "get_available_queries", "request_id": "DUMMY_ID"}
    reply = send_zmq_message_and_receive_reply(msg)
    expected_reply = {
        "status": "done",
        "msg": "",
        "data": {"available_queries": ["daily_location", "modal_location"]},
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
    assert "done" == reply["status"]
    spec_as_json_string = json.dumps(
        reply["data"]["query_schemas"], indent=2, sort_keys=True
    )
    verify(spec_as_json_string, diff_reporter)


def test_run_daily_location_query(send_zmq_message_and_receive_reply):
    """
    Can run daily location query and receive successful response including the query_id.
    """
    msg = {
        "action": "run_query",
        "params": {
            "query_kind": "daily_location",
            "date": "2016-01-01",
            "method": "most-common",
            "aggregation_unit": "admin3",
            "subscriber_subset": None,
        },
        "request_id": "DUMMY_ID",
    }
    reply = send_zmq_message_and_receive_reply(msg)

    assert "accepted" == reply["status"]
    assert "4503884d13687efd7ff25163b462596a" == reply["data"]["query_id"]
    assert ["query_id"] == list(reply["data"].keys())


def test_run_modal_location_query(send_zmq_message_and_receive_reply):
    """
    Can run modal location query and receive successful response including the query_id.
    """
    msg = {
        "action": "run_query",
        "params": {
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
        "request_id": "DUMMY_ID",
    }
    reply = send_zmq_message_and_receive_reply(msg)

    assert "accepted" == reply["status"]
    assert "2fd6df01e9bb630117e7c87b5eed7fd0" == reply["data"]["query_id"]
    assert ["query_id"] == list(reply["data"].keys())
