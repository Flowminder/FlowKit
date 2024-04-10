# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core.server.utils import send_zmq_message_and_receive_reply

from .helpers import poll_until_done

# TODO: add test for code path that raises QueryProxyError with the 'get_params' action


@pytest.mark.parametrize(
    "params",
    [
        {
            "query_kind": "spatial_aggregate",
            "locations": {
                "query_kind": "daily_location",
                "date": "2016-01-01",
                "method": "last",
                "aggregation_unit": "admin3",
                "subscriber_subset": None,
            },
        },
        {
            "query_kind": "spatial_aggregate",
            "locations": {
                "query_kind": "daily_location",
                "date": "2016-01-04",
                "method": "most-common",
                "aggregation_unit": "admin1",
                "subscriber_subset": None,
            },
        },
    ],
)
def test_get_query_params(params, zmq_port, zmq_host):
    """
    Running 'get_query_params' against an existing query_id returns the expected parameters with which the query was run.
    """
    #
    # Run query.
    #
    msg = {"action": "run_query", "params": params, "request_id": "DUMMY_ID"}

    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)
    query_id = reply["payload"]["query_id"]
    # assert reply["status"] in ("executing", "queued", "completed")
    assert reply["status"] == "success"

    #
    # Wait until the query has finished.
    #
    poll_until_done(zmq_port, query_id)

    #
    # Get query result.
    #
    msg = {
        "action": "get_query_params",
        "params": {"query_id": query_id},
        "request_id": "DUMMY_ID",
    }

    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)
    expected_reply = {
        "status": "success",
        "msg": "",
        "payload": {"query_id": query_id, "query_params": params},
    }
    assert expected_reply == reply


@pytest.mark.skip(reason="The 'get_query_params' action will likely be removed soon.")
def test_get_query_params_for_nonexistent_query_id(zmq_port, zmq_host):
    """
    Running 'get_query_params' on a non-existent query id returns an error.
    """
    #
    # Try getting query result for nonexistent ID.
    #
    msg = {
        "action": "get_query_params",
        "params": {"query_id": "FOOBAR"},
        "request_id": "DUMMY_ID",
    }

    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)
    assert {
        "status": "awol",
        "id": "FOOBAR",
        "error": "Unknown query id: FOOBAR",
    } == reply
