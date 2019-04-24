# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core.server.utils import (
    send_zmq_message_and_receive_reply,
    FM_EXAMPLE_MESSAGE,
)


def test_send_zmq_message_and_receive_reply(zmq_host, zmq_port):
    """
    Reply from the flowmachine server to the example message stored in `FM_EXAMPLE_MESSAGE` is as expected.
    """

    # Check that FM_EXAMPLE_MESSAGE contains the expected message
    msg_expected = {
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
    assert msg_expected == FM_EXAMPLE_MESSAGE

    # Check that the flowmachine server sends the expected reply
    reply = send_zmq_message_and_receive_reply(
        FM_EXAMPLE_MESSAGE, host=zmq_host, port=zmq_port
    )
    assert "a2fb1efca05a42d558e8c613970262de" == reply["payload"]["query_id"]
    # assert reply["status"] in ("executing", "queued", "completed")
    assert reply["status"] in ("success")
