# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core.server.utils import (
    send_message_and_receive_reply,
    FM_EXAMPLE_MESSAGE,
)


def test_send_message_and_receive_reply(zmq_host, zmq_port):
    """
    Reply from the flowmachine server to the example message stored in `FM_EXAMPLE_MESSAGE` is as expected.
    """

    # Check that FM_EXAMPLE_MESSAGE contains the expected message
    msg_expected = {
        "action": "run_query",
        "query_kind": "daily_location",
        "request_id": "DUMMY_ID",
        "params": {
            "date": "2016-01-01",
            "daily_location_method": "last",
            "aggregation_unit": "admin3",
            "subscriber_subset": "all",
        },
    }
    assert msg_expected == FM_EXAMPLE_MESSAGE

    # Check that the flowmachine server sends the expected reply
    reply = send_message_and_receive_reply(FM_EXAMPLE_MESSAGE, host=zmq_host, port=zmq_port)
    expected_reply = {"status": "accepted", "id": "ddc61a04f608dee16fff0655f91c2057"}
    assert expected_reply == reply
