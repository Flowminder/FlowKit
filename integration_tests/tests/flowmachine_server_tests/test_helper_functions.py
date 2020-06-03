# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core.server.utils import (
    send_zmq_message_and_receive_reply,
    FM_EXAMPLE_MESSAGE,
)
from flowmachine.core import make_spatial_unit
from flowmachine.features.location.spatial_aggregate import SpatialAggregate
from flowmachine.features.location.redacted_spatial_aggregate import (
    RedactedSpatialAggregate,
)
from flowmachine.features import daily_location
from .helpers import poll_until_done


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

    # Check that the flowmachine server sends the expected reply
    reply = send_zmq_message_and_receive_reply(
        FM_EXAMPLE_MESSAGE, host=zmq_host, port=zmq_port
    )
    assert expected_query_id == reply["payload"]["query_id"]
    # assert reply["status"] in ("executing", "queued", "completed")
    assert reply["status"] in ("success")

    # FIXME: At the moment we have to explicitly wait for all running queries
    # to finish before finishing the test, otherwise unexpected behaviour may
    # occur when we reset the cache before the next test
    # (see https://github.com/Flowminder/FlowKit/issues/1245).
    poll_until_done(zmq_port, expected_query_id)
