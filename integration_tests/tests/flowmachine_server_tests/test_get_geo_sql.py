# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core.server.utils import send_zmq_message_and_receive_reply
from flowmachine.core import make_spatial_unit
from flowmachine.features.location.spatial_aggregate import SpatialAggregate
from flowmachine.features.location.redacted_spatial_aggregate import (
    RedactedSpatialAggregate,
)
from flowmachine.features import daily_location
from .helpers import poll_until_done


def test_get_geo_sql(zmq_port, zmq_host):
    """
    Running 'get_geo_sql' on finished query returns the expected result.
    """
    #
    # Run daily_location query.
    #
    msg = {
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

    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)
    # assert reply["status"] in ("executing", "queued", "completed")
    assert "success" == reply["status"]
    assert expected_query_id == reply["payload"]["query_id"]

    #
    # Wait until the query has finished.
    #
    poll_until_done(zmq_port, expected_query_id)

    #
    # Get query result.
    #
    msg = {
        "action": "get_geo_sql_for_query_result",
        "params": {"query_id": expected_query_id},
        "request_id": "DUMMY_ID",
    }
    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)
    assert "success" == reply["status"]
    assert q.geojson_query() == reply["payload"]["sql"]
