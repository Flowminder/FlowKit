# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from tests.unit.zmq_helpers import ZMQReply

import pytest


@pytest.mark.asyncio
async def test_post_query(app, dummy_zmq_server, access_token_builder):
    """
    Test that correct status of 202 & redirect is returned when sending a query.
    """

    token = access_token_builder(
        [
            "run&daily_location.aggregation_unit.admin2",
            "run&daily_location.aggregation_unit.admin3",
        ]
    )
    dummy_zmq_server.return_value = ZMQReply(
        status="success",
        payload={
            "query_id": "DUMMY_QUERY_ID",
            "progress": {"eligible": 0, "queued": 0, "executing": 0},
        },
    )
    response = await app.client.post(
        f"/api/0/run",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "query_kind": "daily_location",
            "date": "2016-01-01",
            "aggregation_unit": "admin3",
        },
    )
    assert response.status_code == 202
    assert "/api/0/poll/DUMMY_QUERY_ID" == response.headers["Location"]


@pytest.mark.parametrize(
    "query, expected_msg",
    [
        ({"date": "2016-01-01"}, "Query kind must be specified when running a query."),
    ],
)
@pytest.mark.asyncio
async def test_post_query_error(
    query, expected_msg, app, dummy_zmq_server, access_token_builder
):
    """
    Test that correct status of 400 is returned for a broken query.
    """

    token = access_token_builder([f"run&spatial_aggregate"])
    dummy_zmq_server.return_value = ZMQReply(status="error", msg="Broken query")
    response = await app.client.post(
        f"/api/0/run", headers={"Authorization": f"Bearer {token}"}, json=query
    )
    json = await response.get_json()
    assert response.status_code == 400
    assert expected_msg == json["msg"]
