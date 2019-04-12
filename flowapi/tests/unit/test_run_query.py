# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from tests.unit.zmq_helpers import ZMQReply


@pytest.mark.asyncio
async def test_post_query(app, dummy_zmq_server, access_token_builder):
    """
    Test that correct status of 202 & redirect is returned when sending a query.
    """
    client, db, log_dir, app = app

    token = access_token_builder(
        {
            "daily_location": {
                "permissions": {"run": True},
                "spatial_aggregation": ["admin3"],
            }
        }
    )
    dummy_zmq_server.return_value = ZMQReply(
        status="success", payload={"query_id": "DUMMY_QUERY_ID"}
    ).as_json()
    response = await client.post(
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
        (
            {
                "query_kind": "spatial_aggregate",
                "locations": {"query_kind": "daily_location", "date": "2016-01-01"},
            },
            "Aggregation unit must be specified when running a query.",
        ),
        ({"date": "2016-01-01"}, "Query kind must be specified when running a query."),
        (
            {"query_kind": "spatial_aggregate", "locations": {"date": "2016-01-01"}},
            "Query kind must be specified when running a query.",
        ),
        (
            {
                "query_kind": "joined_spatial_aggregate",
                "locations": {
                    "query_kind": "daily_location",
                    "date": "2016-01-01",
                    "aggregation_unit": "admin3",
                },
            },
            "Could not parse query spec.",
        ),
        (
            {
                "query_kind": "joined_spatial_aggregate",
                "locations": {"query_kind": "daily_location", "date": "2016-01-01"},
            },
            "Aggregation unit must be specified when running a query.",
        ),
        (
            {
                "query_kind": "joined_spatial_aggregate",
                "locations": {
                    "query_kind": "daily_location",
                    "date": "2016-01-01",
                    "aggregation_unit": "admin3",
                },
                "metric": {"DUMMY_PARAM": "DUMMY_VALUE"},
            },
            "Query kind must be specified when running a query.",
        ),
    ],
)
@pytest.mark.asyncio
async def test_post_query_error(
    query, expected_msg, app, dummy_zmq_server, access_token_builder
):
    """
    Test that correct status of 400 is returned for a broken query.
    """
    client, db, log_dir, app = app

    token = access_token_builder({"daily_location": {"permissions": {"run": True}}})
    dummy_zmq_server.return_value = ZMQReply(
        status="error", msg="Broken query"
    ).as_json()
    response = await client.post(
        f"/api/0/run", headers={"Authorization": f"Bearer {token}"}, json=query
    )
    json = await response.get_json()
    assert response.status_code == 400
    assert expected_msg == json["msg"]
