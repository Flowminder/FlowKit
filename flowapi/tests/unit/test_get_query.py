# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from json import loads

from asynctest import return_once


@pytest.mark.asyncio
async def test_get_query(app, access_token_builder, dummy_zmq_server):
    """
    Test that JSON is returned when getting a query.
    """
    client, db, log_dir, app = app
    # Set the rows returned by iterating over the rows from the db
    # This is a long chain of mocks corresponding to getting a connection using
    # the pool's context manager, getting the cursor on that, and then looping
    # over the values in cursor
    db.acquire.return_value.__aenter__.return_value.cursor.return_value.__aiter__.return_value = [
        {"some": "valid"},
        {"json": "bits"},
    ]
    token = access_token_builder(
        {
            "modal_location": {
                "permissions": {"get_result": True},
                "spatial_aggregation": ["DUMMY_AGGREGATION"],
            }
        }
    )

    reply_1 = {
        "status": "done",
        "payload": {
            "query_id": "5ffe4a96dbe33a117ae9550178b81836",
            "query_kind": "modal_location",
            "query_state": "completed",
        },
    }
    reply_2 = {
        "status": "done",
        "payload": {"query_params": {"aggregation_unit": "DUMMY_AGGREGATION"}},
    }
    reply_3 = {
        "status": "done",
        "payload": {"query_state": "completed", "sql": "SELECT 1;"},
    }
    dummy_zmq_server.side_effect = (reply_1, reply_2, reply_3)
    response = await client.get(
        f"/api/0/get/DUMMY_QUERY_ID", headers={"Authorization": f"Bearer {token}"}
    )
    reply = await response.get_data()
    json_data = loads(reply)
    assert "DUMMY_QUERY_ID" == json_data["query_id"]
    assert [{"some": "valid"}, {"json": "bits"}] == json_data["query_result"]
    assert (
        "attachment;filename=DUMMY_QUERY_ID.json"
        == response.headers["content-disposition"]
    )


@pytest.mark.parametrize(
    "status, http_code",
    [
        ("completed", 200),
        ("executing", 202),
        ("queued", 202),
        ("awol", 404),
        ("errored", 403),
        ("known", 404),
        ("NOT_A_STATUS", 500),
    ],
)
@pytest.mark.asyncio
async def test_get_json_status(
    status, http_code, app, dummy_zmq_server, access_token_builder
):
    """
    Test that correct status code and any redirect is returned when getting json.
    """
    client, db, log_dir, app = app

    token = access_token_builder(
        {
            "modal_location": {
                "permissions": {"get_result": True},
                "spatial_aggregation": ["DUMMY_AGGREGATION"],
            }
        }
    )
    dummy_zmq_server.side_effect = (
        {"id": 0, "query_kind": "modal_location"},
        {"id": 0, "params": {"aggregation_unit": "DUMMY_AGGREGATION"}},
        {"status": status, "id": 0, "error": "Some error", "sql": "SELECT 1;"},
    )
    response = await client.get(
        f"/api/0/get/0", headers={"Authorization": f"Bearer {token}"}
    )
    assert http_code == response.status_code


@pytest.mark.asyncio
async def test_get_error_for_missing_status(
    app, dummy_zmq_server, access_token_builder
):
    """
    Test that status code 500 is returned if a message without a status is
    received in get_query.
    """
    client, db, log_dir, app = app

    token = access_token_builder(
        {
            "modal_location": {
                "permissions": {"get_result": True},
                "spatial_aggregation": ["DUMMY_AGGREGATION"],
            }
        }
    )
    dummy_zmq_server.side_effect = (
        {"id": 0, "query_kind": "modal_location"},
        {"id": 0, "params": {"aggregation_unit": "DUMMY_AGGREGATION"}},
        {"id": 0},
    )
    response = await client.get(
        f"/api/0/get/0", headers={"Authorization": f"Bearer {token}"}
    )
    assert 500 == response.status_code
