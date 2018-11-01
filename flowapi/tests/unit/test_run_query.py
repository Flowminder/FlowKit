# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest


@pytest.mark.asyncio
async def test_post_query(app, dummy_zmq_server, access_token_builder):
    """
    Test that correct status of 202 & redirect is returned when sending a query.
    """
    client, db, log_dir = app

    token = access_token_builder({"daily_location": {"permissions": {"run": True}}})
    dummy_zmq_server.return_value = {"id": 0}
    response = await client.post(
        f"/api/0/run",
        headers={"Authorization": f"Bearer {token}"},
        json={"query_kind": "daily_location", "params": {"date": "2016-01-01"}},
    )
    assert response.status_code == 202
    assert "/api/0/poll/0" == response.headers["Location"]


@pytest.mark.asyncio
async def test_post_query_error(app, dummy_zmq_server, access_token_builder):
    """
    Test that correct status of 403 is returned for a broken query.
    """
    client, db, log_dir = app

    token = access_token_builder({"daily_location": {"permissions": {"run": True}}})
    dummy_zmq_server.return_value = {"error": "Broken"}
    response = await client.post(
        f"/api/0/run",
        headers={"Authorization": f"Bearer {token}"},
        json={"query_kind": "daily_location", "params": {"date": "2016-01-01"}},
    )
    json = await response.get_json()
    assert response.status_code == 403
    assert "Broken" == json["reason"]
