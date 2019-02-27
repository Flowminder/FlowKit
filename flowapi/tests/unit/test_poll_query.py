# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from asynctest import return_once


@pytest.mark.parametrize(
    "status, http_code",
    [
        ("executed", 303),
        ("executing", 202),
        ("awol", 404),
        ("queued", 202),
        ("errored", 500),
        ("cancelled", 500),
    ],
)
@pytest.mark.asyncio
async def test_poll_query(
    status, http_code, app, dummy_zmq_server, access_token_builder
):
    """
    Test that correct status code and any redirect is returned when polling a running query
    """
    client, db, log_dir, app = app

    token = access_token_builder({"modal_location": {"permissions": {"poll": True}}})
    dummy_zmq_server.side_effect = return_once(
        {"id": 0, "query_kind": "modal_location"}, then={"status": status, "id": 0}
    )
    response = await client.get(
        f"/api/0/poll/0", headers={"Authorization": f"Bearer {token}"}
    )
    assert response.status_code == http_code
    if status == "done":
        assert "/api/0/get/0" == response.headers["Location"]
