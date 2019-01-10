# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest


@pytest.mark.asyncio
async def test_unknown_query_type(app, dummy_zmq_server, access_token_builder):
    """Test that a query type which doesn't exist returns a 404."""
    client, db, log_dir, app = app

    token = access_token_builder({"read_queries": ["nosuchquery"]})
    dummy_zmq_server.return_value = {"status": "awol", "query_kind": "nosuchquery"}
    response = await client.get(
        f"/api/0/nosuchquery", headers={"Authorization": f"Bearer {token}"}
    )
    assert 404 == response.status_code
