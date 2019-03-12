# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest


@pytest.mark.asyncio
async def test_app(app):
    """
    Test that the flowapi start, and responds on the root route.

    Parameters
    ----------
    app: tuple
        Pytest fixture providing the flowapi, with a mock for the db
    """
    client, db, log_dir, app = app

    response = await client.get("/")
    assert response.status_code == 200
