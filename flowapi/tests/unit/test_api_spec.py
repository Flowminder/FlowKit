# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import json
import pathlib
import pytest


@pytest.mark.asyncio
async def test_api_spec(app, dummy_zmq_server):
    """
    Test that flowapi successfully generates and returns the api spec.

    Parameters
    ----------
    app: tuple
        Pytest fixture providing the flowapi app
    """
    fm_spec_path = (
        pathlib.Path(__file__).parent.parent.parent.parent
        / "integration_tests"
        / "tests"
        / "flowmachine_server_tests"
        / "test_server.test_api_spec_of_flowmachine_query_schemas.approved.txt"
    )
    with open(fm_spec_path) as fin:
        spec = json.load(fin)
    dummy_zmq_server.return_value = {"payload": {"query_schemas": spec}}

    response = await app.client.get("/api/0/spec/openapi.json")
    assert response.status_code == 200
