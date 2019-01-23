# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from json import loads

from asynctest import return_once


@pytest.mark.asyncio
async def test_get_geography(app, dummy_zmq_server, access_token_builder):
    """
    Test that JSON is returned when getting a query.
    """
    client, db, log_dir, app = app
    aggregation_unit = "DUMMY_AGGREGATION"
    # Set the rows returned by iterating over the rows from the db
    # This is a long chain of mocks corresponding to getting a connection using
    # the pool's context manager, getting the cursor on that, and then looping
    # over the values in cursor
    db.acquire.return_value.__aenter__.return_value.cursor.return_value.__aiter__.return_value = [
        ['{"some": "valid"}'],
        ['{"json": "bits"}'],
    ]
    token = access_token_builder(
        {
            "geography": {
                "permissions": {"get_result": True},
                "spatial_aggregation": [aggregation_unit],
            }
        }
    )

    dummy_zmq_server.side_effect = (
        {"status": "done", "crs": "DUMMY_CRS", "sql": "SELECT 1;"},
    )
    response = await client.get(
        f"/api/0/geography/{aggregation_unit}",
        headers={"Authorization": f"Bearer {token}"},
    )
    gjs = loads(await response.get_data())
    assert 200 == response.status_code
    assert "DUMMY_CRS" == gjs["properties"]["crs"]
    assert [{"some": "valid"}, {"json": "bits"}] == gjs["features"]
    assert "application/geo+json" == response.headers["content-type"]
    assert (
        f"attachment;filename={aggregation_unit}.geojson"
        == response.headers["content-disposition"]
    )


@pytest.mark.parametrize("status, http_code", [("awol", 404), ("error", 403)])
@pytest.mark.asyncio
async def test_get_geography_status(
    status, http_code, app, dummy_zmq_server, access_token_builder
):
    """
    Test that correct status code is returned when server returns an error.
    """
    client, db, log_dir, app = app

    token = access_token_builder(
        {
            "geography": {
                "permissions": {"get_result": True},
                "spatial_aggregation": ["DUMMY_AGGREGATION"],
            }
        }
    )
    dummy_zmq_server.side_effect = ({"status": status, "error": "Some error., "},)
    response = await client.get(
        f"/api/0/geography/DUMMY_AGGREGATION",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert http_code == response.status_code
