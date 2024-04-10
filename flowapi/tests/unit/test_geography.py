# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from json import loads

import pytest
from tests.unit.zmq_helpers import ZMQReply


@pytest.mark.asyncio
async def test_get_geography(app, access_token_builder, dummy_zmq_server):
    """
    Test that JSON is returned when getting a query.
    """

    aggregation_unit = "DUMMY_AGGREGATION"
    # Set the rows returned by iterating over the rows from the db
    # This is a long chain of mocks corresponding to getting a connection using
    # the pool's context manager, getting the cursor on that, and then looping
    # over the values in cursor
    app.db_pool.acquire.return_value.__aenter__.return_value.cursor.return_value.__aiter__.return_value = [
        {"some": "valid"},
        {"json": "bits"},
    ]
    token = access_token_builder(
        {"test_role": ["get_result", f"{aggregation_unit}:geography:geography"]}
    )

    zmq_reply = ZMQReply(
        status="success", payload={"query_state": "completed", "sql": "SELECT 1;"}
    )
    dummy_zmq_server.side_effect = (zmq_reply,)
    response = await app.client.get(
        f"/api/0/geography/{aggregation_unit}",
        headers={"Authorization": f"Bearer {token}"},
    )
    gjs = loads(await response.get_data())
    assert 200 == response.status_code
    assert "FeatureCollection" == gjs["type"]
    assert [{"some": "valid"}, {"json": "bits"}] == gjs["features"]
    assert "application/geo+json" == response.headers["content-type"]
    assert (
        f"attachment;filename={aggregation_unit}.geojson"
        == response.headers["content-disposition"]
    )


# TODO: Reinstate correct statuses for geographies
# @pytest.mark.parametrize(
#    "status, http_code", [("awol", 404), ("error", 403), ("NOT_A_STATUS", 500)]
# )
@pytest.mark.parametrize("status, http_code", [("NOT_A_STATUS", 500)])
@pytest.mark.asyncio
async def test_get_geography_status(
    status, http_code, app, dummy_zmq_server, access_token_builder
):
    """
    Test that correct status code is returned when server returns an error.
    """

    token = access_token_builder(
        ["get_result&geography.aggregation_unit.DUMMY_AGGREGATION"]
    )
    zmq_reply = ZMQReply(status="error", msg="Some error")
    dummy_zmq_server.side_effect = (zmq_reply,)
    response = await app.client.get(
        f"/api/0/geography/DUMMY_AGGREGATION",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert http_code == response.status_code


@pytest.mark.parametrize("response", [{"status": "NOT_AN_ERROR"}, {"status": "error"}])
@pytest.mark.asyncio
async def test_geography_errors(response, app, dummy_zmq_server, access_token_builder):
    """
    Test that status code 500 is returned for error and missing payload.
    """

    token = access_token_builder(
        ["get_result&geography.aggregation_unit.DUMMY_AGGREGATION"]
    )

    dummy_zmq_server.side_effect = (response,)
    response = await app.client.get(
        f"/api/0/geography/DUMMY_AGGREGATION",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert 500 == response.status_code
