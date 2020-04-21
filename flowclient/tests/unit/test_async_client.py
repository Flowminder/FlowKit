# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from unittest.mock import Mock

from asynctest import Mock as AMock, CoroutineMock

import pytest

from flowclient.async_client import (
    query_is_ready,
    get_geography,
    get_status,
    get_json_dataframe,
    get_geojson_result_by_query_id,
    run_query,
    get_available_dates,
    get_result_location_from_id_when_ready,
)
from flowclient.errors import FlowclientConnectionError


@pytest.mark.asyncio
async def test_query_ready_reports_false():
    """ Test that status code 202 is interpreted as query running. """
    con_mock = AMock()
    con_mock.get_url = CoroutineMock(
        return_value=AMock(
            status_code=202,
            json=Mock(
                return_value={
                    "status": "completed",
                    "progress": {"eligible": 0, "queued": 0, "running": 0},
                }
            ),
        )
    )
    is_ready, reply = await query_is_ready(connection=con_mock, query_id="foo")
    assert not is_ready


@pytest.mark.asyncio
async def test_query_ready_reports_true():
    """ Test that status code 303 is interpreted as query ready. """
    con_mock = AMock()
    con_mock.get_url = CoroutineMock(return_value=AMock(status_code=303))
    is_ready, reply = await query_is_ready(connection=con_mock, query_id="foo")
    assert is_ready


@pytest.mark.asyncio
async def test_query_ready_raises():
    """ Test that status codes other than 202, 303, 401, and 404 raise a generic error. """
    con_mock = AMock()
    con_mock.get_url = CoroutineMock(return_value=AMock(status_code=999))
    with pytest.raises(FlowclientConnectionError):
        await query_is_ready(connection=con_mock, query_id="foo")


@pytest.mark.asyncio
async def test_run_query_raises():
    con_mock = AMock()
    con_mock.post_json = CoroutineMock(
        return_value=Mock(
            status_code=500, json=Mock(return_value=dict(msg="DUMMY_ERROR"))
        )
    )
    with pytest.raises(
        FlowclientConnectionError,
        match="Error running the query: DUMMY_ERROR. Status code: 500.",
    ):
        await run_query(connection=con_mock, query_spec="foo")


@pytest.mark.asyncio
async def test_run_query_raises_with_default_error():
    con_mock = AMock()
    con_mock.post_json = CoroutineMock(
        return_value=Mock(status_code=500, json=Mock(return_value=dict()))
    )
    with pytest.raises(
        FlowclientConnectionError,
        match="Error running the query: Unknown error. Status code: 500.",
    ):
        await run_query(connection=con_mock, query_spec="foo")


@pytest.mark.parametrize("http_code", [401, 500])
@pytest.mark.asyncio
async def test_available_dates_error(http_code):
    """
    Any unexpected http code should raise an exception.
    """
    connection_mock = AMock()
    connection_mock.get_url = CoroutineMock(
        return_value=Mock(
            status_code=http_code, json=Mock(return_value=dict(msg="MESSAGE"))
        )
    )
    with pytest.raises(
        FlowclientConnectionError,
        match=f"Could not get available dates. API returned with status code: {http_code}. Reason: MESSAGE",
    ):
        await get_available_dates(connection=connection_mock, event_types=["FOOBAR"])


@pytest.mark.asyncio
async def test_get_result_location_from_id_when_ready():
    """
    Any unexpected http code should raise an exception.
    """
    connection_mock = AMock()
    connection_mock.get_url = CoroutineMock(
        return_value=Mock(
            status_code=303, headers=dict(Location="/api/0/DUMMY_LOCATION")
        )
    )
    assert (
        await get_result_location_from_id_when_ready(
            connection=connection_mock, query_id="DUMMY_ID"
        )
        == "DUMMY_LOCATION"
    )


@pytest.mark.asyncio
async def test_available_dates_error_with_no_info():
    """
    Any unexpected http code should raise an exception.
    """
    connection_mock = AMock()
    connection_mock.get_url = CoroutineMock(
        return_value=Mock(status_code=401, json=Mock(return_value=dict(msg="MESSAGE")))
    )
    with pytest.raises(
        FlowclientConnectionError,
        match=f"Could not get available dates. API returned with status code: 401.",
    ):
        await get_available_dates(connection=connection_mock, event_types=["FOOBAR"])


@pytest.mark.asyncio
@pytest.mark.parametrize("running_status", ["queued", "executing"])
async def test_get_status_reports_running(running_status):
    """ Test that status code 202 is interpreted as query running or queued. """
    con_mock = AMock()
    con_mock.get_url = CoroutineMock(
        return_value=Mock(
            status_code=202,
            json=Mock(
                return_value={
                    "status": running_status,
                    "progress": {"eligible": 0, "queued": 0, "running": 0},
                }
            ),
        )
    )
    status = await get_status(connection=con_mock, query_id="foo")
    assert status == running_status


@pytest.mark.asyncio
async def test_get_status_reports_finished():
    """ Test that status code 303 is interpreted as query finished. """
    con_mock = AMock()
    con_mock.get_url = CoroutineMock(return_value=Mock(status_code=303))
    status = await get_status(connection=con_mock, query_id="foo")
    assert status == "completed"


@pytest.mark.asyncio
async def test_get_status_404():
    """ Test that get_status reports that a query is not running. """
    con_mock = AMock()
    con_mock.get_url = CoroutineMock(side_effect=FileNotFoundError("DUMMY_404"))
    status_returned = await get_status(connection=con_mock, query_id="foo")
    assert status_returned == "not_running"


@pytest.mark.asyncio
async def test_get_status_raises():
    """ Test that get_status raises an error for a status code other than 202, 303 or 404. """
    con_mock = AMock()
    con_mock.get_url = CoroutineMock(return_value=Mock(status_code=500))
    with pytest.raises(FlowclientConnectionError):
        await get_status(connection=con_mock, query_id="foo")


@pytest.mark.asyncio
async def test_get_json_dataframe_raises():
    """ Test that get_json_dataframe raises an error. """
    con_mock = AMock()
    con_mock.get_url = CoroutineMock(
        return_value=Mock(
            status_code=500, json=Mock(return_value=dict(msg="DUMMY_ERROR"))
        )
    )
    with pytest.raises(FlowclientConnectionError, match=r".*Reason: DUMMY_ERROR"):
        await get_json_dataframe(connection=con_mock, location="foo")


@pytest.mark.asyncio
async def test_get_geojson_result_by_query_id_raises(monkeypatch):
    """ Test that get_geojson_result_by_query_id raises an error. """
    con_mock = AMock()
    con_mock.get_url = CoroutineMock(
        return_value=Mock(
            status_code=500, json=Mock(return_value=dict(msg="DUMMY_ERROR"))
        )
    )
    monkeypatch.setattr(
        "flowclient.async_client.get_result_location_from_id_when_ready",
        CoroutineMock(return_value="DUMMY"),
    )
    with pytest.raises(FlowclientConnectionError, match=r".*Reason: DUMMY_ERROR"):
        await get_geojson_result_by_query_id(connection=con_mock, query_id="foo")


@pytest.mark.asyncio
async def test_get_status_raises_without_status():
    """ Test that get_status raises an error if the status field is absent. """
    con_mock = AMock()
    con_mock.get_url = CoroutineMock(
        return_value=Mock(
            status_code=202,
            json=Mock(
                return_value=dict(progress=dict(queued=0, running=0, eligible=0))
            ),
        )
    )
    with pytest.raises(FlowclientConnectionError, match="No status reported"):
        await get_status(connection=con_mock, query_id="foo")


@pytest.mark.asyncio
async def test_get_geography(token):
    """
    Test that getting geography returns the returned dict
    """
    connection_mock = AMock()
    connection_mock.get_url = CoroutineMock(
        return_value=Mock(status_code=200, json=Mock(return_value={"some": "json"}))
    )
    gj = await get_geography(
        connection=connection_mock, aggregation_unit="DUMMY_AGGREGATION"
    )
    assert {"some": "json"} == gj


@pytest.mark.parametrize("http_code", [401, 404, 418, 400])
@pytest.mark.asyncio
async def test_get_geography_error(http_code, token):
    """
    Any unexpected http code should raise an exception.
    """
    connection_mock = AMock()
    connection_mock.get_url = CoroutineMock(
        return_value=Mock(
            status_code=http_code, json=Mock(return_value={"msg": "MESSAGE"})
        )
    )
    with pytest.raises(
        FlowclientConnectionError,
        match=f"Could not get result. API returned with status code: {http_code}. Reason: MESSAGE",
    ):
        await get_geography(
            connection=connection_mock, aggregation_unit="DUMMY_AGGREGATION"
        )


@pytest.mark.asyncio
async def test_get_geography_no_msg_error(token):
    """
    A response with an unexpected http code and no "msg" should raise a FlowclientConnectionError.
    """
    connection_mock = AMock()
    connection_mock.get_url = CoroutineMock(
        return_value=Mock(status_code=404, json=Mock(return_value={}))
    )
    with pytest.raises(
        FlowclientConnectionError,
        match=f"Could not get result. API returned with status code: 404.",
    ):
        await get_geography(
            connection=connection_mock, aggregation_unit="DUMMY_AGGREGATION"
        )
