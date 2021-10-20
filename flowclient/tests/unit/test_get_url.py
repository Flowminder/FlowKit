# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import pytest
import respx
from httpx import RequestError

import flowclient
from flowclient.errors import FlowclientConnectionError


@pytest.mark.parametrize("status_code", [200, 202, 303])
def test_get_url_good_statuses(
    session_mock, dummy_route, flowclient_connection, status_code
):
    """response object should be returned for OK status codes.."""
    request = session_mock.get(dummy_route).respond(
        status_code=status_code,
        content="TEST",
    )
    assert b"TEST" == flowclient_connection.get_url(route="DUMMY_ROUTE").content


def test_get_url_reraises(session_mock, dummy_route, flowclient_connection):
    """get_url should reraise anything raises by requests."""
    request = session_mock.get(dummy_route).mock(
        side_effect=RequestError("DUMMY_MESSAGE"),
    )
    with pytest.raises(FlowclientConnectionError, match="DUMMY_MESSAGE"):
        flowclient_connection.get_url(route="DUMMY_ROUTE")


def test_404_raises_error(session_mock, dummy_route, flowclient_connection):
    """Exception should be raised for a 404 response."""
    request = session_mock.get(dummy_route).respond(status_code=404, content=[])
    with pytest.raises(
        FileNotFoundError, match="https://DUMMY_API/api/0/DUMMY_ROUTE not found."
    ):
        flowclient_connection.get_url(route="DUMMY_ROUTE")


@pytest.mark.parametrize("denial_status_code", [401, 403])
def test_access_denied_error(
    denial_status_code, session_mock, dummy_route, flowclient_connection
):
    """If a msg field is available for an access denied it should be used as the error message."""
    session_mock.get(dummy_route).respond(
        status_code=denial_status_code,
        json={"msg": "ERROR_MESSAGE"},
    )
    with pytest.raises(FlowclientConnectionError, match="ERROR_MESSAGE"):
        flowclient_connection.get_url(route="DUMMY_ROUTE")


@pytest.mark.parametrize("denial_status_code", [401, 403])
def test_access_denied_error(
    denial_status_code, session_mock, dummy_route, flowclient_connection
):
    """If a msg field is not available for an access denied a generic error should be used."""
    session_mock.get(dummy_route).respond(
        status_code=denial_status_code,
        json={},
    )
    with pytest.raises(FlowclientConnectionError, match="Unknown access denied error"):
        flowclient_connection.get_url(route="DUMMY_ROUTE")


def test_generic_status_code_error(session_mock, dummy_route, flowclient_connection):
    """An error should be raised for status codes that aren't expected."""
    session_mock.get(dummy_route).respond(
        status_code=418,
        json={"msg": "I AM A TEAPOT"},
    )
    with pytest.raises(
        FlowclientConnectionError,
        match="Something went wrong: I AM A TEAPOT. API returned with status code: 418",
    ):
        flowclient_connection.get_url(route="DUMMY_ROUTE")


@pytest.mark.parametrize("json_failure", ["FOO", {"TEST": "TEST"}])
def test_generic_status_code_unknown_error(
    json_failure, session_mock, dummy_route, flowclient_connection
):
    """An error should be raised for status codes that aren't expected, with a default error message if not given."""
    err_arg = (
        dict(content=json_failure)
        if isinstance(json_failure, str)
        else dict(json=json_failure)
    )
    session_mock.get(dummy_route).respond(status_code=418, **err_arg)
    with pytest.raises(
        FlowclientConnectionError,
        match="Something went wrong: Unknown error. API returned with status code: 418 and status 'Unknown status'",
    ):
        flowclient_connection.get_url(route="DUMMY_ROUTE")
