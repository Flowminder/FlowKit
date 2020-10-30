# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import pytest
from httpx import RequestError

import flowclient
from flowclient.errors import FlowclientConnectionError


@pytest.mark.parametrize("status_code", [200, 202, 303])
def test_get_url_good_statuses(status_code, session_mock, token):
    """response object should be returned for OK status codes.."""
    session_mock.request.return_value.status_code = status_code
    connection = flowclient.connect(url="DUMMY_API", token=token)
    assert session_mock.request.return_value == connection.get_url(route="DUMMY_ROUTE")


def test_get_url_reraises(session_mock, token):
    """get_url should reraise anything raises by requests."""
    session_mock.request.side_effect = RequestError("DUMMY_MESSAGE", request=None)
    connection = flowclient.connect(url="DUMMY_API", token=token)
    with pytest.raises(FlowclientConnectionError, match="DUMMY_MESSAGE"):
        connection.get_url(route="DUMMY_ROUTE")


def test_404_raises_error(session_mock, token):
    """Exception should be raised for a 404 response."""
    session_mock.request.return_value.status_code = 404
    connection = flowclient.connect(url="DUMMY_API", token=token)
    with pytest.raises(
        FileNotFoundError, match="DUMMY_API/api/0/DUMMY_ROUTE not found."
    ):
        connection.get_url(route="DUMMY_ROUTE")


@pytest.mark.parametrize("denial_status_code", [401, 403])
def test_access_denied_error(denial_status_code, session_mock, token):
    """If a msg field is available for an access denied it should be used as the error message."""
    session_mock.request.return_value.status_code = denial_status_code
    session_mock.request.return_value.json.return_value = {"msg": "ERROR_MESSAGE"}
    connection = flowclient.connect(url="DUMMY_API", token=token)
    with pytest.raises(FlowclientConnectionError, match="ERROR_MESSAGE"):
        connection.get_url(route="DUMMY_ROUTE")


@pytest.mark.parametrize("denial_status_code", [401, 403])
def test_access_denied_error(denial_status_code, session_mock, token):
    """If a msg field is not available for an access denied a generic error should be used."""
    session_mock.request.return_value.status_code = denial_status_code
    session_mock.request.return_value.json.return_value = {}
    connection = flowclient.connect(url="DUMMY_API", token=token)
    with pytest.raises(FlowclientConnectionError, match="Unknown access denied error"):
        connection.get_url(route="DUMMY_ROUTE")


def test_generic_status_code_error(session_mock, token):
    """An error should be raised for status codes that aren't expected."""
    session_mock.request.return_value.status_code = 418
    session_mock.request.return_value.json.return_value = {"msg": "I AM A TEAPOT"}
    connection = flowclient.connect(url="DUMMY_API", token=token)
    with pytest.raises(
        FlowclientConnectionError,
        match="Something went wrong: I AM A TEAPOT. API returned with status code: 418",
    ):
        connection.get_url(route="DUMMY_ROUTE")


@pytest.mark.parametrize("json_failure", [ValueError, KeyError])
def test_generic_status_code_unknown_error(json_failure, session_mock, token):
    """An error should be raised for status codes that aren't expected, with a default error message if not given."""
    session_mock.request.return_value.status_code = 418
    session_mock.request.return_value.json.side_effect = json_failure
    connection = flowclient.connect(url="DUMMY_API", token=token)
    with pytest.raises(
        FlowclientConnectionError,
        match="Something went wrong: Unknown error. API returned with status code: 418 and status 'Unknown status'",
    ):
        connection.get_url(route="DUMMY_ROUTE")
