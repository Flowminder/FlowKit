# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from requests import ConnectionError

import flowclient
from flowclient.errors import FlowclientConnectionError

from .zmq_helpers import ZMQReply


@pytest.mark.parametrize("status_code", [202])
def test_post_json_good_statuses(status_code, session_mock, token):
    """response object should be returned for OK status codes.."""
    session_mock.post.return_value.status_code = status_code
    connection = flowclient.connect(url="DUMMY_API", token=token)
    assert session_mock.post.return_value == connection.post_json(
        route="DUMMY_ROUTE", data={}
    )


def test_post_json_reraises(session_mock, token):
    """post_json should reraise anything raised by requests."""
    session_mock.post.side_effect = ConnectionError("DUMMY_MESSAGE")
    connection = flowclient.connect(url="DUMMY_API", token=token)
    with pytest.raises(FlowclientConnectionError, match="DUMMY_MESSAGE"):
        connection.post_json(route="DUMMY_ROUTE", data={})


def test_404_raises_error(session_mock, token):
    """Exception should be raised for a 404 response."""
    session_mock.post.return_value.status_code = 404
    connection = flowclient.connect(url="DUMMY_API", token=token)
    with pytest.raises(
        FileNotFoundError, match="DUMMY_API/api/0/DUMMY_ROUTE not found."
    ):
        connection.post_json(route="DUMMY_ROUTE", data={})


@pytest.mark.parametrize("denial_status_code", [401, 403])
def test_access_denied_error(denial_status_code, session_mock, token):
    """If a msg field is available for an access denied it should be used as the error message."""
    session_mock.post.return_value.status_code = denial_status_code
    session_mock.post.return_value.json.return_value = ZMQReply(
        status="error", msg="ERROR_MESSAGE"
    )
    connection = flowclient.connect(url="DUMMY_API", token=token)
    with pytest.raises(FlowclientConnectionError, match="ERROR_MESSAGE"):
        connection.post_json(route="DUMMY_ROUTE", data={})


@pytest.mark.parametrize("denial_status_code", [401, 403])
def test_access_denied_unknown_error(denial_status_code, session_mock, token):
    """If a msg field is not available for an access denied a generic message is supplied."""
    session_mock.post.return_value.status_code = denial_status_code
    session_mock.post.return_value.json.side_effect = ValueError
    connection = flowclient.connect(url="DUMMY_API", token=token)
    with pytest.raises(FlowclientConnectionError, match="Unknown access denied error"):
        connection.post_json(route="DUMMY_ROUTE", data={})


def test_generic_status_code_error(session_mock, token):
    """An error should be raised for status codes that aren't expected."""
    session_mock.post.return_value.status_code = 418
    session_mock.post.return_value.json.return_value = ZMQReply(
        status="error", msg="I AM A TEAPOT"
    )
    connection = flowclient.connect(url="DUMMY_API", token=token)
    with pytest.raises(
        FlowclientConnectionError,
        match="Something went wrong. API returned with status code 418. Error message: 'I AM A TEAPOT'.",
    ):
        connection.post_json(route="DUMMY_ROUTE", data={})


def test_generic_status_code_unknown_error(session_mock, token):
    """An error should be raised for status codes that aren't expected, with a default error message if not given."""
    session_mock.post.return_value.status_code = 418
    session_mock.post.return_value.json.side_effect = (
        ValueError  # indicated that the resonse body doesn't contain valid JSON
    )
    connection = flowclient.connect(url="DUMMY_API", token=token)
    with pytest.raises(
        FlowclientConnectionError,
        match="Something went wrong. API returned with status code 418. Error message: 'the response did not contain valid JSON'.",
    ):
        connection.post_json(route="DUMMY_ROUTE", data={})


def test_generic_status_code_no_payload(session_mock, token):
    """An error should be raised for status codes that aren't expected, with a blank payload if not given."""
    session_mock.post.return_value.status_code = 418
    session_mock.post.return_value.json.return_value = dict(msg="DUMMY_ERROR")
    connection = flowclient.connect(url="DUMMY_API", token=token)
    with pytest.raises(
        FlowclientConnectionError,
        match="Something went wrong. API returned with status code 418. Error message: 'DUMMY_ERROR'.",
    ):
        connection.post_json(route="DUMMY_ROUTE", data={})
