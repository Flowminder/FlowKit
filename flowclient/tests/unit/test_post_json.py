# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from requests import ConnectionError

import flowclient
from flowclient.client import FlowclientConnectionError
from flowclient.zmq_helpers import ZMQReply


@pytest.mark.parametrize("status_code", [202])
def test_post_json_good_statuses(status_code, session_mock, token):
    """response object should be returned for OK status codes.."""
    session_mock.post.return_value.status_code = status_code
    connection = flowclient.connect("DUMMY_API", token)
    assert session_mock.post.return_value == connection.post_json("DUMMY_ROUTE", {})


def test_post_json_reraises(session_mock, token):
    """post_json should reraise anything raised by requests."""
    session_mock.post.side_effect = ConnectionError("DUMMY_MESSAGE")
    connection = flowclient.connect("DUMMY_API", token)
    with pytest.raises(FlowclientConnectionError, match="DUMMY_MESSAGE"):
        connection.post_json("DUMMY_ROUTE", {})


def test_404_raises_error(session_mock, token):
    """Exception should be raised for a 404 response."""
    session_mock.post.return_value.status_code = 404
    connection = flowclient.connect("DUMMY_API", token)
    with pytest.raises(
        FileNotFoundError, match="DUMMY_API/api/0/DUMMY_ROUTE not found."
    ):
        connection.post_json("DUMMY_ROUTE", {})


def test_401_error(session_mock, token):
    """If a msg field is available for a 401, it should be used as the error message."""
    session_mock.post.return_value.status_code = 401
    session_mock.post.return_value.json.return_value = ZMQReply(
        status="error", msg="ERROR_MESSAGE"
    ).as_json()
    connection = flowclient.connect("DUMMY_API", token)
    with pytest.raises(FlowclientConnectionError, match="ERROR_MESSAGE"):
        connection.post_json("DUMMY_ROUTE", {})


def test_401_unknown_error(session_mock, token):
    """If a msg field is not available for a 401, a generic message is used."""
    session_mock.get.return_value.status_code = 401
    session_mock.get.return_value.json.return_value = ZMQReply(
        status="error", msg="Unknown access denied error", payload={}
    ).as_json()
    connection = flowclient.connect("DUMMY_API", token)
    with pytest.raises(FlowclientConnectionError, match="Unknown access denied error"):
        connection.get_url("DUMMY_ROUTE")


def test_generic_status_code_error(session_mock, token):
    """An error should be raised for status codes that aren't expected."""
    session_mock.post.return_value.status_code = 418
    session_mock.post.return_value.json.return_value = ZMQReply(
        status="error", msg="I AM A TEAPOT"
    ).as_json()
    connection = flowclient.connect("DUMMY_API", token)
    with pytest.raises(
        FlowclientConnectionError,
        match="Something went wrong: I AM A TEAPOT. API returned with status code: 418",
    ):
        connection.post_json("DUMMY_ROUTE", {})


def test_generic_status_code_unknown_error(session_mock, token):
    """An error should be raised for status codes that aren't expected, with a default error message if not given."""
    session_mock.post.return_value.status_code = 418
    session_mock.post.return_value.json.side_effect = (
        ValueError
    )  # indicated that the resonse body doesn't contain valid JSON
    connection = flowclient.connect("DUMMY_API", token)
    with pytest.raises(
        FlowclientConnectionError,
        match="Something went wrong: the response did not contain valid JSON. API returned with status code: 418.",
    ):
        connection.post_json("DUMMY_ROUTE", {})
