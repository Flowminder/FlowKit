# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from unittest.mock import Mock

import jwt
import pytest

import flowclient
from flowclient.client import Connection, FlowclientConnectionError


@pytest.fixture(autouse=True)
def _get_session_mock(monkeypatch):
    """
    Fixture which replaces the client's `_get_session` method with a mock,
    returning a mocked session object. Yields the session mock for use in
    the test.

    Yields
    ------
    unittests.Mock

    """
    mock = Mock()
    mock.return_value.headers = {}
    monkeypatch.setattr(flowclient.client, "_get_session", mock)
    yield mock.return_value


def test_https_warning(token):
    """
    Test that connection object has the right properties.
    """
    with pytest.warns(
        UserWarning, match="Communications with this server are NOT SECURE."
    ):
        c = Connection("foo", token)


def test_no_warning_for_https(token, monkeypatch):
    """ Test that no insecure warning is raised when connecting via https. """
    monkeypatch.delattr("flowclient.client.HTTP20Adapter")  # Disable hyper
    with pytest.warns(None) as warnings_record:
        c = Connection("https://foo", token)
    assert not warnings_record.list


def test_login(token):
    """
    Test that connection object has the right properties.
    """
    c = Connection("foo", token)
    assert "foo" == c.url
    assert token == c.token
    assert "bar" == c.user
    assert "Authorization" in c.session.headers


def test_connection_repr(token):
    """
    Test string representation of Connection object is correct.
    """
    c = Connection("foo", token)
    assert "bar@foo v0" == str(c)


def test_token_decode_error(token):
    broken_token = token[25:]
    with pytest.raises(FlowclientConnectionError):
        Connection("foo", broken_token)


def test_missing_ident_error():
    bad_token = jwt.encode({"not_identity": "bar"}, "secret")
    with pytest.raises(FlowclientConnectionError):
        Connection("foo", bad_token)
