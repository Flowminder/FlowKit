# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from unittest.mock import Mock

import jwt
import pytest

from flowclient.errors import FlowclientConnectionError
from flowclient import Connection

pytestmark = pytest.mark.usefixtures("session_mock")


def test_https_warning(token):
    """
    Test that connection object has the right properties.
    """
    with pytest.warns(
        UserWarning, match="Communications with this server are NOT SECURE."
    ):
        c = Connection(url="foo", token=token)


def test_no_warning_for_https(token, monkeypatch):
    """Test that no insecure warning is raised when connecting via https."""
    with pytest.warns(None) as warnings_record:
        c = Connection(url="https://foo", token=token)
    assert not warnings_record.list


def test_login(token):
    """
    Test that connection object has the right properties.
    """
    c = Connection(url="foo", token=token)
    assert "foo" == c.url
    assert token == c.token
    assert "bar" == c.user
    assert "Authorization" in c.session.headers


def test_connection_repr(token):
    """
    Test string representation of Connection object is correct.
    """
    c = Connection(url="foo", token=token)
    assert "bar@foo v0" == str(c)


def test_token_decode_error(token):
    broken_token = token[25:]
    with pytest.raises(FlowclientConnectionError):
        Connection(url="foo", token=broken_token)


def test_missing_ident_error():
    bad_token = jwt.encode({"not_identity": "bar"}, "secret")
    with pytest.raises(FlowclientConnectionError):
        Connection(url="foo", token=bad_token)
