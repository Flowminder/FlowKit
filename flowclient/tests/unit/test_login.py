# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from unittest.mock import Mock

import jwt
import pytest

from flowclient.client import Connection, FlowclientConnectionError


pytestmark = pytest.mark.usefixtures("session_mock")


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
    assert isinstance(
        c.session.verify, Mock
    )  # Shouldn't be set if no ssl_certificate passed


def test_ssl_cert_path_set(token):
    """
    Test that if a path to certificate is given it gets set on the session object.
    """
    c = Connection("foo", token, ssl_certificate="DUMMY_CERT_PATH")
    assert "DUMMY_CERT_PATH" == c.session.verify


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
