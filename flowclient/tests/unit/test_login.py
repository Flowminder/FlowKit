# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowclient.client import Connection


def test_https_warning(token, api_mock):
    """
    Test that connection object has the right properties.
    """
    with pytest.warns(
        UserWarning, match="Communications with this server are NOT SECURE."
    ):
        c = Connection("foo", token)


def test_no_warning_for_https(token, api_mock, monkeypatch):
    """ Test that no insecure warning is raised when connecting via https. """
    monkeypatch.delattr("flowclient.client.HTTP20Adapter")  # Disable hyper
    with pytest.warns(None) as warnings_record:
        c = Connection("https://foo", token)
    assert not warnings_record.list


def test_login(token, api_mock):
    """
    Test that connection object has the right properties.
    """
    c = Connection("foo", token)
    assert "foo" == c.url
    assert token == c.token
    assert "bar" == c.user
    assert "Authorization" in c.session.headers


def test_connection_repr(token, api_mock):
    """
    Test string representation of Connection object is correct.
    """
    c = Connection("foo", token)
    assert "bar@foo v0" == str(c)
