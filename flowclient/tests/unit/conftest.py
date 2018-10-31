# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from unittest.mock import Mock

import jwt
import pytest

import flowclient


@pytest.fixture
def api_mock(monkeypatch):
    """
    Fixture which replaces the client's `_get_session` method with a mock,
    returning a mocked session object. Yields the session mock for use in
    the test.

    Yields
    ------
    unittests.Mock

    """
    mock = Mock()
    get_post_mock = Mock()
    mock.return_value.get = get_post_mock
    mock.return_value.post = get_post_mock
    mock.return_value.headers = {}
    monkeypatch.setattr(flowclient.client, "_get_session", mock)
    yield mock.return_value


@pytest.fixture
def api_response(api_mock):
    """
    Fixture returning a mocked api response.

    Parameters
    ----------
    api_mock: pytest.fixture
        Mock which is standing in for the session object

    Yields
    ------
    Mock
        Mock object returned by any calls to _get_session().get or _get_session().post

    """
    yield api_mock.get.return_value


@pytest.fixture
def token():
    return jwt.encode({"identity": "bar"}, "secret")
