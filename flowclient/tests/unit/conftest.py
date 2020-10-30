# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from unittest.mock import Mock

import httpx
import jwt
import pytest

import flowclient


@pytest.fixture
def session_mock(monkeypatch):
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
    monkeypatch.setattr(httpx, "Client", mock)
    monkeypatch.setattr(httpx, "AsyncClient", mock)
    yield mock.return_value


@pytest.fixture
def token():
    return jwt.encode({"identity": "bar"}, "secret")
