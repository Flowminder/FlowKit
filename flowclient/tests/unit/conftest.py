# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from unittest.mock import Mock

import httpx
import jwt
import pytest
import respx

import flowclient


@pytest.fixture
def dummy_api_url():
    return "https://DUMMY_API"


@pytest.fixture
def dummy_route(dummy_api_url):
    return f"{dummy_api_url}/api/0/DUMMY_ROUTE"


@pytest.fixture
def session_mock(dummy_api_url):
    """
    Fixture which replaces the client's `_get_session` method with a mock,
    returning a mocked session object. Yields the session mock for use in
    the test.

    Yields
    ------
    unittests.Mock

    """
    with respx.mock(base_url=f"{dummy_api_url}/api/0") as respx_mock:
        yield respx_mock


@pytest.fixture
def token():
    return jwt.encode({"identity": "bar"}, "secret")


@pytest.fixture
def flowclient_connection(session_mock, dummy_route, dummy_api_url, token):
    yield flowclient.connect(url=dummy_api_url, token=token)


@pytest.fixture
def async_flowclient_connection(session_mock, dummy_route, dummy_api_url, token):
    yield flowclient.connect_async(url=dummy_api_url, token=token)
