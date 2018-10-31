# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime

import pytest
from pytest import approx


def test_user_group_listing(client, auth, test_user):
    """Test that user's groups are returned in expected format.."""
    uid, username, password = test_user

    # Log in first
    response, csrf_cookie = auth.login(username, password)
    response = client.get("/user/groups", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code  # Should get an OK

    assert [
        {"id": 1, "group_name": username}
    ] == response.get_json()  # Should get exactly one group back, their user group


@pytest.mark.usefixtures("test_data")
def test_server_access(client, auth):
    # Log in first
    response, csrf_cookie = auth.login("TEST_USER", "DUMMY_PASSWORD")
    response = client.get("/user/servers", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code  # Should get an OK
    assert [{"id": 1, "server_name": "DUMMY_SERVER_A"}] == response.get_json()


@pytest.mark.usefixtures("test_data")
def test_no_tokens(client, auth):
    # Log in first
    response, csrf_cookie = auth.login("TEST_USER", "DUMMY_PASSWORD")
    response = client.get("/user/tokens", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code  # Should get an OK
    result = response.get_json()

    assert [] == result  # Can't do anything


@pytest.mark.usefixtures("test_data")
def test_list_tokens(client, auth):
    # Log in first
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    response = client.get("/user/tokens", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code  # Should get an OK
    result = response.get_json()

    assert [
        {
            "id": 1,
            "name": "DUMMY_TOKEN",
            "token": "DUMMY_TOKEN_STRING",
            "expires": "Tue, 01 Jan 2019 00:00:00 GMT",
            "server_name": "DUMMY_SERVER_B",
            "username": "TEST_ADMIN",
        }
    ] == result


@pytest.mark.usefixtures("test_data")
def test_list_tokens_for_server(client, auth):
    # Log in first
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    response = client.get("/user/tokens/2", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code  # Should get an OK
    result = response.get_json()

    assert [
        {
            "id": 1,
            "name": "DUMMY_TOKEN",
            "token": "DUMMY_TOKEN_STRING",
            "expires": "Tue, 01 Jan 2019 00:00:00 GMT",
            "server_name": "DUMMY_SERVER_B",
            "username": "TEST_ADMIN",
        }
    ] == result

    response = client.get("/user/tokens/1", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code  # Should get an OK
    result = response.get_json()

    assert [] == result
