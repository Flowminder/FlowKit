# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime

import pytest
from pytest import approx
from werkzeug.http import http_date


def test_user_group_listing(client, auth, test_user):
    """Test that user's groups are returned in expected format.."""
    uid, username, password = test_user

    # Log in first
    response, csrf_cookie = auth.login(username, password)
    response = client.get("/tokens/groups", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code  # Should get an OK

    assert [
        {"id": uid, "group_name": username}
    ] == response.get_json()  # Should get exactly one group back, their user group


@pytest.mark.usefixtures("test_data")
def test_server_access(client, auth, test_user):
    uid, uname, upass = test_user
    # Log in first
    response, csrf_cookie = auth.login(uname, upass)
    response = client.get("/tokens/servers", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code  # Should get an OK
    assert [{"id": 1, "server_name": "DUMMY_SERVER_A"}] == response.get_json()


@pytest.mark.usefixtures("test_data")
def test_no_tokens(client, auth, test_user):
    uid, uname, upass = test_user
    # Log in first
    response, csrf_cookie = auth.login(uname, upass)
    response = client.get("/tokens/tokens", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code  # Should get an OK
    result = response.get_json()

    assert [] == result  # Can't do anything


@pytest.mark.usefixtures("test_data")
def test_list_tokens(client, auth, test_admin):
    uid, uname, upass = test_admin
    # Log in first
    response, csrf_cookie = auth.login(uname, upass)
    response = client.get("/tokens/tokens", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code  # Should get an OK
    result = response.get_json()

    assert [
        {
            "id": 1,
            "name": "DUMMY_TOKEN",
            "token": "DUMMY_TOKEN_STRING",
            "expires": http_date(
                (
                    datetime.datetime.now().date() + datetime.timedelta(days=1)
                ).timetuple()
            ),
            "server_name": "DUMMY_SERVER_B",
            "username": "TEST_ADMIN",
        }
    ] == result


@pytest.mark.usefixtures("test_data")
def test_list_tokens_for_server(client, auth, test_admin):
    uid, uname, upass = test_admin
    # Log in first
    response, csrf_cookie = auth.login(uname, upass)
    response = client.get("/tokens/tokens/2", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code  # Should get an OK
    result = response.get_json()

    assert [
        {
            "id": 1,
            "name": "DUMMY_TOKEN",
            "token": "DUMMY_TOKEN_STRING",
            "expires": http_date(
                (
                    datetime.datetime.now().date() + datetime.timedelta(days=1)
                ).timetuple()
            ),
            "server_name": "DUMMY_SERVER_B",
            "username": "TEST_ADMIN",
        }
    ] == result

    response = client.get("/tokens/tokens/1", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code  # Should get an OK
    result = response.get_json()

    assert [] == result
