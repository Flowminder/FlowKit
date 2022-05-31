# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http.//mozilla.org/MPL/2.0/.
import datetime

from freezegun import freeze_time
from werkzeug.http import http_date

import pytest


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_list_servers(client, auth, app):

    # Log in first
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")

    response = client.get("/admin/servers", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code  # Should get an OK
    assert [
        {"id": 1, "name": "DUMMY_SERVER_A"},
        {"id": 2, "name": "DUMMY_SERVER_B"},
    ] == response.get_json()


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_get_server(client, auth, app):

    # Log in first
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")

    response = client.get("/admin/servers/1", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code  # Should get an OK
    assert {"id": 1, "name": "DUMMY_SERVER_A"} == response.get_json()


@freeze_time("2020-12-31")
@pytest.mark.usefixtures("test_data_with_access_rights")
def test_get_server_time_limits(client, auth, app):

    # Log in first
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")

    response = client.get(
        "/admin/servers/1/time_limits", headers={"X-CSRF-Token": csrf_cookie}
    )
    assert 200 == response.status_code  # Should get an OK
    assert {
        "longest_token_life_minutes": 2880,
        "latest_token_expiry": http_date(
            (datetime.datetime.now().date() + datetime.timedelta(days=365)).timetuple()
        ),
    } == response.get_json()


def test_create_server(client, auth, test_admin):
    uid, username, password = test_admin
    response, csrf_cookie = auth.login(username, password)
    response = client.post(
        "/admin/servers",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "latest_token_expiry": "2019-01-01T00:00:00.0Z",
            "longest_token_life_minutes": 1440,
            "name": "DUMMY_SERVER_Z",
            "scopes": ["run", "read", "dummy_scope_1"],
        },
    )
    assert 200 == response.status_code
    response = client.get("/admin/servers", headers={"X-CSRF-Token": csrf_cookie})
    assert [{"id": 1, "name": "DUMMY_SERVER_Z"}] == response.get_json()
    response = client.get(
        "/admin/servers/1/scopes", headers={"X-CSRF-Token": csrf_cookie}
    )
    assert {"1": "run", "2": "read", "3": "dummy_scope_1"} == response.get_json()


def test_create_server_errors_with_missing_name(client, auth, test_admin):
    """Should block create of server with no name key and return error json."""
    uid, username, password = test_admin
    response, csrf_cookie = auth.login(username, password)
    response = client.post(
        "/admin/servers",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "latest_token_expiry": "2019-01-01T00:00:00.0Z",
            "longest_token_life_minutes": 1440,
            "secret_key": "DUMMY_SECRET_KEY",
        },
    )
    assert 400 == response.status_code
    assert {
        "bad_field": "name",
        "code": 400,
        "message": "Must provide server name",
    } == response.get_json()
    response = client.get("/admin/servers", headers={"X-CSRF-Token": csrf_cookie})
    assert [] == response.get_json()


@pytest.mark.parametrize(
    "name, expected_message",
    [
        ("", "Must provide server name"),
        ("A" * 121, "Server name must be 120 characters or less."),
    ],
)
def test_create_server_errors_with_bad_name(
    name, expected_message, client, auth, test_admin
):
    """Should block create of server with zero length or too long name and return error json."""
    uid, username, password = test_admin
    response, csrf_cookie = auth.login(username, password)
    response = client.post(
        "/admin/servers",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "latest_token_expiry": "2019-01-01T00:00:00.0Z",
            "longest_token_life_minutes": 1440,
            "secret_key": "DUMMY_SECRET_KEY",
            "name": name,
        },
    )
    assert 400 == response.status_code
    assert {
        "bad_field": "name",
        "code": 400,
        "message": expected_message,
    } == response.get_json()
    response = client.get("/admin/servers", headers={"X-CSRF-Token": csrf_cookie})
    assert [] == response.get_json()


def test_create_server_errors_with_same_name(client, auth, test_admin):
    """Should block create of server with same name as an existing one and return error json."""
    uid, username, password = test_admin
    response, csrf_cookie = auth.login(username, password)
    for i in range(2):
        response = client.post(
            "/admin/servers",
            headers={"X-CSRF-Token": csrf_cookie},
            json={
                "latest_token_expiry": "2019-01-01T00:00:00.0Z",
                "longest_token_life_minutes": 1440,
                "name": "TEST_SERVER",
            },
        )
    assert 400 == response.status_code
    assert {
        "bad_field": "name",
        "code": 400,
        "message": "Server with this name already exists.",
    } == response.get_json()
    response = client.get("/admin/servers", headers={"X-CSRF-Token": csrf_cookie})
    assert [{"id": 1, "name": "TEST_SERVER"}] == response.get_json()


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_rm_server(client, auth):
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    # Need to check that the server is removed, all token for it gone
    response = client.delete("/admin/servers/2", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code
    response = client.get("/admin/servers", headers={"X-CSRF-Token": csrf_cookie})
    assert [{"id": 1, "name": "DUMMY_SERVER_A"}] == response.get_json()
    # response = client.get("/tokens/tokens", headers={"X-CSRF-Token": csrf_cookie})
    # assert [] == response.get_json()  # Should have no tokens


def test_edit_server(client, auth, test_admin):
    uid, username, password = test_admin
    response, csrf_cookie = auth.login(username, password)
    response = client.post(
        "/admin/servers",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "latest_token_expiry": "2019-01-01T00:00:00.0Z",
            "longest_token_life_minutes": 1440,
            "name": "DUMMY_SERVER_Z",
        },
    )
    response = client.patch(
        "/admin/servers/1",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "latest_token_expiry": "2020-01-01T00:00:00.0Z",
            "longest_token_life_minutes": 1,
            "name": "DUMMY_SERVER_X",
        },
    )
    assert 200 == response.status_code
    response = client.get("/admin/servers/1", headers={"X-CSRF-Token": csrf_cookie})
    assert {"id": 1, "name": "DUMMY_SERVER_X"} == response.get_json()
    response = client.get(
        "/admin/servers/1/time_limits", headers={"X-CSRF-Token": csrf_cookie}
    )
    assert 200 == response.status_code  # Should get an OK
    assert {
        "longest_token_life_minutes": 1,
        "latest_token_expiry": "Wed, 01 Jan 2020 00:00:00 GMT",
    } == response.get_json()


def test_list_scopes(client, auth, test_scopes, test_servers, test_admin):
    uid, uname, password = test_admin
    response, csrf_cookie = auth.login(uname, password)
    response = client.get(
        "/admin/servers/1/scopes",
        headers={"X-CSRF-Token": csrf_cookie},
    )
    assert response.status_code == 200
    assert response.json == {
        "get_result": True,
        "run": True,
        "dummy_query:admin_level_1": True,
    }


def test_enabled_scopes(client, auth, test_scopes, test_servers, test_admin):
    uid, uname, password = test_admin
    response, csrf_cookie = auth.login(uname, password)
    json = {"dummy_query:admin_level_1": False}
    response = client.patch(
        "/admin/servers/1/scopes", json=json, headers={"X-CSRF-Token": csrf_cookie}
    )
    assert response.status_code == 200
    assert response.json == {
        "get_result": True,
        "run": True,
        "dummy_query:admin_level_1": False,
    }
