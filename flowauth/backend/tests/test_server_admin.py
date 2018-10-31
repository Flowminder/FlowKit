# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

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
    assert {
        "id": 1,
        "name": "DUMMY_SERVER_A",
        "secret_key": "DUMMY_SERVER_A_KEY",
    } == response.get_json()


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_get_server_time_limits(client, auth, app):

    # Log in first
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")

    response = client.get(
        "/admin/servers/1/time_limits", headers={"X-CSRF-Token": csrf_cookie}
    )
    assert 200 == response.status_code  # Should get an OK
    assert {
        "longest_token_life": 2,
        "latest_token_expiry": "Wed, 01 Jan 2020 00:00:00 GMT",
    } == response.get_json()


def test_create_server(client, auth, test_admin):
    uid, username, password = test_admin
    response, csrf_cookie = auth.login(username, password)
    response = client.post(
        "/admin/servers",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "latest_token_expiry": "2019-01-01T00:00:00.0Z",
            "longest_token_life": 1440,
            "secret_key": "DUMMY_SECRET_KEY",
            "name": "DUMMY_SERVER_Z",
        },
    )
    assert 200 == response.status_code
    response = client.get("/admin/servers", headers={"X-CSRF-Token": csrf_cookie})
    assert [{"id": 1, "name": "DUMMY_SERVER_Z"}] == response.get_json()


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_rm_server(client, auth):
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    # Need to check that the server is removed, all token for it gone
    response = client.delete("/admin/servers/2", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code
    response = client.get("/admin/servers", headers={"X-CSRF-Token": csrf_cookie})
    assert [{"id": 1, "name": "DUMMY_SERVER_A"}] == response.get_json()
    response = client.get("/user/tokens", headers={"X-CSRF-Token": csrf_cookie})
    assert [] == response.get_json()  # Should have no tokens


def test_edit_server(client, auth, test_admin):
    uid, username, password = test_admin
    response, csrf_cookie = auth.login(username, password)
    response = client.post(
        "/admin/servers",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "latest_token_expiry": "2019-01-01T00:00:00.0Z",
            "longest_token_life": 1440,
            "secret_key": "DUMMY_SECRET_KEY",
            "name": "DUMMY_SERVER_Z",
        },
    )
    response = client.patch(
        "/admin/servers/1",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "latest_token_expiry": "2020-01-01T00:00:00.0Z",
            "longest_token_life": 1,
            "secret_key": "DUMMY_SECRET_KEY_X",
            "name": "DUMMY_SERVER_X",
        },
    )
    assert 200 == response.status_code
    response = client.get("/admin/servers/1", headers={"X-CSRF-Token": csrf_cookie})
    assert {
        "id": 1,
        "name": "DUMMY_SERVER_X",
        "secret_key": "DUMMY_SECRET_KEY_X",
    } == response.get_json()
    response = client.get(
        "/admin/servers/1/time_limits", headers={"X-CSRF-Token": csrf_cookie}
    )
    assert 200 == response.status_code  # Should get an OK
    assert {
        "longest_token_life": 1,
        "latest_token_expiry": "Wed, 01 Jan 2020 00:00:00 GMT",
    } == response.get_json()


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_edit_server_capabilities(client, auth):
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    # Need to check that the server is removed, all token for it gone
    response = client.get(
        "/admin/servers/1/capabilities", headers={"X-CSRF-Token": csrf_cookie}
    )
    assert 200 == response.status_code
    existing_routes = response.get_json()
    assert {
        "DUMMY_ROUTE_A": {
            "id": 1,
            "permissions": {"get_result": True, "run": True, "poll": False},
            "spatial_aggregation": ["admin0", "admin1", "admin2", "admin3"],
        },
        "DUMMY_ROUTE_B": {
            "id": 2,
            "permissions": {"get_result": True, "run": True, "poll": False},
            "spatial_aggregation": ["admin0", "admin1", "admin2", "admin3"],
        },
    } == existing_routes  # Two routes available
    response = client.patch(
        "/admin/servers/1/capabilities",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "DUMMY_ROUTE_A": {
                "id": 1,
                "permissions": {"get_result": True, "run": True, "poll": True},
            }
        },
    )
    assert 200 == response.status_code
    response = client.get(
        "/admin/servers/1/capabilities", headers={"X-CSRF-Token": csrf_cookie}
    )
    assert 200 == response.status_code
    new_routes = response.get_json()
    assert {
        "DUMMY_ROUTE_A": {
            "id": 1,
            "permissions": {"get_result": True, "run": True, "poll": True},
            "spatial_aggregation": ["admin0", "admin1", "admin2", "admin3"],
        },
        "DUMMY_ROUTE_B": {
            "id": 2,
            "permissions": {"get_result": False, "run": False, "poll": False},
            "spatial_aggregation": ["admin0", "admin1", "admin2", "admin3"],
        },
    } == new_routes  # Both capacities still listed, but now no rights


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_add_server_capabilities(client, auth):
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    response = client.post(
        "/admin/capabilities",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"name": "DUMMY_ROUTE_C"},
    )  # Add a new capability first

    response = client.patch(
        "/admin/servers/1/capabilities",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "DUMMY_ROUTE_C": {
                "id": 3,
                "permissions": {"get_result": True, "run": True, "poll": False},
                "spatial_aggregation": ["admin0"],
            }
        },
    )  # Then add it to the server
    assert 200 == response.status_code
    response = client.get(
        "/admin/servers/1/capabilities", headers={"X-CSRF-Token": csrf_cookie}
    )
    assert 200 == response.status_code
    new_routes = response.get_json()
    assert {
        "id": 3,
        "permissions": {"get_result": True, "run": True, "poll": False},
        "spatial_aggregation": ["admin0"],
    } == new_routes["DUMMY_ROUTE_C"]


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_group_server_listing(client, auth):
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    response = client.get(
        "/admin/groups/1/servers", headers={"X-CSRF-Token": csrf_cookie}
    )

    assert 200 == response.status_code
    new_routes = response.get_json()
    assert [{"id": 1, "name": "DUMMY_SERVER_A"}] == new_routes


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_group_server_time_limits(client, auth):
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    response = client.get(
        "/admin/groups/1/servers/1/time_limits", headers={"X-CSRF-Token": csrf_cookie}
    )

    assert 200 == response.status_code
    json = response.get_json()
    assert 2 == json["longest_token_life"]
    assert "Tue, 01 Jan 2019 00:00:00 GMT" == json["latest_token_expiry"]


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_group_server_rights(client, auth):
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    response = client.get(
        "/admin/groups/1/servers/1/capabilities", headers={"X-CSRF-Token": csrf_cookie}
    )

    assert 200 == response.status_code
    json = response.get_json()
    assert {"get_result": True, "run": True, "poll": False} == json["DUMMY_ROUTE_A"][
        "permissions"
    ]
    assert {"get_result": False, "run": False, "poll": False} == json["DUMMY_ROUTE_B"][
        "permissions"
    ]


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_edit_group_server_rights(client, auth):
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    response = client.patch(
        "/admin/groups/1/servers",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "servers": [
                {
                    "id": 1,
                    "max_life": 1,
                    "latest_expiry": "2018-01-01T00:00:00.0Z",
                    "rights": {"DUMMY_ROUTE_A": {"permissions": {"get_result": False}}},
                }
            ]
        },
    )

    assert 200 == response.status_code
    response = client.get(
        "/admin/groups/1/servers/1/capabilities", headers={"X-CSRF-Token": csrf_cookie}
    )

    assert 200 == response.status_code
    json = response.get_json()
    assert {"get_result": False, "run": True, "poll": False} == json["DUMMY_ROUTE_A"][
        "permissions"
    ]
    assert {"get_result": False, "run": False, "poll": False} == json["DUMMY_ROUTE_B"][
        "permissions"
    ]

    response = client.get(
        "/admin/groups/1/servers/1/time_limits", headers={"X-CSRF-Token": csrf_cookie}
    )
    json = response.get_json()
    assert 1 == json["longest_token_life"]
    assert "Mon, 01 Jan 2018 00:00:00 GMT" == json["latest_token_expiry"]


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_edit_group_server_rights_rejected_for_max_life(client, auth):
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    response = client.patch(
        "/admin/groups/1/servers",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "servers": [
                {
                    "id": 1,
                    "max_life": 99,
                    "latest_expiry": "2018-01-01T00:00:00.0Z",
                    "rights": {"DUMMY_ROUTE_A": {"permissions": {"get_result": False}}},
                }
            ]
        },
    )
    assert 400 == response.status_code
    assert {"code": 400, "message": "lifetime_too_long"} == response.get_json()


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_edit_group_server_rights_rejected_for_expiry(client, auth):
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    response = client.patch(
        "/admin/groups/1/servers",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "servers": [
                {
                    "id": 1,
                    "max_life": 1,
                    "latest_expiry": "2040-01-01T00:00:00.0Z",
                    "rights": {"DUMMY_ROUTE_A": {"permissions": {"get_result": False}}},
                }
            ]
        },
    )
    assert 400 == response.status_code
    assert {"code": 400, "message": "end_date_too_late"} == response.get_json()


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_edit_group_server_rights_rejected_for_rights(client, auth):
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    response = client.patch(
        "/admin/groups/1/servers",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "servers": [
                {
                    "id": 1,
                    "max_life": 1,
                    "latest_expiry": "2018-01-01T00:00:00.0Z",
                    "rights": {
                        "DUMMY_ROUTE_A": {
                            "permissions": {"poll": True},
                            "spatial_aggregation": ["admin0"],
                        }
                    },
                }
            ]
        },
    )
    assert 400 == response.status_code
    assert {
        "code": 400,
        "message": "capability_not_allowed_on_server",
    } == response.get_json()
