# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_list_capabilities(client, auth, app):

    # Log in first
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")

    response = client.get("/admin/capabilities", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code  # Should get an OK
    assert {
        "DUMMY_ROUTE_A": {
            "id": 1,
            "permissions": {"get_result": False, "run": False, "poll": False},
            "spatial_aggregation": [],
        },
        "DUMMY_ROUTE_B": {
            "id": 2,
            "permissions": {"get_result": False, "run": False, "poll": False},
            "spatial_aggregation": [],
        },
    } == response.get_json()


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_add_capability(client, auth, app):

    # Log in first
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    response = client.post(
        "/admin/capabilities",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"name": "DUMMY_ROUTE_C"},
    )
    assert 200 == response.status_code  # Should get an OK
    assert {"id": 3, "name": "DUMMY_ROUTE_C"} == response.get_json()
    response = client.get("/admin/capabilities", headers={"X-CSRF-Token": csrf_cookie})

    assert {
        "DUMMY_ROUTE_A": {
            "id": 1,
            "permissions": {"get_result": False, "run": False, "poll": False},
            "spatial_aggregation": [],
        },
        "DUMMY_ROUTE_B": {
            "id": 2,
            "permissions": {"get_result": False, "run": False, "poll": False},
            "spatial_aggregation": [],
        },
        "DUMMY_ROUTE_C": {
            "id": 3,
            "permissions": {"get_result": False, "run": False, "poll": False},
            "spatial_aggregation": [],
        },
    } == response.get_json()  # Newly added capacity should appear in the list of servers


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_remove_capability(client, auth, app):

    # Log in first
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    response = client.delete(
        "/admin/capabilities/1", headers={"X-CSRF-Token": csrf_cookie}
    )
    assert 200 == response.status_code  # Should get an OK
    response = client.get("/admin/capabilities", headers={"X-CSRF-Token": csrf_cookie})

    assert {
        "DUMMY_ROUTE_B": {
            "id": 2,
            "permissions": {"get_result": False, "run": False, "poll": False},
            "spatial_aggregation": [],
        }
    } == response.get_json()  # Capacity should be gone from the list


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_remove_capability_removes_from_server(client, auth, app):

    # Log in first
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    response = client.delete(
        "/admin/capabilities/1", headers={"X-CSRF-Token": csrf_cookie}
    )
    assert 200 == response.status_code  # Should get an OK
    response = client.get(
        "/admin/servers/1/capabilities", headers={"X-CSRF-Token": csrf_cookie}
    )

    assert {
        "DUMMY_ROUTE_B": {
            "id": 2,
            "permissions": {"get_result": True, "run": True, "poll": False},
            "spatial_aggregation": ["admin0", "admin1", "admin2", "admin3"],
        }
    } == response.get_json()  # Capacity should be gone from the list
