import pytest


@pytest.mark.usefixtures("test_data_with_access_rights")
@pytest.fixture
def logged_in_session(client, auth, app):
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")


def test_list_roles(client, auth, app, test_roles, test_scopes):
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    response = client.get(
        "/admin/servers/1/roles", headers={"X-CSRF-Token": csrf_cookie}
    )
    assert 200 == response.status_code
    assert [
        {
            "id": 1,
            "name": "runner",
            "scopes": ["run", "read", "dummy_query:admin_level_1"],
        },
        {"id": 2, "name": "reader", "scopes": ["read"]},
    ] == response.get_json()


def test_add_role(client, auth, app, test_scopes):
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    response = client.post(
        "/admin/servers/1/roles",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "name": "test_role",
            "scopes": ["run", "dummy_scope:admin_level_2"],
            "server_id": 1,
        },
    )
    assert response.status_code == 200
    response = client.get(
        "/admin/servers/1/roles", headers={"X-CSRF-Token": csrf_cookie}
    )
    assert response.json == [
        {"id": 1, "name": "test_role", "scopes": ["run", "dummy_scope:admin_level_2"]}
    ]


def test_list_scopes_in_role(client, auth, test_scopes, test_roles):
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    response = client.get(
        "/servers/1/roles/1/scopes", headers={"X-CSRF-Token": csrf_cookie}
    )
    assert 200 == response.status_code
    assert [["DUMMY_SCOPE_1"]] == response.get_json()
    response = client.get(
        "/admin/roles/2/scopes", headers={"X-CSRF-Token": csrf_cookie}
    )
    assert 200 == response.status_code
    assert [
        "DUMMY_SCOPE_2",
    ] == response.get_json()
