from urllib import response
import pytest
from freezegun import freeze_time

from flowauth.roles import list_my_roles_on_server


@freeze_time("2020-12-31")
def test_list_roles(client, auth, app, test_roles, test_scopes):
    with app.app_context():
        response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
        response = client.get(
            "/admin/servers/1/roles", headers={"X-CSRF-Token": csrf_cookie}
        )
        assert response.status_code == 200
        assert [
            {
                "id": 1,
                "name": "runner",
                "scopes": ["dummy_query:admin_level_1", "get_result", "run"],
                "latest_token_expiry": "2021-12-31T00:00:00.000000Z",
                "longest_token_life_minutes": 2880,
            },
            {
                "id": 2,
                "name": "reader",
                "scopes": ["get_result"],
                "latest_token_expiry": "2021-12-31T00:00:00.000000Z",
                "longest_token_life_minutes": 2880,
            },
        ] == response.get_json()


def test_list_roles_user(client, auth, app, test_servers, test_user_with_roles):
    with app.app_context():
        uid, uname, passwd = test_user_with_roles
        response, csrf_cookie = auth.login(uname, passwd)
        assert response.status_code == 200
        response = client.get("/roles/server/1", headers={"X-CSRF=Token": csrf_cookie})
        assert response.status_code == 200
        assert [
            {
                "id": 1,
                "name": "runner",
                "scopes": ["dummy_query:admin_level_1", "get_result", "run"],
                "latest_token_expiry": "2021-12-31T00:00:00.000000Z",
                "longest_token_life_minutes": 2880,
            },
            {
                "id": 2,
                "name": "reader",
                "scopes": ["get_result"],
                "latest_token_expiry": "2021-12-31T00:00:00.000000Z",
                "longest_token_life_minutes": 2880,
            },
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
            "latest_token_expiry": "2021-12-31T12:00:00.0Z",
            "longest_token_life_minutes": 2880,
        },
    )
    assert response.status_code == 200
    response = client.get(
        "/admin/servers/1/roles", headers={"X-CSRF-Token": csrf_cookie}
    )
    assert response.json == [
        {
            "id": 1,
            "name": "test_role",
            "scopes": ["run", "dummy_scope:admin_level_2"],
            "latest_token_expiry": "2021-12-31T12:00:00.000000Z",
            "longest_token_life_minutes": 2880,
        }
    ]


@pytest.mark.skip(
    reason="Scopes are included in regular listing now; not needed anymore"
)
def test_list_scopes_in_role(client, auth, test_scopes, test_roles):
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    response = client.get(
        "/servers/1/roles/1/scopes", headers={"X-CSRF-Token": csrf_cookie}
    )
    assert response.status_code == 200
    assert [["DUMMY_SCOPE_1"]] == response.get_json()
    response = client.get(
        "/admin/roles/2/scopes", headers={"X-CSRF-Token": csrf_cookie}
    )
    assert response.status_code == 200
    assert [
        "DUMMY_SCOPE_2",
    ] == response.get_json()
