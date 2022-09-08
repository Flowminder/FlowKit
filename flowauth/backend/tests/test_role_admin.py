from urllib import response
import pytest
from freezegun import freeze_time

from flowauth.roles import list_my_roles_on_server, role_to_dict


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
                "scopes": [
                    "run",
                    "get_result",
                    "dummy_agg_unit:dummy_query:dummy_query",
                ],
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


@pytest.mark.skip(
    reason="Skipping until I can determine if this is being used in frontend"
)
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
                "scopes": [
                    "run",
                    "get_result",
                    "dummy_agg_unit:dummy_query:dummy_query",
                ],
                "latest_token_expiry": "2021-12-31T00:00:00.000000Z",
                "longest_token_life_minutes": 2880,
                "server": 1,
            },
            {
                "id": 2,
                "name": "reader",
                "scopes": ["get_result"],
                "latest_token_expiry": "2021-12-31T00:00:00.000000Z",
                "longest_token_life_minutes": 2880,
                "server": 1,
            },
        ] == response.get_json()


def test_get_role(client, auth, app, test_user_with_roles):
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    uid, _, _ = test_user_with_roles
    response = client.get("/roles/1", headers={"X-CSRF_Token": csrf_cookie})
    assert response.json == {
        "id": 1,
        "name": "runner",
        "scopes": [1, 3, 4],
        "server": 1,
        "longest_token_life_minutes": 2880,
        "latest_token_expiry": "2021-12-31T00:00:00.000000Z",
        "users": [uid],
    }


def test_add_role(client, auth, app, test_scopes):
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    response = client.post(
        "roles/",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "name": "test_role",
            "scopes": [
                3,
                4,
            ],  # "run" and "dummy_agg_unit:dummy_scope:dummy_scope", server 1
            "server_id": 1,
            "latest_token_expiry": "2021-12-31T12:00:00.0Z",
            "longest_token_life_minutes": 2 * 24 * 60,
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
            "scopes": ["run", "dummy_agg_unit:dummy_query:dummy_query"],
            "latest_token_expiry": "2021-12-31T12:00:00.000000Z",
            "longest_token_life_minutes": 2 * 24 * 60,
        }
    ]


def test_update_role(auth, client, test_roles):
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    response = client.patch(
        "/roles/1",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"name": "runner_patched", "users": [1]},
    )
    assert response.status_code == 200
    response = client.get("/roles/1", headers={"X-CSRF-Token": csrf_cookie})
    assert response.json == {
        "id": 1,
        "name": "runner_patched",
        "scopes": [1, 3, 4],
        "server": 1,
        "longest_token_life_minutes": 2880,
        "latest_token_expiry": "2021-12-31T00:00:00.000000Z",
        "users": [1],
    }


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
