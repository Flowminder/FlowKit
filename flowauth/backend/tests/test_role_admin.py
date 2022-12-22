# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from datetime import timedelta
from sys import base_prefix
from time import strptime
from urllib import response
import pytest
from freezegun import freeze_time

from flowauth.models import db
from flowauth.invalid_usage import InvalidUsage


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
                "latest_token_expiry": "2020-12-31T00:05:00.000000Z",
                "longest_token_life_minutes": 2880,
            },
            {
                "id": 2,
                "name": "reader",
                "scopes": ["get_result"],
                "latest_token_expiry": "2020-12-31T00:05:00.000000Z",
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
                "latest_token_expiry": "2020-12-31T00:05:00.000000Z",
                "longest_token_life_minutes": 2880,
                "server": 1,
            },
            {
                "id": 2,
                "name": "reader",
                "scopes": ["get_result"],
                "latest_token_expiry": "2020-12-31T00:05:00.000000Z",
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
        "latest_token_expiry": "2020-12-31T00:05:00.000000Z",
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
            "latest_token_expiry": "2020-12-31T12:00:00.0Z",
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
            "latest_token_expiry": "2020-12-31T12:00:00.000000Z",
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
        "latest_token_expiry": "2020-12-31T00:05:00.000000Z",
        "users": [1],
    }


def test_invalid_role(app, auth, client, test_servers):
    with app.app_context():
        server, _ = test_servers
        db.session.add(server)
        response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
        invalid_expiry = server.latest_token_expiry + timedelta(minutes=1)
        base_payload = {
            "name": "test_role",
            "scopes": [
                3,
                4,
            ],  # "run" and "dummy_agg_unit:dummy_scope:dummy_scope", server 1
            "server_id": server.id,
            "latest_token_expiry": server.latest_token_expiry.strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            ),
            "longest_token_life_minutes": server.longest_token_life_minutes,
        }
        invalid_expiry_payload = base_payload.copy()
        invalid_expiry_payload["latest_token_expiry"] = invalid_expiry.strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        response = client.post(
            "/roles/",
            headers={"X-CSRF-Token": csrf_cookie},
            json=invalid_expiry_payload,
        )
        assert response.status_code == 400
        invalid_life_payload = base_payload.copy()
        invalid_life_payload["longest_token_life_minutes"] = (
            server.longest_token_life_minutes + 1
        )
        response = client.post(
            "/roles/", headers={"X-CSRF-Token": csrf_cookie}, json=invalid_life_payload
        )
        assert response.status_code == 400


def test_role_server_check(app, auth, client, test_scopes, test_roles):
    # Tests that if you add a scope to a role, the scope exists on the server
    with app.app_context():
        read_a, read_b, _, _ = test_scopes
        db.session.add(read_a)
        db.session.add(read_b)
        response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
        # Role 1 lives on server 1.
        response = client.patch(
            "/roles/1",
            headers={"X-CSRF-Token": csrf_cookie},
            json={"scopes": [read_a.id]},
        )
        assert response.status_code == 200

        # scope read_b lives on server 2
        response = client.patch(
            "/roles/1",
            headers={"X-CSRF-Token": csrf_cookie},
            json={"scopes": [read_b.id]},
        )
        assert response.status_code == 400


def test_duplicate_role_name_post(app, auth, client, test_servers):
    # Checks that you can't have two roles with the same name on a server
    with app.app_context():
        response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")

        role_payload = {
            "name": "test_role",
            "scopes": [
                3,
                4,
            ],  # "run" and "dummy_agg_unit:dummy_scope:dummy_scope", server 1
            "server_id": 1,
            "latest_token_expiry": "2020-12-31T12:00:00.0Z",
            "longest_token_life_minutes": 2 * 24 * 60,
        }

        response = client.post(
            "roles/", headers={"X-CSRF-Token": csrf_cookie}, json=role_payload
        )
        assert response.status_code == 200

        response = client.post(
            "roles/", headers={"X-CSRF-Token": csrf_cookie}, json=role_payload
        )
        assert response.status_code == 400  # this the error code?
        assert "Name already exists" in response.text

        role_payload.update({"server_id": 2})
        response = client.post(
            "roles/", headers={"X-CSRF-Token": csrf_cookie}, json=role_payload
        )
        assert response.status_code == 200


def test_duplicate_role_name_patch(app, auth, client, test_servers):
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")

    role_payload = {
        "name": "test_role",
        "scopes": [
            3,
            4,
        ],  # "run" and "dummy_agg_unit:dummy_scope:dummy_scope", server 1
        "server_id": 1,
        "latest_token_expiry": "2020-12-31T12:00:00.0Z",
        "longest_token_life_minutes": 2 * 24 * 60,
    }

    second_role_payload = role_payload.copy()
    second_role_payload["name"] = "test_role_2"
    response = client.post(
        "roles/", headers={"X-CSRF-Token": csrf_cookie}, json=role_payload
    )
    assert response.status_code == 200
    response = client.post(
        "roles/", headers={"X-CSRF-Token": csrf_cookie}, json=second_role_payload
    )
    assert response.status_code == 200
    # TODO: Fix the bug this exposes in scope_server
    # response = client.patch(
    #     "/roles/2",
    #     headers={"X-CSRF-Token":csrf_cookie},
    #     json = {"scopes":[3,4,5]}
    # )
    # assert response.status_code == 200
    response = client.patch(
        "/roles/2",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"name": "test_role"},
    )
    assert response.status_code == 400  # this the error code?
    assert "Name already exists" in response.text

    response = client.patch(
        "/roles/2",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"name": "test_role_no_dupe"},
    )
    assert response.status_code == 200


def test_invalid_payload_patch(app, auth, client, test_servers):
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")

    role_payload = {
        "name": "test_role",
        "scopes": [
            3,
            4,
        ],  # "run" and "dummy_agg_unit:dummy_scope:dummy_scope", server 1
        "server_id": 1,
        "latest_token_expiry": "2020-12-31T12:00:00.0Z",
        "longest_token_life_minutes": 2 * 24 * 60,
        "not_a_real_heading": 9999999,
    }
    response = client.post(
        "roles/", headers={"X-CSRF-Token": csrf_cookie}, json=role_payload
    )
    assert response.status_code == 200
    assert "not_a_real_heading" not in response.json.keys()
