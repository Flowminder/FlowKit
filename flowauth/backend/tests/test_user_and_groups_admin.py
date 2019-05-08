# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


def test_create_user(client, auth, test_admin):
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    response = client.post(
        "/admin/users",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "username": "TEST_USER",
            "password": "A_VERY_STRONG_DUMMY_PASSWORD_THAT_IS_VERY_LONG",
        },
    )
    assert 200 == response.status_code  # Should get an OK
    response = client.get("/admin/users/2", headers={"X-CSRF-Token": csrf_cookie})

    assert {
        "id": 2,
        "name": "TEST_USER",
        "is_admin": False,
        "groups": [{"id": 2, "name": "TEST_USER"}],
        "servers": [],
        "group_id": 2,
    } == response.get_json()


def test_get_users_group(client, auth, test_admin):
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)

    response = client.get(
        "/admin/users/1/user_group", headers={"X-CSRF-Token": csrf_cookie}
    )
    assert 200 == response.status_code
    assert {"id": 1, "name": username} == response.get_json()


def test_edit_user_protects_last_admin(client, auth, test_admin):
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)

    response = client.patch(
        "/admin/users/1",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"is_admin": False},
    )
    assert 400 == response.status_code
    assert {
        "code": 400,
        "message": "Removing this user's admin rights would leave no admins.",
        "bad_field": "is_admin",
    } == response.get_json()


def test_edit_user_requires_nonzero_length_username(client, auth, test_admin):
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)

    response = client.patch(
        "/admin/users/1", headers={"X-CSRF-Token": csrf_cookie}, json={"username": ""}
    )
    assert 400 == response.status_code
    assert {
        "code": 400,
        "message": "Username too short.",
        "bad_field": "username",
    } == response.get_json()


def test_edit_user_requires_enforces_passwword_strength(client, auth, test_admin):
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)

    response = client.patch(
        "/admin/users/1", headers={"X-CSRF-Token": csrf_cookie}, json={"password": "X"}
    )
    assert 400 == response.status_code
    assert {
        "code": 400,
        "message": "Password not complex enough.",
        "bad_field": "password",
    } == response.get_json()


def test_edit_user(client, auth, test_admin):
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    response = client.post(
        "/admin/users",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "username": "TEST_USER",
            "password": "A_VERY_STRONG_DUMMY_PASSWORD_THAT_IS_VERY_LONG",
        },
    )
    assert 200 == response.status_code

    response = client.patch(
        "/admin/users/2",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "username": "TEST_USER_B",
            "password": "A_DIFFERENT_VERY_STRONG_DUMMY_PASSWORD_THAT_IS_VERY_LONG",
            "is_admin": True,
        },
    )

    response = client.get("/admin/users/2", headers={"X-CSRF-Token": csrf_cookie})

    assert {
        "id": 2,
        "name": "TEST_USER_B",
        "is_admin": True,
        "groups": [{"id": 2, "name": "TEST_USER"}],
        "servers": [],
        "group_id": 2,
    } == response.get_json()


def test_create_user_fail_with_short_password(client, auth, test_admin):
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    response = client.post(
        "/admin/users",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"username": "TEST_USER", "password": "A_"},
    )
    assert 400 == response.status_code  # Should get an OK
    assert {"message": "bad_pass", "code": 400}
    response = client.get("/admin/users/2", headers={"X-CSRF-Token": csrf_cookie})

    assert 404 == response.status_code


def test_create_user_fail_with_same_username(client, auth, test_admin):
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    response = client.post(
        "/admin/users",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "username": "TEST_USER",
            "password": "A_VERY_STRONG_DUMMY_PASSWORD_THAT_IS_VERY_LONG",
        },
    )  # Make a user first

    assert 200 == response.status_code  # Should get an OK
    response = client.post(
        "/admin/users",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "username": "TEST_USER",
            "password": "A_DIFFERENT_STRONG_DUMMY_PASSWORD_THAT_IS_VERY_LONG",
        },
    )  # Try to make the user again
    assert 400 == response.status_code
    assert {
        "code": 400,
        "message": "Username already exists.",
        "bad_field": "username",
    } == response.get_json()


def test_cannot_delete_only_admin(client, auth, test_admin):
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)

    response = client.delete("/admin/users/1", headers={"X-CSRF-Token": csrf_cookie})
    assert 400 == response.status_code
    response = client.get("/admin/users/1", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code


def test_delete_user(client, auth, test_admin):
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    response = client.post(
        "/admin/users",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "username": "TEST_USER",
            "password": "A_VERY_STRONG_DUMMY_PASSWORD_THAT_IS_VERY_LONG",
        },
    )  # Make a user first
    assert 200 == response.status_code  # Should get an OK
    response = client.get("/admin/users/2", headers={"X-CSRF-Token": csrf_cookie})

    assert {
        "id": 2,
        "name": "TEST_USER",
        "is_admin": False,
        "groups": [{"id": 2, "name": "TEST_USER"}],
        "servers": [],
        "group_id": 2,
    } == response.get_json()

    response = client.delete("/admin/users/2", headers={"X-CSRF-Token": csrf_cookie})
    response = client.get("/admin/users/2", headers={"X-CSRF-Token": csrf_cookie})
    assert 404 == response.status_code


def test_list_users(client, auth, test_admin):
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    response = client.get("/admin/users", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code  # Should get an OK
    assert [{"id": 1, "name": "TEST_ADMIN"}] == response.get_json()


def test_list_groups(client, auth, test_admin):
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    response = client.get("/admin/groups", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code  # Should get an OK
    assert (
        [] == response.get_json()
    )  # Should be empty because only group is the admin's user group


def test_create_group(client, auth, test_admin):
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    response = client.post(
        "/admin/groups",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"name": "TEST_GROUP"},
    )
    assert 200 == response.status_code  # Should get an OK
    response = client.get("/admin/groups/2", headers={"X-CSRF-Token": csrf_cookie})

    assert {
        "id": 2,
        "name": "TEST_GROUP",
        "members": [],
        "servers": [],
    } == response.get_json()


def test_create_group_fail_with_same_name(client, auth, test_admin):
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    response = client.post(
        "/admin/groups",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"name": "TEST_GROUP"},
    )
    assert 200 == response.status_code  # Should get an OK
    response = client.get("/admin/groups/2", headers={"X-CSRF-Token": csrf_cookie})

    response = client.post(
        "/admin/groups",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"name": "TEST_GROUP"},
    )
    assert 400 == response.status_code
    assert {
        "code": 400,
        "message": "Group name already exists.",
        "bad_field": "groupname",
    } == response.get_json()


def test_edit_group(client, auth, test_admin):
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    response = client.post(
        "/admin/groups",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"name": "TEST_GROUP"},
    )
    assert 200 == response.status_code  # Should get an OK

    response = client.patch(
        "/admin/groups/2",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"name": "TEST_GROUP_X"},
    )
    assert response.status_code == 200
    assert {"id": 2, "name": "TEST_GROUP_X"} == response.get_json()
    response = client.get("/admin/groups/2", headers={"X-CSRF-Token": csrf_cookie})
    assert {
        "id": 2,
        "name": "TEST_GROUP_X",
        "members": [],
        "servers": [],
    } == response.get_json()


def test_remove_group(client, auth, test_admin):
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    response = client.post(
        "/admin/groups",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"name": "TEST_GROUP"},
    )
    assert 200 == response.status_code  # Should get an OK
    response = client.get("/admin/groups/2", headers={"X-CSRF-Token": csrf_cookie})

    assert {
        "id": 2,
        "name": "TEST_GROUP",
        "members": [],
        "servers": [],
    } == response.get_json()

    response = client.delete("/admin/groups/2", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code
    response = client.get("/admin/groups/2", headers={"X-CSRF-Token": csrf_cookie})
    assert 404 == response.status_code


def test_add_group_member(client, auth, test_admin):
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    client.post(
        "/admin/groups",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"name": "TEST_GROUP"},
    )
    response = client.patch(
        "/admin/groups/2/members",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"members": [{"id": 1}]},
    )
    assert 200 == response.status_code
    assert {"id": 2, "name": "TEST_GROUP"} == response.get_json()
    response = client.get(
        "/admin/groups/2/members", headers={"X-CSRF-Token": csrf_cookie}
    )  # Group should show them as a member
    assert 200 == response.status_code
    assert [{"id": 1, "name": username}] == response.get_json()
    response = client.get("/admin/users/1/groups")  # They should show they're a member
    assert 200 == response.status_code
    assert [{"id": 2, "name": "TEST_GROUP"}] == response.get_json()


def test_remove_group_member(client, auth, test_admin):
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    client.post(
        "/admin/groups",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"name": "TEST_GROUP"},
    )
    client.patch(
        "/admin/groups/2/members",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"members": [{"id": 1}]},
    )
    client.patch(
        "/admin/groups/2/members",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"members": []},
    )
    response = client.get(
        "/admin/groups/2/members", headers={"X-CSRF-Token": csrf_cookie}
    )
    assert 200 == response.status_code
    assert [] == response.get_json()
    response = client.get("/admin/users/1/groups")  # They should show they're a member
    assert 200 == response.status_code
    assert [] == response.get_json()


def test_join_group(client, auth, test_admin):
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    client.post(
        "/admin/groups",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"name": "TEST_GROUP"},
    )
    response = client.patch(
        "/admin/users/1/groups",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"groups": [{"id": 2}]},
    )
    assert 200 == response.status_code
    assert [{"id": 2, "name": "TEST_GROUP"}] == response.get_json()
    response = client.get(
        "/admin/groups/2/members", headers={"X-CSRF-Token": csrf_cookie}
    )  # Group should show them as a member
    assert 200 == response.status_code
    assert [{"id": 1, "name": username}] == response.get_json()
    response = client.get("/admin/users/1/groups")  # They should show they're a member
    assert 200 == response.status_code
    assert [{"id": 2, "name": "TEST_GROUP"}] == response.get_json()


def test_remove_leave_group(client, auth, test_admin):
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    client.post(
        "/admin/groups",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"name": "TEST_GROUP"},
    )
    response = client.patch(
        "/admin/users/1/groups",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"groups": [{"id": 2}]},
    )
    response = client.patch(
        "/admin/users/1/groups",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"groups": []},
    )
    response = client.get(
        "/admin/groups/2/members", headers={"X-CSRF-Token": csrf_cookie}
    )
    assert 200 == response.status_code
    assert [] == response.get_json()
    response = client.get("/admin/users/1/groups")  # They should show they're a member
    assert 200 == response.status_code
    assert [] == response.get_json()
