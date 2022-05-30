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
        "has_two_factor": False,
        "require_two_factor": False,
        "roles": [],
    } == response.get_json()


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


def test_edit_user_enforces_password_strength(client, auth, test_admin):
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


def test_remove_two_factor_regression(client, auth, test_admin):
    """
    Regression test for #1374 - check that removing two factor from a user with no two factor doesn't error.
    """
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
        json={"has_two_factor": False},
    )
    assert 200 == response.status_code

    response = client.get("/admin/users/2", headers={"X-CSRF-Token": csrf_cookie})

    assert {
        "id": 2,
        "name": "TEST_USER",
        "is_admin": False,
        "has_two_factor": False,
        "require_two_factor": False,
        "roles": [],
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
        "has_two_factor": False,
        "require_two_factor": False,
        "roles": [],
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
    assert {
        "code": 400,
        "message": "Password not complex enough.",
        "bad_field": "password",
    } == response.get_json()
    response = client.get("/admin/users/2", headers={"X-CSRF-Token": csrf_cookie})

    assert 404 == response.status_code


def test_create_user_fail_with_empty_password(client, auth, test_admin):
    """
    Test that an appropriate error response is sent if password is an empty string
    when trying to create a user.
    """
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    response = client.post(
        "/admin/users",
        headers={"X-CSRF-Token": csrf_cookie},
        json={"username": "TEST_USER", "password": ""},
    )
    assert 400 == response.status_code  # Should get an OK
    assert {
        "code": 400,
        "message": "Password must be provided.",
        "bad_field": "password",
    } == response.get_json()


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
        "has_two_factor": False,
        "require_two_factor": False,
        "roles": [],
    } == response.get_json()

    # Delete user
    response = client.delete("/admin/users/2", headers={"X-CSRF-Token": csrf_cookie})
    # Check user does not exist
    response = client.get("/admin/users/2", headers={"X-CSRF-Token": csrf_cookie})
    assert 404 == response.status_code


def test_re_add_deleted_user(client, auth, test_admin):
    """
    Regression test for https://github.com/Flowminder/FlowKit/issues/1638
    Ensure that a user can be added with the same name as a previously-deleted user.
    """
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
        "has_two_factor": False,
        "require_two_factor": False,
        "roles": [],
    } == response.get_json()

    # Delete the user
    response = client.delete("/admin/users/2", headers={"X-CSRF-Token": csrf_cookie})
    response = client.get("/admin/users/2", headers={"X-CSRF-Token": csrf_cookie})
    assert 404 == response.status_code

    response = client.post(
        "/admin/users",
        headers={"X-CSRF-Token": csrf_cookie},
        json={
            "username": "TEST_USER",
            "password": "A_VERY_STRONG_DUMMY_PASSWORD_THAT_IS_VERY_LONG",
        },
    )  # Make a new user with the same name
    assert 200 == response.status_code  # Should get an OK
    response = client.get("/admin/users/2", headers={"X-CSRF-Token": csrf_cookie})

    assert {
        "id": 2,
        "name": "TEST_USER",
        "is_admin": False,
        "has_two_factor": False,
        "require_two_factor": False,
        "roles": [],
    } == response.get_json()


def test_list_users(client, auth, test_admin):
    uid, username, password = test_admin
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    response = client.get("/admin/users", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code  # Should get an OK
    assert [{"id": 1, "name": "TEST_ADMIN"}] == response.get_json()
