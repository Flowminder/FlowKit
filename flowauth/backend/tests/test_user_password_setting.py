# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flask import session


def test_new_password_required(client, auth, test_user):
    """Test that a new password must be supplied."""
    uid, username, password = test_user
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    with client:
        response = client.patch(
            "/user/password",
            json={"password": "DUMMY_PASSWORDS"},
            headers={"X-CSRF-Token": csrf_cookie},
        )
        assert 400 == response.status_code  # Should get an error

        assert {
            "message": "Missing new password.",
            "bad_field": "newPassword",
            "code": 400,
        } == response.get_json()  # Should get a message about why it failed


def test_old_password_required(client, auth, test_user):
    """Test that the existing password must be supplied."""
    uid, username, password = test_user
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    with client:
        response = client.patch(
            "/user/password",
            json={"newPassword": "BAD"},
            headers={"X-CSRF-Token": csrf_cookie},
        )
        assert 400 == response.status_code  # Should get an error

        assert {
            "message": "Missing old password.",
            "bad_field": "password",
            "code": 400,
        } == response.get_json()  # Should get a message about why it failed


def test_incorrect_old_password_rejected(client, auth, test_user):
    """Test that an incorrect old password doesn't permit a reset."""
    uid, username, password = test_user
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    with client:
        response = client.patch(
            "/user/password",
            json={"newPassword": "BAD", "password": "INCORRECT_PASSWORD"},
            headers={"X-CSRF-Token": csrf_cookie},
        )
        assert 400 == response.status_code  # Should get an error

        assert {
            "message": "Password incorrect.",
            "bad_field": "password",
            "code": 400,
        } == response.get_json()  # Should get a message about why it failed


def test_weak_password_rejected(client, auth, test_user):
    """Test that weak passwords give a 400 error."""
    uid, username, password = test_user
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    with client:
        response = client.patch(
            "/user/password",
            json={"newPassword": "BAD", "password": password},
            headers={"X-CSRF-Token": csrf_cookie},
        )
        assert 400 == response.status_code  # Should get an error

        assert {
            "message": "Password not complex enough.",
            "bad_field": "newPassword",
            "code": 400,
        } == response.get_json()  # Should get a message about why it failed


def test_password_reset(client, auth, test_user):
    """Test that password can be reset."""
    new_password = "THIS_IS_ACTUALLY_QUITE_A_STRONG_DUMMY_PASSWORD"

    uid, username, password = test_user
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    with client:
        response = client.patch(
            "/user/password",
            json={"newPassword": new_password, "password": password},
            headers={"X-CSRF-Token": csrf_cookie},
        )
        assert 200 == response.status_code  # Should get an OK

        assert {} == response.get_json()  # Should get nothing back

        assert "user_id" not in session  # Should be logged out

    response, _ = auth.login(
        username, new_password
    )  # Should be able to log with new password
    assert {"logged_in": True, "is_admin": False} == response.get_json()
