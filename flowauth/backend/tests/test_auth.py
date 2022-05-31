# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from flask import g, session


def test_login(app, client, auth, test_user):
    with app.app_context():
        """Test that we can log in by posting a username and password as json"""
        # test that signin route doesn't accept a get
        assert client.get("/signin").status_code == 405
        uid, username, password = test_user
        # test that successful login redirects to the index page
        response, _ = auth.login(username, password)
        assert {
            "logged_in": True,
            "is_admin": False,
            "require_two_factor_setup": False,
        } == response.get_json()

        # login request set the user_id in the session
        # check that the user is loaded from the session
        with client:
            client.get("/")
            assert session["identity.id"] == uid
            assert g.user.username == username


@pytest.mark.parametrize(
    ("username", "password"), (("a", "test"), ("test", "a"), ("", ""))
)
def test_login_validate_input(auth, username, password):
    """Test that bad username/password combos don't allow login."""
    response, _ = auth.login(username, password)
    assert b"Incorrect username or password." in response.data


def test_is_logged_in(client, auth, test_user):
    """Test is_logged_in route returns True and correct 'is_admin'."""
    uid, username, password = test_user
    auth.login(username, password)

    with client:
        response = client.get("/is_signed_in")
        assert {"logged_in": True, "is_admin": False} == response.get_json()


def test_is_logged_in(client):
    """Test is_logged_in route returns 401 when not logged in."""
    with client:
        assert client.get("/is_signed_in").status_code == 401


def test_logout(app, client, auth, test_user):
    with app.app_context():
        """Test that we can log out"""
        uid, username, password = test_user
        auth.login(username, password)

        with client:
            auth.logout()
            assert "user_id" not in session
