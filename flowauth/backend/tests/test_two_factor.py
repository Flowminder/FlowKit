# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import pyotp
from flowauth import User, db, LastUsedTwoFactorCode


def test_two_factor_enabled_but_not_confirmed(client, auth, test_user):
    """Test that enabling two factor creates it, but doesn't confirm it."""
    uid, username, password = test_user
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    with client:
        client.get("/")
        db_user = User.query.filter(User.id == uid).first()
        assert db_user.two_factor_auth is None
        response = client.post(
            "/user/enable_two_factor", headers={"X-CSRF-Token": csrf_cookie}
        )
        assert 200 == response.status_code  # Should get an error

        json = response.get_json()
        assert "provisioning_url" in json
        assert "secret" in json
        assert "issuer" in json
        db_user = User.query.filter(User.id == uid).first()
        assert db_user.two_factor_auth is not None
        assert not db_user.two_factor_auth.enabled


def test_two_factor_confirm_requires_codes(client, auth, test_user):
    """Test that we can't confirm two factor without backup codes being generated."""
    uid, username, password = test_user
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    with client:
        client.get("/")
        db_user = User.query.filter(User.id == uid).first()
        assert db_user.two_factor_auth is None
        response = client.post(
            "/user/enable_two_factor", headers={"X-CSRF-Token": csrf_cookie}
        )
        assert 200 == response.status_code  # Should get an error

        json = response.get_json()
        assert "secret" in json
        assert "issuer" in json
        db_user = User.query.filter(User.id == uid).first()
        assert db_user.two_factor_auth is not None
        assert not db_user.two_factor_auth.enabled
        response = client.post(
            "/user/confirm_two_factor",
            headers={"X-CSRF-Token": csrf_cookie},
            json={"two_factor_code": 0},
        )
        assert response.status_code == 400
        assert response.json["message"] == "Must view backup codes first."


def test_backup_code_generation(client, auth, test_user):
    """Test that we can't confirm two factor without backup codes being generated."""
    uid, username, password = test_user
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    with client:
        client.get("/")
        db_user = User.query.filter(User.id == uid).first()
        assert db_user.two_factor_auth is None
        response = client.post(
            "/user/enable_two_factor", headers={"X-CSRF-Token": csrf_cookie}
        )
        assert 200 == response.status_code  # Should get an error

        json = response.get_json()
        assert "secret" in json
        assert "issuer" in json
        db_user = User.query.filter(User.id == uid).first()
        assert db_user.two_factor_auth is not None
        assert not db_user.two_factor_auth.enabled
        response = client.post(
            "/user/generate_two_factor_backups", headers={"X-CSRF-Token": csrf_cookie}
        )
        assert response.status_code == 200
        assert len(response.json) > 0
        assert isinstance(response.json[0], str)


def test_two_factor_login(client, auth, test_user):
    """Test that we can log in with two factor.."""
    uid, username, password = test_user
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    with client:
        client.get("/")
        db_user = User.query.filter(User.id == uid).first()
        assert db_user.two_factor_auth is None
        response = client.post(
            "/user/enable_two_factor", headers={"X-CSRF-Token": csrf_cookie}
        )
        assert 200 == response.status_code  # Should get an error

        json = response.get_json()
        assert "secret" in json
        assert "issuer" in json
        secret = json["secret"]
        db_user = User.query.filter(User.id == uid).first()
        assert db_user.two_factor_auth is not None
        assert not db_user.two_factor_auth.enabled
        response = client.post(
            "/user/generate_two_factor_backups", headers={"X-CSRF-Token": csrf_cookie}
        )
        backup_codes = response.json
        response = client.post(
            "/user/confirm_two_factor",
            headers={"X-CSRF-Token": csrf_cookie},
            json={"two_factor_code": pyotp.TOTP(secret).now()},
        )
        assert response.json["two_factor_enabled"]
        # Remove the code we just used because it will no longer be valid and we don't
        # want to wait for a new one
        db.session.delete(LastUsedTwoFactorCode.query.all()[0])
        db.session.commit()
        auth.logout()
        response = client.post(
            "signin",
            json={
                "username": username,
                "password": password,
                "two_factor_code": pyotp.TOTP(secret).now(),
            },
        )
        assert response.status_code == 200
        assert response.json == {"logged_in": True, "is_admin": False}


def test_two_factor_login_no_reuse(client, auth, test_two_factor_auth_user):
    """Test that we can't log in twice with the same code."""
    uid, username, password, otp_generator, backup_codes = test_two_factor_auth_user
    otp_code = otp_generator.now()
    # Log in once
    response, csrf_cookie = auth.two_factor_login(
        username=username, password=password, otp_code=otp_code
    )
    assert response.status_code == 200
    auth.logout()
    response, csrf_cookie = auth.two_factor_login(
        username=username, password=password, otp_code=otp_code
    )
    assert response.status_code == 401
    assert response.json["message"] == "Code not valid."


def test_two_factor_login_required_when_enabled(
    client, auth, test_two_factor_auth_user
):
    """Test that we can't log in twice with the same backup code."""
    uid, username, password, otp_generator, backup_codes = test_two_factor_auth_user

    # Log in once with a backup code
    response, csrf_cookie = auth.login(username=username, password=password)
    assert response.status_code == 400
    assert response.json["message"] == "Must supply a two-factor authentication code."


def test_two_factor_login_no_backup_reuse(client, auth, test_two_factor_auth_user):
    """Test that we can't log in twice with the same backup code."""
    uid, username, password, otp_generator, backup_codes = test_two_factor_auth_user

    # Log in once with a backup code
    response, csrf_cookie = auth.two_factor_login(
        username=username, password=password, otp_code=backup_codes[0]
    )
    assert response.status_code == 200
    auth.logout()
    # Log in again with a real code to make sure the backup isn't the last used
    response, csrf_cookie = auth.two_factor_login(
        username=username, password=password, otp_code=otp_generator.now()
    )
    auth.logout()
    # Log in again with the original backup code
    response, csrf_cookie = auth.two_factor_login(
        username=username, password=password, otp_code=backup_codes[0]
    )
    assert response.status_code == 401
    assert response.json["message"] == "Invalid backup code."


def test_disable_two_factor_login(client, auth, test_two_factor_auth_user):
    """Test that we can disable two-factor login."""
    uid, username, password, otp_generator, backup_codes = test_two_factor_auth_user

    # Log in once with a backup code
    response, csrf_cookie = auth.two_factor_login(
        username=username, password=password, otp_code=otp_generator.now()
    )
    assert response.status_code == 200
    with client:
        client.post("/user/disable_two_factor", headers={"X-CSRF-Token": csrf_cookie})
    auth.logout()
    # Log in again without a two factor code
    response, csrf_cookie = auth.login(username=username, password=password)

    assert response.status_code == 200
    assert response.json["logged_in"]
