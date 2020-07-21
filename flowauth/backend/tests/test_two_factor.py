# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from itsdangerous import TimestampSigner
import pytest
from flowauth.invalid_usage import Unauthorized
from flowauth.models import User, db


def test_two_factor_enabled_but_not_confirmed(client, auth, test_user):
    """Test that enabling two factor doesn't enable it."""
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
        assert "backup_codes" in json
        assert "backup_codes_signature" in json
        db_user = User.query.filter(User.id == uid).first()
        assert db_user.two_factor_auth is None


def test_two_factor_confirmed(app, client, auth, test_user, get_two_factor_code):
    """Test that confirming two factor enables it."""
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
        secret = (
            TimestampSigner(app.config["SECRET_KEY"])
            .unsign(json["secret"], max_age=86400)
            .decode()
        )
        json["two_factor_code"] = get_two_factor_code(secret)
        response = client.post(
            "/user/confirm_two_factor", headers={"X-CSRF-Token": csrf_cookie}, json=json
        )
        assert response.status_code == 200
        assert response.json == {"two_factor_enabled": True}
        assert (
            User.query.filter(User.id == uid)
            .first()
            .two_factor_auth.decrypted_secret_key
            == secret
        )


@pytest.mark.parametrize(
    "data, expected_status, expected_message",
    [
        ({}, 400, "Must supply a two-factor authentication code."),
        (
            {"two_factor_code": "", "backup_codes_signature": "BAD_SIGNATURE"},
            400,
            "Must supply a two-factor authentication secret.",
        ),
        (
            {"secret": "", "two_factor_code": ""},
            400,
            "Must supply signed backup codes.",
        ),
        (
            {
                "secret": "",
                "two_factor_code": "",
                "backup_codes_signature": "BAD_SIGNATURE",
            },
            401,
            "Two-factor setup attempt has been tampered with.",
        ),
    ],
)
def test_confirm_errors(
    data, expected_status, expected_message, client, auth, test_two_factor_auth_user
):
    """Test expected error statuses come back when bad data is sent to confirm code reset.."""
    uid, username, password, otp_generator, backup_codes = test_two_factor_auth_user
    otp_code = otp_generator()
    # Log in once
    response, csrf_cookie = auth.two_factor_login(
        username=username, password=password, otp_code=otp_code
    )
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    with client:

        response = client.post(
            "/user/confirm_two_factor", headers={"X-CSRF-Token": csrf_cookie}, json=data
        )
        assert response.status_code == expected_status
        assert response.json["message"] == expected_message


def test_backup_code_generation(client, auth, test_two_factor_auth_user):
    """Test that we can generate backup codes."""
    uid, username, password, otp_generator, backup_codes = test_two_factor_auth_user
    otp_code = otp_generator()
    # Log in once
    response, csrf_cookie = auth.two_factor_login(
        username=username, password=password, otp_code=otp_code
    )
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    with client:
        response = client.get(
            "/user/generate_two_factor_backups", headers={"X-CSRF-Token": csrf_cookie}
        )
        assert response.status_code == 200
        assert len(response.json["backup_codes"]) == 16
        assert len(response.json["backup_codes"][0]) == 10
        assert "backup_codes_signature" in response.json
        new_backup_codes = response.json["backup_codes"]
        # Backup codes should not yet be reset
        assert (
            User.query.filter(User.id == uid)
            .first()
            .two_factor_auth.validate_backup_code(backup_codes[0])
        )
        response = client.post(
            "/user/generate_two_factor_backups",
            headers={"X-CSRF-Token": csrf_cookie},
            json=response.json,
        )
        assert response.status_code == 200
        # Original backup codes should no longer be valid
        with pytest.raises(Unauthorized):
            User.query.filter(
                User.id == uid
            ).first().two_factor_auth.validate_backup_code(backup_codes[2])
        assert (
            User.query.filter(User.id == uid)
            .first()
            .two_factor_auth.validate_backup_code(new_backup_codes[0])
        )


@pytest.mark.parametrize(
    "data, expected_status",
    [({}, 400), ({"backup_codes_signature": "BAD_SIGNATURE"}, 401)],
)
def test_backup_code_confirm_errors(
    data, expected_status, client, auth, test_two_factor_auth_user
):
    """Test expected error statuses come back when bad data is sent to confirm code reset.."""
    uid, username, password, otp_generator, backup_codes = test_two_factor_auth_user
    otp_code = otp_generator()
    # Log in once
    response, csrf_cookie = auth.two_factor_login(
        username=username, password=password, otp_code=otp_code
    )
    # Log in first
    response, csrf_cookie = auth.login(username, password)
    with client:

        response = client.post(
            "/user/generate_two_factor_backups",
            headers={"X-CSRF-Token": csrf_cookie},
            json=data,
        )
        assert response.status_code == expected_status


def test_two_factor_login(client, auth, test_two_factor_auth_user):
    """Test that we can log in with two factor.."""
    uid, username, password, otp_generator, backup_codes = test_two_factor_auth_user
    otp_code = otp_generator()
    # Log in once
    response, csrf_cookie = auth.two_factor_login(
        username=username, password=password, otp_code=otp_code
    )
    assert response.status_code == 200
    assert response.json == {
        "logged_in": True,
        "is_admin": False,
        "require_two_factor_setup": False,
    }


def test_two_factor_login_no_reuse(client, auth, test_two_factor_auth_user):
    """Test that we can't log in twice with the same code."""
    uid, username, password, otp_generator, backup_codes = test_two_factor_auth_user
    otp_code = otp_generator()
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
    """Test that we can't log in without a code when 2fa is enabled."""
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
        username=username, password=password, otp_code=otp_generator()
    )
    auth.logout()
    # Log in again with the original backup code
    response, csrf_cookie = auth.two_factor_login(
        username=username, password=password, otp_code=backup_codes[0]
    )
    assert response.status_code == 401
    assert response.json["message"] == "Code not valid."


def test_disable_two_factor_login(client, auth, test_two_factor_auth_user):
    """Test that we can disable two-factor login."""
    uid, username, password, otp_generator, backup_codes = test_two_factor_auth_user

    # Log in once with a backup code
    response, csrf_cookie = auth.two_factor_login(
        username=username, password=password, otp_code=otp_generator()
    )
    assert response.status_code == 200
    with client:
        client.post("/user/disable_two_factor", headers={"X-CSRF-Token": csrf_cookie})
    auth.logout()
    # Log in again without a two factor code
    response, csrf_cookie = auth.login(username=username, password=password)

    assert response.status_code == 200
    assert response.json["logged_in"]


def test_two_factor_status(client, auth, test_two_factor_auth_user):
    """Test two factor status accurately reported;."""
    uid, username, password, otp_generator, backup_codes = test_two_factor_auth_user

    # Log in once with a backup code
    response, csrf_cookie = auth.two_factor_login(
        username=username, password=password, otp_code=otp_generator()
    )
    with client:
        response = client.get(
            "/user/two_factor_active", headers={"X-CSRF-Token": csrf_cookie}
        )
        assert response.json["two_factor_enabled"]


def test_two_factor_require(client, auth, test_two_factor_auth_user):
    """Test two factor is not required unless set as such."""
    uid, username, password, otp_generator, backup_codes = test_two_factor_auth_user

    # Log in once with a backup code
    response, csrf_cookie = auth.two_factor_login(
        username=username, password=password, otp_code=otp_generator()
    )
    with client:
        response = client.get(
            "/user/two_factor_required", headers={"X-CSRF-Token": csrf_cookie}
        )
        assert not response.json["require_two_factor"]
        user = User.query.filter(User.id == uid).first()
        user.require_two_factor = True
        db.session.add(user)
        db.session.commit()
        response = client.get(
            "/user/two_factor_required", headers={"X-CSRF-Token": csrf_cookie}
        )
        assert response.json["require_two_factor"]
