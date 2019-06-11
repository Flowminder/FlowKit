# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import pyotp
from flask import jsonify, Blueprint, request
from flask_login import login_required, current_user

from .models import *
from .invalid_usage import InvalidUsage
from zxcvbn import zxcvbn

blueprint = Blueprint(__name__, __name__)


@blueprint.route("/password", methods=["PATCH"])
@login_required
def set_password():
    """
    Set a new password for the logged in user..

    Notes
    -----
    Expects json containing 'password' and 'newPassword' keys.
    Checks the password is the same as the existing one and that
    the new password is strong.
    """
    edits = request.get_json()
    current_app.logger.debug(f"User {current_user.username} tried to change password.")
    try:
        old_pass = edits["password"]
    except KeyError:
        raise InvalidUsage("Missing old password.", payload={"bad_field": "password"})
    try:
        new_pass = edits["newPassword"]
    except KeyError:
        raise InvalidUsage(
            "Missing new password.", payload={"bad_field": "newPassword"}
        )

    if current_user.is_correct_password(old_pass):
        if len(new_pass) == 0 or zxcvbn(new_pass)["score"] < 4:
            raise InvalidUsage(
                "Password not complex enough.", payload={"bad_field": "newPassword"}
            )
        current_user.password = new_pass
        db.session.add(current_user)
        db.session.commit()
        current_app.logger.debug(f"User {current_user.username} password changed.")
        return jsonify({}), 200
    else:

        raise InvalidUsage("Password incorrect.", payload={"bad_field": "password"})


@blueprint.route("/enable_two_factor", methods=["POST"])
@login_required
def enable_two_factor():
    """
    Switch two factor auth on for the currently logged in user.
    """
    secret = pyotp.random_base32()
    provisioning_url = pyotp.totp.TOTP(secret).provisioning_uri(
        current_user.username, issuer_name="FlowAuth"
    )
    old_auth = current_user.two_factor_auth
    if old_auth is not None:
        db.session.delete(old_auth)
    auth = TwoFactorAuth(user=current_user)
    auth.secret_key = secret
    db.session.add(auth)
    db.session.commit()
    return (
        jsonify(
            {
                "provisioning_url": provisioning_url,
                "secret": secret,
                "issuer": "FlowAuth",
            }
        ),
        200,
    )


@blueprint.route("/confirm_two_factor", methods=["POST"])
@login_required
def confirm_two_factor():
    json = request.get_json()
    if "two_factor_code" not in json:
        raise InvalidUsage("Must supply a two-factor authentication code.")
    code = json["two_factor_code"]

    if current_user.two_factor_auth is None:
        raise InvalidUsage("Two factor must be enabled first.")
    if len(current_user.two_factor_auth.two_factor_backups) == 0:
        raise InvalidUsage("Must view backup codes first.")
    current_user.two_factor_auth.validate(code)
    current_user.two_factor_auth.enabled = True
    db.session.add(current_user.two_factor_auth)
    db.session.commit()
    return jsonify({"two_factor_enabled": True}), 200


@blueprint.route("/disable_two_factor", methods=["POST"])
@login_required
def disable_two_factor():
    """
    Switch two factor auth off for the currently logged in user.
    """
    db.session.delete(current_user.two_factor_auth)
    db.session.commit()
    return jsonify({"two_factor_enabled": False}), 200


@blueprint.route("/generate_two_factor_backups", methods=["POST"])
@login_required
def reset_backup_codes():
    """
    Generate a new list of two-factor auth backup codes for the currently logged in user.
    """
    new_codes = TwoFactorBackup.generate(current_user.id)
    return jsonify(new_codes), 200


@blueprint.route("/two_factor_required")
@login_required
def is_two_factor_required():
    """
    Check if (for this user), two factor authentication is required.
    """
    return jsonify({"require_two_factor": current_user.require_two_factor})


@blueprint.route("/two_factor_active")
@login_required
def is_two_factor_active():
    """
    Check if (for this user), two factor authentication is activated.
    """
    return jsonify(
        {
            "two_factor_enabled": current_user.two_factor_auth
            and current_user.two_factor_auth.enabled
        }
    )
