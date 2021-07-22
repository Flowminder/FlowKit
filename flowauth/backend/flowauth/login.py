# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import binascii

from flask import Blueprint, current_app, jsonify, request, session
from werkzeug.exceptions import abort

from flask_login import current_user, login_required, login_user, logout_user
from flask_principal import AnonymousIdentity, Identity, identity_changed

from .invalid_usage import InvalidUsage, Unauthorized
from .models import *

blueprint = Blueprint(__name__.split(".").pop(), __name__)


@blueprint.route("/signin", methods=["POST"])
def signin():
    json = request.get_json()
    if "username" not in json or "password" not in json:
        raise InvalidUsage("Must supply username or password.")
    user = User.query.filter(User.username == json["username"]).first()
    if user is not None:
        current_app.logger.debug(
            "Login attempt", username=user.username, user_id=user.id
        )
        if user.is_correct_password(json["password"]):
            two_factor = user.two_factor_auth
            if two_factor is not None and two_factor.enabled:
                if "two_factor_code" not in json or json["two_factor_code"] == "":
                    raise InvalidUsage(
                        "Must supply a two-factor authentication code.",
                        payload={"need_two_factor": True},
                    )
                try:
                    two_factor.validate(json["two_factor_code"])
                except (Unauthorized, binascii.Error):
                    two_factor.validate_backup_code(json["two_factor_code"])
            login_user(user, remember=False)
            identity_changed.send(
                current_app._get_current_object(), identity=Identity(user.id)
            )
            session.modified = True
            return jsonify(
                {
                    "logged_in": True,
                    "is_admin": user.is_admin,
                    "require_two_factor_setup": current_user.two_factor_setup_required,
                }
            )
    current_app.logger.debug("Failed login attempt", username=json["username"])
    raise Unauthorized("Incorrect username or password.")


@blueprint.route("/is_signed_in")
@login_required
def is_signed_in():
    return jsonify(
        {
            "logged_in": True,
            "is_admin": current_user.is_admin,
            "require_two_factor_setup": current_user.two_factor_setup_required,
        }
    )


@blueprint.route("/signout")
@login_required
def signout():
    logout_user()
    for key in ("identity.name", "identity.auth_type"):
        session.pop(key, None)
    session.modified = True
    # Tell Flask-Principal the user is anonymous
    identity_changed.send(
        current_app._get_current_object(), identity=AnonymousIdentity()
    )
    return jsonify({"logged_in": False})
