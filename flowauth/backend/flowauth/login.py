# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flask import request, jsonify, Blueprint, current_app, session
from flask_login import login_user, logout_user, login_required, current_user
from flask_principal import identity_changed, Identity, AnonymousIdentity
from werkzeug.exceptions import abort

from .models import *
from .invalid_usage import InvalidUsage, Unauthorized


blueprint = Blueprint(__name__, __name__)


@blueprint.route("/signin", methods=["POST"])
def signin():
    json = request.get_json()
    if "username" not in json or "password" not in json:
        raise InvalidUsage("Must supply username or password.")
    user = User.query.filter(User.username == json["username"]).first()
    if user is not None:
        current_app.logger.debug(f"{user.username}:{user.id} trying to log in.")
        if user.is_correct_password(json["password"]):
            login_user(user, remember=False)
            identity_changed.send(
                current_app._get_current_object(), identity=Identity(user.id)
            )
            session.modified = True
            return jsonify({"logged_in": True, "is_admin": user.is_admin})
    current_app.logger.debug(f"{json['username']} failed to log in.")
    raise Unauthorized("Incorrect username or password.")


@blueprint.route("/is_signed_in")
@login_required
def is_signed_in():
    return jsonify({"logged_in": True, "is_admin": current_user.is_admin})


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
