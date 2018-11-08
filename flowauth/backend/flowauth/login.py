# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flask import request, jsonify, Blueprint, current_app, session
from flask_login import login_user, logout_user, login_required
from flask_principal import identity_changed, Identity, AnonymousIdentity
from werkzeug.exceptions import abort

from .models import *
from .invalid_usage import InvalidUsage


blueprint = Blueprint(__name__, __name__)


@blueprint.route("/signin", methods=["POST"])
def signin():
    json = request.get_json()
    if "username" not in json or "password" not in json:
        raise InvalidUsage("Must supply username or password.")
    user = User.query.filter(User.username == json["username"]).first()
    if user is not None:
        if user.is_correct_password(json["password"]):
            login_user(user)
            identity_changed.send(
                current_app._get_current_object(), identity=Identity(user.id)
            )
            return jsonify({"logged_in": True, "is_admin": user.is_admin})
    abort(401, "Incorrect username or password.")


@blueprint.route("/signout")
@login_required
def signout():
    logout_user()
    for key in ("identity.name", "identity.auth_type"):
        session.pop(key, None)

        # Tell Flask-Principal the user is anonymous
    identity_changed.send(
        current_app._get_current_object(), identity=AnonymousIdentity()
    )
    return jsonify({"logged_in": False})
