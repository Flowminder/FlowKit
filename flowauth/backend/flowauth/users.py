# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flask import Blueprint, jsonify, request

from flask_login import login_required
from flask_principal import Permission, RoleNeed
from zxcvbn import zxcvbn

from .invalid_usage import InvalidUsage
from .models import *

blueprint = Blueprint(__name__.split(".").pop(), __name__)
admin_permission = Permission(RoleNeed("admin"))


@blueprint.route("/users")
@login_required
@admin_permission.require(http_exception=401)
def list_all_users():
    """Get a list of all the users."""
    return jsonify(
        [{"id": user.id, "name": user.username} for user in User.query.all()]
    )


@blueprint.route("/users", methods=["POST"])
@login_required
@admin_permission.require(http_exception=401)
def add_user():
    """
    Create a new user.

    Notes
    -----
    Expects json of the form {"username":<username>, "password":<password>, "is_admin":<bool>}.
    Passwords will be tested for strength, and usernames must be unique.
    Returns the new user's id and group_id.
    """
    json = request.get_json()
    try:
        if zxcvbn(json["password"])["score"] > 3:
            user = User(**json)
        else:
            raise InvalidUsage(
                "Password not complex enough.", payload={"bad_field": "password"}
            )
    except (KeyError, IndexError):
        raise InvalidUsage(
            "Password must be provided.", payload={"bad_field": "password"}
        )

    if User.query.filter(User.username == json["username"]).first() is not None:
        raise InvalidUsage(
            "Username already exists.", payload={"bad_field": "username"}
        )
    else:
        db.session.add(user)
        db.session.commit()
        return jsonify({"id": user.id})


@blueprint.route("/users/<user_id>", methods=["DELETE"])
@login_required
@admin_permission.require(http_exception=401)
def rm_user(user_id):
    """
    Delete a user if they exist.

    Parameters
    ----------
    user_id : int

    Notes
    -----
    You cannot delete the _final_ admin user.

    """
    user = User.query.filter(User.id == user_id).first_or_404()
    if user.is_admin and len(User.query.filter(User.is_admin).all()) == 1:
        raise InvalidUsage("Removing this user would leave no admins.")
    db.session.delete(user)
    db.session.commit()
    return jsonify({"poll": "OK"})


@blueprint.route("/users/<user_id>", methods=["PATCH"])
@login_required
@admin_permission.require(http_exception=401)
def edit_user(user_id):
    """
    Modify an existing user.

    Parameters
    ----------
    user_id : int

    Notes
    -----
    Will not allow you to revoke admin access if that would leave no
    admins.
    Expects json of the same form as `add_user`, but all parts are
    optional.

    See Also
    --------
    add_user

    """
    user = User.query.filter(User.id == user_id).first_or_404()
    edits = request.get_json()
    if "username" in edits:
        if len(edits["username"]) > 0:
            user.username = edits["username"]
        else:
            raise InvalidUsage("Username too short.", payload={"bad_field": "username"})
    if "password" in edits:
        if len(edits["password"]) > 0:
            if zxcvbn(edits["password"])["score"] > 3:
                user.password = edits["password"]
            else:
                raise InvalidUsage(
                    "Password not complex enough.", payload={"bad_field": "password"}
                )
    if "is_admin" in edits:
        if (
            not edits["is_admin"]
            and user.is_admin
            and len(User.query.filter(User.is_admin).all()) == 1
        ):
            raise InvalidUsage(
                "Removing this user's admin rights would leave no admins.",
                payload={"bad_field": "is_admin"},
            )
        else:
            user.is_admin = edits["is_admin"]
    if "require_two_factor" in edits:
        user.require_two_factor = edits["require_two_factor"]
    if (
        "has_two_factor" in edits
        and not edits["has_two_factor"]
        and user.two_factor_auth is not None
    ):
        db.session.delete(user.two_factor_auth)
    db.session.add(user)
    db.session.commit()
    return jsonify({"id": user.id})


@blueprint.route("/users/<user_id>")
@login_required
@admin_permission.require(http_exception=401)
def user_details(user_id):
    """
    Get the details of a user - id, name, admin poll, group memberships, server access granted
    to their user group, and their user group.

    Parameters
    ----------
    user_id:int

    Returns
    -------

    """
    user = User.query.filter(User.id == user_id).first_or_404()
    return jsonify(
        {
            "id": user.id,
            "name": user.username,
            "is_admin": user.is_admin,
            "has_two_factor": user.two_factor_auth is not None
            and user.two_factor_auth.enabled,
            "require_two_factor": user.require_two_factor,
            "roles": [{"id": role.id, "name": role.name} for role in user.roles],
        }
    )
