# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flask import Blueprint, jsonify, request, current_app

from flask_login import current_user, login_required
from flask_principal import Permission, RoleNeed

from .models import Role, Server, User, db

blueprint = Blueprint(__name__.split(".").pop(), __name__)

admin_permission = Permission(RoleNeed("admin"))


def role_to_dict(role):
    return {
        "id": role.id,
        "name": role.name,
        "scopes": sorted([scope.id for scope in role.scopes]),
        "latest_token_expiry": role.latest_token_expiry.strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        ),
        "longest_token_life_minutes": role.longest_token_life_minutes,
        "server": role.server_id,
        "users": sorted([user.id for user in role.users]),
    }


@blueprint.route("/", methods=["GET"])
@login_required
@admin_permission.require(http_exception=401)
def list_roles():
    return jsonify([role_to_dict(role) for role in Role.query.all()])


@blueprint.route("/<role_id>", methods=["GET"])
@login_required
@admin_permission.require(http_exception=401)
def get_role(role_id):
    role = Role.query.filter(Role.id == role_id).first()  # First or error?
    return jsonify(role_to_dict(role))


@blueprint.route("/server/<server_id>", methods=["GET"])
@login_required
def list_my_roles_on_server(server_id):
    """
    Returns a list of roles for this user on this server
    """
    roles = {role for role in current_user.roles if int(server_id) == role.server.id}
    return jsonify(
        sorted(
            [
                {
                    "id": role.id,
                    "name": role.name,
                    "scopes": sorted([scope.scope for scope in role.scopes]),
                    "latest_token_expiry": role.latest_token_expiry.strftime(
                        "%Y-%m-%dT%H:%M:%S.%fZ"
                    ),
                    "longest_token_life_minutes": role.longest_token_life_minutes,
                    "server": role.server_id,
                }
                for role in roles
            ],
            key=lambda x: x["id"],
        )
    )
