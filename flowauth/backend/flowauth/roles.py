# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flask import Blueprint, jsonify, request

from flask_login import current_user, login_required
from flask_principal import Permission, RoleNeed

from .models import Role, Server, User

blueprint = Blueprint(__name__.split(".").pop(), __name__)

@blueprint.route("/server/<server_id>", methods=["GET"])
@login_required
def list_my_roles_on_server(server_id):
    """
    Returns a list of roles for this user on this server
    """
    roles = {role for role in current_user.roles if int(server_id) == role.server.id}
    return jsonify(sorted([
        {
            "id":role.id,
            "name":role.name,
            "scopes":sorted([scope.scope for scope in role.scopes]),
            "latest_token_expiry": role.latest_token_expiry.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "longest_token_life_minutes": role.longest_token_life_minutes
        }
        for role in roles
    ], key=lambda x: x["id"]))