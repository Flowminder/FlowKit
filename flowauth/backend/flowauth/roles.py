# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flask import Blueprint, jsonify, request, current_app
import datetime

from flask_login import current_user, login_required
from flask_principal import Permission, RoleNeed

from .models import Role, Scope, Server, User, db

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


@blueprint.route("/", methods=["POST"])
@login_required
@admin_permission.require(http_exception=401)
def add_role():
    json = request.get_json()
    json["latest_token_expiry"] = datetime.datetime.strptime(
        json["latest_token_expiry"], "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    server = Server.query.filter(Server.id == json["server_id"]).first()
    role_scopes = Scope.query.filter(Scope.id.in_(json["scopes"])).all()
    role_users = User.query.filter(User.id.in_(json["users"])).all()
    new_role = Role(
        name=json["name"],
        scopes=role_scopes,
        users=role_users,
        server=server,
        latest_token_expiry=json["latest_token_expiry"],
        longest_token_life_minutes=json["longest_token_life_minutes"],
    )
    db.session.add(new_role)
    db.session.commit()
    current_app.logger.info(f"Created role {new_role.name}")
    return get_role(new_role.id)


# NOTES FOR REVIEW: I wondered whether to have this merge lists of users and scopes
# when updated, but in the end I think that it's cleaner to have that on the frontend
# and make an explicit decision
@blueprint.route("/<role_id>", methods=["PATCH"])
@login_required
@admin_permission.require(http_exception=401)
def edit_role(role_id):
    edits = request.get_json()
    role = Role.query.filter(Role.id == role_id).first_or_404()
    for key, value in edits.items():
        if key == "id":
            current_app.logger.warning("Cannot change role ID; ignoring")
            pass
        if key == "users":
            value = [User.query.filter(User.id == uid).first() for uid in value]
        elif key == "scopes":
            value = [Scope.query.filter(Scope.id == sid).first() for sid in value]
        elif key == "server":
            value = Server.query.filter(Server.id == value).first()
        elif key == "latest_token_expiry":
            value = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
        setattr(role, key, value)
    db.session.add(role)
    db.session.commit()
    current_app.logger.info(f"Role {role_id} updated with {edits}")
    # TODO: Add audit log here
    return list_roles()


@blueprint.route("/<role_id>", methods=["DELETE"])
@login_required
@admin_permission.require(http_exception=401)
def delete_role(role_id):
    role = Role.query.filter(Role.id == role_id).first_or_404()
    db.session.delete(role)
    db.session.commit()
    current_app.logger.info(f"Role {role_id} deleted.")
    return jsonify({"state": "done"})


@blueprint.route("/<role_id>/members")
@login_required
@admin_permission.require(http_exception=401)
def get_role_members(role_id):
    role = Role.query.filter(Role.id == role_id).first_or_404()

    return jsonify([{"name": user.username, "id": user.id} for user in role.users])


@blueprint.route("/<role_id>/scopes")
@login_required
@admin_permission.require(http_exception=401)
def get_role_scopes(role_id):
    role = Role.query.filter(Role.id == role_id).first_or_404()
    return jsonify([{"name": scope.name, "id": scope.id} for scope in role.scopes])


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
                    "scopes": sorted([scope.name for scope in role.scopes]),
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
