# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import datetime
import logging
from hashlib import md5

from flask import Blueprint, jsonify, request, current_app

from flask_login import login_required
from flask_principal import Permission, RoleNeed

from flowauth.invalid_usage import InvalidUsage
from flowauth.models import Role, Server, Scope, db

blueprint = Blueprint(__name__.split(".").pop(), __name__)
admin_permission = Permission(RoleNeed("admin"))


@blueprint.route("/servers")
@login_required
@admin_permission.require(http_exception=401)
def list_all_servers():
    """Get a list of all the servers in the form [{"id":<server_id>, "name":<server_name>}]"""
    return jsonify(
        [{"id": server.id, "name": server.name} for server in Server.query.all()]
    )


@blueprint.route("/servers/<server_id>")
@login_required
@admin_permission.require(http_exception=401)
def get_server(server_id):
    """
    Get the id, name, and secret key of a server by its ID.

    Notes
    -----
    Responds with {"id":<server_id>, "name":<server_name>, "secret_key":<secret_key>}
    """
    server = Server.query.filter(Server.id == server_id).first_or_404()
    return jsonify({"id": server.id, "name": server.name})


@blueprint.route("/servers/<server_id>/roles")
@login_required
@admin_permission.require(http_exception=401)
def get_roles(server_id):
    """
    Gets the id, name and scopes granted on a role in a server

    Notes
    -----
    Responds with {"id":<role_id>, "name":<role_name>, "scopes":[<list of scopes>]
    """
    server = Server.query.filter(Server.id == server_id).first_or_404()
    return jsonify(
        list(
            {
                "id": role.id,
                "name": role.name,
                "scopes": [scope.scope for scope in role.scopes],
                "latest_token_expiry": role.latest_token_expiry.strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                ),
                "longest_token_life_minutes": role.longest_token_life_minutes,
            }
            for role in server.roles
        )
    )


@blueprint.route("/servers/<server_id>/roles", methods=["POST"])
@login_required
@admin_permission.require(http_exception=401)
def add_role(server_id):
    server = Server.query.filter_by(id=server_id).first_or_404()
    json = request.get_json()
    json["latest_token_expiry"] = datetime.datetime.strptime(
        json["latest_token_expiry"], "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    role_scopes = [Scope(scope=scope, server=server) for scope in json["scopes"]]
    new_role = Role(
        name=json["name"],
        scopes=role_scopes,
        server_id=server.id,
        latest_token_expiry=json["latest_token_expiry"],
        longest_token_life_minutes=json["longest_token_life_minutes"],
    )
    db.session.add(new_role)
    db.session.commit()
    return get_roles(server_id)


@blueprint.route("/servers/<server_id>/scopes")
@login_required
@admin_permission.require(http_exception=401)
def list_scopes(server_id):
    """
    Returns the list of available scopes on a server
    """
    server = Server.query.filter_by(id=server_id).first_or_404()
    return jsonify({scope.scope: scope.enabled for scope in server.scopes})


@blueprint.route("/servers/<server_id>/scopes", methods=["PATCH"])
@login_required
@admin_permission.require(http_exception=401)
def edit_scope_activation(server_id):
    """
    Bulk activates/deactivates scopes on a server
    Expects a json of the form {scope_string:True/False}

    """
    server = Server.query.filter_by(id=server_id).first_or_404()
    json = request.get_json()
    scopes_to_edit = (
        db.session.query(Scope)
        .join(Server)
        .filter(Scope.server_id == server_id)
        .filter(Scope.scope.in_(json.keys()))
    )
    for scope in scopes_to_edit:
        scope.enabled = json[scope.scope]
        db.session.add(scope)
    db.session.commit()
    return list_scopes(server_id)


@blueprint.route("/servers/<server_id>/time_limits")
@login_required
@admin_permission.require(http_exception=401)
def list_server_time_limits(server_id):
    """
    Get the longest lifetime for tokens (in minutes) and latest expiry date
    on a server.
    """
    server = Server.query.filter(Server.id == server_id).first_or_404()
    # I have no idea why this test is failing, it's been like this since before
    # I started messing with stuff
    return jsonify(
        {
            "longest_token_life_minutes": server.longest_token_life_minutes,
            "latest_token_expiry": server.latest_token_expiry,
        }
    )


@blueprint.route("/servers", methods=["POST"])
@login_required
@admin_permission.require(http_exception=401)
def add_server():
    """
    Create a new server.

    Notes
    -----
    Expects json of the form {"latest_token_expiry":<%Y-%m-%dT%H:%M:%S.%fZ>, "secret_key":<key>,
    "longest_token_life_minutes":<int>, "name":<server_name>, "scopes"[<list of scopes>]}
    """
    json = request.get_json()
    json["latest_token_expiry"] = datetime.datetime.strptime(
        json["latest_token_expiry"], "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    if "name" not in json:
        raise InvalidUsage("Must provide server name", payload={"bad_field": "name"})
    if len(json["name"]) == 0:
        raise InvalidUsage("Must provide server name", payload={"bad_field": "name"})
    if len(json["name"]) > 120:
        raise InvalidUsage(
            "Server name must be 120 characters or less.", payload={"bad_field": "name"}
        )
    if Server.query.filter(Server.name == json["name"]).first() is not None:
        raise InvalidUsage(
            "Server with this name already exists.", payload={"bad_field": "name"}
        )

    try:
        server = Server(
            name=json["name"],
            latest_token_expiry=json["latest_token_expiry"],
            longest_token_life_minutes=json["longest_token_life_minutes"],
        )
    except KeyError as e:
        raise InvalidUsage from e
    try:
        scopes_list = [
            Scope(scope=scope_str, server=server) for scope_str in json["scopes"]
        ]
        server.scopes = scopes_list
    except KeyError:
        logging.warning(f"No scopes set for {server.name}")
        pass
    db.session.add(server)
    db.session.commit()
    return jsonify({"id": server.id})


@blueprint.route("/servers/<server_id>", methods=["PATCH"])
@login_required
@admin_permission.require(http_exception=401)
def edit_server(server_id):
    """
    Alter the name, latest token expiry, secret key or longest token life of a server.

    Notes
    -----
    Expects json of the form {"latest_token_expiry":<%Y-%m-%dT%H:%M:%S.%fZ>, "secret_key":<key>,
    "longest_token_life_minutes":<int>, "name":<server_name>}

    """
    server = Server.query.filter(Server.id == server_id).first_or_404()
    json = request.get_json()
    json["latest_token_expiry"] = datetime.datetime.strptime(
        json["latest_token_expiry"], "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    for key, val in json.items():
        setattr(server, key, val)
    db.session.add(server)
    db.session.commit()
    return jsonify({"id": server.id})


@blueprint.route("/servers/<server_id>", methods=["DELETE"])
@login_required
@admin_permission.require(http_exception=401)
def rm_server(server_id):
    """Remove a server."""
    server = Server.query.filter_by(id=server_id).first_or_404()
    db.session.delete(server)
    db.session.commit()
    return jsonify({"poll": "OK"})
