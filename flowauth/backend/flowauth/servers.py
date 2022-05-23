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
from flowauth.models import Role, Server, Scope, ServerCapability, db

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
    role_scopes = [Scope(scope=scope, server=server) for scope in json["scopes"]]
    new_role = Role(name=json["name"], scopes=role_scopes, server_id=server.id)
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
    return jsonify({scope.id: scope.scope for scope in server.scopes})


@blueprint.route("/servers/<server_id>/capabilities")
@login_required
@admin_permission.require(http_exception=401)
def list_server_capabilities(server_id):
    """
    Get a list of all the capabilities enabled on a server.

    Notes
    -----
    Responds with {<capability_name>: <enabled>}
    """
    server = Server.query.filter(Server.id == server_id).first_or_404()
    return jsonify({cap.capability: cap.enabled for cap in server.capabilities})


@blueprint.route("/servers/<server_id>/capabilities", methods=["PATCH"])
@login_required
@admin_permission.require(http_exception=401)
def edit_server_capabilities(server_id):
    """
    Alter the capabilities enabled on a server.

    Notes
    -----
    Expects json of the form {<capability_name>: bool}

    Any capabilities not included are removed.
    """
    server_obj = Server.query.filter_by(id=server_id).first_or_404()
    json = request.get_json()

    to_remove = []
    caps = []
    for x in server_obj.capabilities:
        try:
            x.enabled = json.pop(x.capability)
            caps.append(x)
        except KeyError:
            to_remove.append(x)
    current_app.logger.debug(
        "Editing capabilities for server", server_id=server_obj, new_permissions=json
    )

    caps += [
        ServerCapability(
            server_id=server_id,
            capability=cap,
            capability_hash=md5(cap.encode()).hexdigest(),
            enabled=enabled,
        )
        for cap, enabled in json.items()
    ]
    db.session.bulk_save_objects(caps)

    for cap in to_remove:
        db.session.delete(cap)

    db.session.commit()
    return jsonify({"poll": "OK"})


@blueprint.route("/servers/<server_id>/time_limits")
@login_required
@admin_permission.require(http_exception=401)
def list_server_time_limits(server_id):
    """
    Get the longest lifetime for tokens (in minutes) and latest expiry date
    on a server.
    """
    server = Server.query.filter(Server.id == server_id).first_or_404()
    return jsonify(
        {
            "longest_token_life": server.longest_token_life,
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
    "longest_token_life":<int>, "name":<server_name>, "scopes"[<list of scopes>]}
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

    # TODO: Scope validation of some kind?
    # I didn't like injecting all params straight into server; feels like a scope
    # for escalation if someone can pretend to be a server successfully
    try:
        server = Server(
            name=json["name"],
            latest_token_expiry=json["latest_token_expiry"],
            longest_token_life=json["longest_token_life"],
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
    "longest_token_life":<int>, "name":<server_name>}

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
    for cap in server.capabilities:
        db.session.delete(cap)
    db.session.delete(server)
    db.session.commit()
    return jsonify({"poll": "OK"})
