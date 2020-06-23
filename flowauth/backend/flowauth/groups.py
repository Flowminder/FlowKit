# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flask import Blueprint, jsonify, request

from flask_login import login_required
from flask_principal import Permission, RoleNeed

from .invalid_usage import InvalidUsage
from .models import *

blueprint = Blueprint(__name__, __name__)
admin_permission = Permission(RoleNeed("admin"))


@blueprint.route("/groups")
@login_required
@admin_permission.require(http_exception=401)
def list_all_groups():
    """List all the groups."""
    return jsonify(
        [
            {"id": group.id, "name": group.name}
            for group in Group.query.filter(Group.user_group != True)
        ]
    )


@blueprint.route("/groups", methods=["POST"])
@login_required
@admin_permission.require(http_exception=401)
def add_group():
    """
    Create a group.

    Notes
    -----
    Expects a json object with a "name" key. Returns {"id":<gid>, "name": <group_name>}
    """
    json = request.get_json()
    group = Group(**json)
    if Group.query.filter(Group.name == json["name"]).first() is not None:
        raise InvalidUsage(
            "Group name already exists.", payload={"bad_field": "groupname"}
        )
    else:
        db.session.add(group)
        db.session.commit()
        return jsonify({"id": group.id, "name": group.name})


@blueprint.route("/groups/<group_id>", methods=["PATCH"])
@login_required
@admin_permission.require(http_exception=401)
def edit_group(group_id):
    """
    Change the name of a group.

    Notes
    -----
    Expects a json object with a "name" key.
    """
    group = Group.query.filter(Group.id == group_id).first_or_404()
    group.name = request.get_json()["name"]
    db.session.add(group)
    db.session.commit()
    return jsonify({"id": group.id, "name": group.name})


@blueprint.route("/groups/<group_id>", methods=["DELETE"])
@login_required
@admin_permission.require(http_exception=401)
def rm_group(group_id):
    """Delete a group if it exists."""
    group = Group.query.filter(Group.id == group_id).first_or_404()
    db.session.delete(group)
    db.session.commit()
    return jsonify({"poll": "OK"})


@blueprint.route("/groups/<group_id>")
@login_required
@admin_permission.require(http_exception=401)
def group_details(group_id):
    """
    Get the details of a group.

    Notes
    -----
    {"id":<gid>, "name":<group_name>, "members":[{"id":<uid>, "name":<username>}..],
    "servers":[{"id":<server_id>, "name":<server_name>}..]}
    """
    group = Group.query.filter(Group.id == group_id).first_or_404()
    return jsonify(
        {
            "id": group.id,
            "name": group.name,
            "members": [
                {"id": user.id, "name": user.username} for user in group.members
            ],
            "servers": [
                {
                    "id": server_token_limit.server.id,
                    "name": server_token_limit.server.name,
                }
                for server_token_limit in group.server_token_limits
            ],
        }
    )


@blueprint.route("/groups/<group_id>/servers")
@login_required
@admin_permission.require(http_exception=401)
def group_servers(group_id):
    """List all the servers a group can access."""
    group = Group.query.filter(Group.id == group_id).first_or_404()
    return jsonify(
        [
            {"id": server_token_limit.server.id, "name": server_token_limit.server.name}
            for server_token_limit in group.server_token_limits
        ]
    )


@blueprint.route("/groups/<group_id>/servers/<server_id>/time_limits")
@login_required
@admin_permission.require(http_exception=401)
def group_server_time_limits(group_id, server_id):
    """
    Get the longest lifetime for tokens (in minutes) and latest expiry date
    for a group on one server.

    """
    limits = GroupServerTokenLimits.query.filter(
        GroupServerTokenLimits.group_id == group_id,
        GroupServerTokenLimits.server_id == server_id,
    ).first_or_404()
    return jsonify(
        {
            "longest_token_life": limits.longest_life,
            "latest_token_expiry": limits.latest_end,
        }
    )


@blueprint.route("/groups/<group_id>/servers/<server_id>/capabilities")
@login_required
@admin_permission.require(http_exception=401)
def group_server_rights(group_id, server_id):
    """
    Enumerate the rights a group has on a server.

    Notes
    -----
    Return json of the form [<capability>,...]
    """
    group = Group.query.filter(Group.id == group_id).first_or_404()
    server = Server.query.filter(Server.id == server_id).first_or_404()
    group_rights = group.rights(server)
    return jsonify(group_rights)


@blueprint.route("/groups/<group_id>/servers", methods=["PATCH"])
@login_required
@admin_permission.require(http_exception=401)
def edit_group_servers(group_id):
    """
    Alter the server access rights of a group.

    Parameters
    ----------
    group_id: int

    Notes
    -----
    Expects json of the form {"servers":[{"id":<server_id>, "latest_expiry":<%Y-%m-%dT%H:%M:%S.%fZ>,
    "max_life":<n_minutes>, "rights":{<capability_name>:{"permissions":{<right_name>:<bool>}}}}

    Will not accept latest_expiry or max_life greater than those allowed on the server, and will
    reject any claims not allowed on the server.
    """
    servers = request.get_json()["servers"]
    group = Group.query.filter(Group.id == group_id).first_or_404()
    current_app.logger.debug("Edit group servers", servers=servers, group_id=group_id)
    existing_limits = group.server_token_limits

    db.session.add(group)
    revised_limits = []

    for server in servers:
        server["rights"] = set(server["rights"])
        server_obj = Server.query.filter(Server.id == server["id"]).first_or_404()
        # Create limits
        limits = GroupServerTokenLimits.query.filter(
            GroupServerTokenLimits.group_id == group.id,
            GroupServerTokenLimits.server_id == server_obj.id,
        ).first()
        if limits is None:
            limits = GroupServerTokenLimits(
                latest_end=datetime.datetime.strptime(
                    server["latest_expiry"], "%Y-%m-%dT%H:%M:%S.%fZ"
                ),
                longest_life=server["max_life"],
                group=group,
                server=server_obj,
            )

        else:
            limits.latest_end = datetime.datetime.strptime(
                server["latest_expiry"], "%Y-%m-%dT%H:%M:%S.%fZ"
            )
            limits.longest_life = server["max_life"]
        if limits.longest_life > server_obj.longest_token_life:
            raise InvalidUsage("Lifetime too long", payload={"bad_field": "max_life"})
        if limits.latest_end > server_obj.latest_token_expiry:
            raise InvalidUsage(
                "End date too late", payload={"bad_field": "latest_expiry"}
            )
        revised_limits.append(limits)
        db.session.add(limits)
        # Create permissions

        to_add = []
        to_remove = []
        for cap in GroupServerPermission.query.filter(
            GroupServerPermission.group_id == group.id
        ).filter(GroupServerPermission.server_capability.server_id == server_obj.id):
            try:
                server["rights"].remove(cap.server_capability.capability)
            except KeyError:
                to_remove.append(cap)
        for right in server["rights"]:
            cap = ServerCapability.query.filter(
                ServerCapability.capability == right,
                ServerCapability.server_id == server_obj.id,
                ServerCapability.enabled,
            ).first()
            if cap is None:
                raise InvalidUsage(f"{right} not enabled for this server.")

            gsp = GroupServerPermission(group_id=group.id, server_capability_id=cap.id)
            current_app.logger.debug(
                "Added permission.",
                server_capability_id=gsp.server_capability_id,
                group_id=group.id,
                server_id=server_obj.id,
            )
            to_add.append(gsp)
        db.session.bulk_save_objects(to_add)
        # clean up
        for gsp in to_remove:
            current_app.logger.debug(
                "Removed permission",
                server_capability_id=gsp.server_capability.capability,
                group_id=gsp.group.id,
                server_id=gsp.server_capability.server.id,
            )
            db.session.delete(gsp)
    for limit in existing_limits:
        if limit not in revised_limits:
            current_app.logger.debug(
                "Removed limit.", group_id=limit.group.id, server=limit.server.id,
            )
            db.session.delete(limit)
    db.session.commit()
    return jsonify({"id": group.id, "name": group.name})


@blueprint.route("/groups/<group_id>/members")
@login_required
@admin_permission.require(http_exception=401)
def group_members(group_id):
    """List the members of a group."""
    group = Group.query.filter(Group.id == group_id).first_or_404()
    return jsonify([{"id": user.id, "name": user.username} for user in group.members])


@blueprint.route("/groups/<group_id>/members", methods=["PATCH"])
@login_required
@admin_permission.require(http_exception=401)
def edit_group_members(group_id):
    """
    Alter the membership of a group

    Parameters
    ----------
    group_id: int
        ID of the group

    Notes
    -----
    Expects a json object of the form {"members":[<member_id>, ...]}, and returns
     {"id":<gid>, "name":<group_name>}

    """
    members = request.get_json()["members"]
    group = Group.query.filter(Group.id == group_id).first_or_404()
    members = [
        User.query.filter(User.id == member["id"]).first_or_404() for member in members
    ]
    group.members = members
    db.session.add(group)
    db.session.commit()
    return jsonify({"id": group.id, "name": group.name})
