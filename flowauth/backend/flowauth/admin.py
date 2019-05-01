# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flask import jsonify, Blueprint, request
from flask_login import login_required
from flask_principal import Permission, RoleNeed
from zxcvbn import zxcvbn

from .models import *
from .invalid_usage import InvalidUsage

blueprint = Blueprint(__name__, __name__)
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
    return jsonify(
        {"id": server.id, "name": server.name, "secret_key": server.secret_key}
    )


@blueprint.route("/capabilities")
@login_required
@admin_permission.require(http_exception=401)
def list_all_capabilities():
    """
    Get a list of all the capabilities.

    Notes
    -----
    Responds with {<capability_name>: {"id":<capability_id>, "permissions":{<right>:False}}}
    """
    default_permission_setting: bool = current_app.config["DEMO_MODE"]
    aggregations = (
        [agg.name for agg in SpatialAggregationUnit.query.all()]
        if current_app.config["DEMO_MODE"]
        else []
    )
    return jsonify(
        {
            cap.name: {
                "id": cap.id,
                "permissions": {
                    "get_result": default_permission_setting,
                    "run": default_permission_setting,
                    "poll": default_permission_setting,
                },
                "spatial_aggregation": aggregations,
            }
            for cap in Capability.query.all()
        }
    )


@blueprint.route("/capabilities", methods=["POST"])
@login_required
@admin_permission.require(http_exception=401)
def add_capability():
    """
    Add a new capability.

    Notes
    -----
    Expects json with a "name" field.
    Responds with {"name":<capability_name>, "id":<capability_id>}
    """
    cap = request.get_json()
    cap = Capability(**cap)
    db.session.add(cap)
    db.session.commit()
    return jsonify({"name": cap.name, "id": cap.id})


@blueprint.route("/capabilities/<cap_id>", methods=["DELETE"])
@login_required
@admin_permission.require(http_exception=401)
def rm_capability(cap_id):
    """
    Delete a capability.
    """
    cap = Capability.query.filter(Capability.id == cap_id).first_or_404()
    db.session.delete(cap)
    db.session.commit()
    return jsonify(), 200


@blueprint.route("/servers/<server_id>/capabilities")
@login_required
@admin_permission.require(http_exception=401)
def list_server_capabilities(server_id):
    """
    Get a list of all the capabilities enabled on a server.

    Notes
    -----
    Responds with {<capability_name>: {"id":<capability_id>, "permissions":{<right>:<bool>}}}

    Note that this will also include capabilities not explicitly added to the server, but
    will list all their permissions as False.
    """
    server = Server.query.filter(Server.id == server_id).first_or_404()
    agg_units = SpatialAggregationUnit.query.all()
    caps = {
        cap.name: {
            "id": cap.id,
            "permissions": {"get_result": False, "run": False, "poll": False},
            "spatial_aggregation": sorted([unit.name for unit in agg_units]),
        }
        for cap in Capability.query.all()
    }
    caps.update(
        {
            cap.capability.name: {
                "id": cap.capability.id,
                "permissions": {
                    "get_result": cap.get_result,
                    "run": cap.run,
                    "poll": cap.poll,
                },
                "spatial_aggregation": sorted(
                    [
                        unit.name
                        for unit in cap.spatial_aggregation
                        if unit.name in caps[cap.capability.name]["spatial_aggregation"]
                    ]
                ),
            }
            for cap in server.capabilities
        }
    )
    return jsonify(caps)


@blueprint.route("/servers/<server_id>/capabilities", methods=["PATCH"])
@login_required
@admin_permission.require(http_exception=401)
def edit_server_capabilities(server_id):
    """
    Alter the capabilities enabled on a server.

    Notes
    -----
    Expects json of the form {<capability_name>: {"id":<capability_id>, "permissions":{<right>:<bool>}}}

    Any capabilities not included are removed.
    """
    server_obj = Server.query.filter(Server.id == server_id).first_or_404()
    json = request.get_json()
    caps = []
    print(json)
    for cap, rights in json.items():
        cap = ServerCapability.query.filter(
            ServerCapability.server_id == server_id,
            ServerCapability.capability_id == rights["id"],
        ).first()
        if cap is None:
            cap = ServerCapability(
                server=server_obj,
                capability=Capability.query.filter(
                    Capability.id == rights["id"]
                ).first_or_404(),
                **rights["permissions"],
            )
        else:
            for right, value in rights["permissions"].items():
                setattr(cap, right, value)
        if "spatial_aggregation" in rights:
            agg_units = [
                SpatialAggregationUnit.query.filter(
                    SpatialAggregationUnit.name == name
                ).first()
                for name in rights["spatial_aggregation"]
            ]
            cap.spatial_aggregation = agg_units
        caps.append(cap)
        db.session.add(cap)
    server_obj.capabilities = caps
    db.session.add(server_obj)
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
    "longest_token_life":<int>, "name":<server_name>}
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
    server = Server(**json)
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
    server = Server.query.filter(Server.id == server_id).first_or_404()
    db.session.delete(server)
    db.session.commit()
    return jsonify({"poll": "OK"})


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
    Return json of the form {<capability>:{"id":<capability_id>, "permissions":{<right>:<bool>}}
    """
    group = Group.query.filter(Group.id == group_id).first_or_404()
    server = Server.query.filter(Server.id == server_id).first_or_404()
    group_permissions = [
        p for p in group.server_permissions if p.server_capability.server == server
    ]
    group_rights = {
        cap.capability.name: {
            "id": cap.id,
            "permissions": {"run": False, "get_result": False, "poll": False},
            "spatial_aggregation": [agg.name for agg in cap.spatial_aggregation],
        }
        for cap in server.capabilities
    }
    print(f"Group rights: {group_rights}")
    for p in group_permissions:
        print(p.spatial_aggregation)
        group_rights[p.server_capability.capability.name] = {
            "id": p.server_capability.id,
            "permissions": {
                "run": p.run and p.server_capability.run,
                "get_result": p.get_result and p.server_capability.get_result,
                "poll": p.poll and p.server_capability.poll,
            },
            "spatial_aggregation": [
                agg.name
                for agg in p.spatial_aggregation
                if agg.name
                in group_rights[p.server_capability.capability.name][
                    "spatial_aggregation"
                ]
            ],
        }
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
    print(servers)
    existing_limits = group.server_token_limits
    existing_perms = group.server_permissions
    revised_limits = []
    revised_perms = []
    db.session.add(group)
    for server in servers:
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
        for right_name, right in server["rights"].items():
            try:
                server_capability = [
                    cap
                    for cap in server_obj.capabilities
                    if cap.capability.name == right_name
                ][0]

                perm = GroupServerPermission.query.filter(
                    GroupServerPermission.group_id == group.id,
                    GroupServerPermission.server_capability_id == server_capability.id,
                ).first()
                if perm is None:
                    perm = GroupServerPermission(
                        server_capability=server_capability,
                        group=group,
                        **right["permissions"],
                    )
                else:
                    for name, val in right["permissions"].items():
                        setattr(perm, name, val)
                        if val and not getattr(server_capability, name):
                            raise InvalidUsage(
                                f"Permission '{name}' not enabled for this server.",
                                payload={"bad_field": name},
                            )
                if "spatial_aggregation" in right:
                    print(right["spatial_aggregation"])
                    agg_units = [
                        agg
                        for agg in server_capability.spatial_aggregation
                        if agg.name in right["spatial_aggregation"]
                    ]
                    db.session.add_all(agg_units)
                    perm.spatial_aggregation = agg_units
                    print(perm.spatial_aggregation)
                revised_perms.append(perm)
                db.session.add(perm)
            except IndexError:
                pass  # The capability isn't turned on at all for this server, so ignore

    # clean up
    for limit in existing_limits:
        if limit not in revised_limits:
            db.session.delete(limit)
    for perm in existing_perms:
        if perm not in revised_perms:
            db.session.delete(perm)
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
    if zxcvbn(json["password"])["score"] > 3:
        user = User(**json)
    else:
        raise InvalidUsage(
            "Password not complex enough.", payload={"bad_field": "password"}
        )
    if User.query.filter(User.username == json["username"]).first() is not None:
        raise InvalidUsage(
            "Username already exists.", payload={"bad_field": "username"}
        )
    user_group = Group(name=user.username, user_group=True)
    user.groups.append(user_group)
    db.session.add(user)
    db.session.add(user_group)
    db.session.commit()
    return jsonify({"id": user.id, "group_id": user_group.id})


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
    user_group = [g for g in user.groups if g.user_group][0]
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
    db.session.add(user)
    db.session.commit()
    return jsonify({"id": user.id, "group_id": user_group.id})


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
    user_group = [g for g in user.groups if g.user_group][0]
    return jsonify(
        {
            "id": user.id,
            "name": user.username,
            "is_admin": user.is_admin,
            "groups": [{"id": group.id, "name": group.name} for group in user.groups],
            "servers": [
                {
                    "id": server_token_limit.server.id,
                    "name": server_token_limit.server.name,
                }
                for server_token_limit in user_group.server_token_limits
            ],
            "group_id": user_group.id,
        }
    )


@blueprint.route("/users/<user_id>/groups")
@login_required
@admin_permission.require(http_exception=401)
def get_user_groups(user_id):
    """
    Get all the groups a user is a member of.

    Parameters
    ----------
    user_id:int
    """
    user = User.query.filter(User.id == user_id).first_or_404()
    return jsonify(
        [
            {"id": group.id, "name": group.name}
            for group in user.groups
            if not group.user_group
        ]
    )


@blueprint.route("/users/<user_id>/user_group")
@login_required
@admin_permission.require(http_exception=401)
def get_user_group(user_id):
    """
    Get a user's personal group.

    Parameters
    ----------
    user_id: int

    """
    user = User.query.filter(User.id == user_id).first_or_404()
    user_group = [g for g in user.groups if g.user_group][0]
    return jsonify({"id": user_group.id, "name": user_group.name})


@blueprint.route("/users/<user_id>/groups", methods=["PATCH"])
@login_required
@admin_permission.require(http_exception=401)
def set_user_groups(user_id):
    """
    Alter the groups a user is a member of.

    Parameters
    ----------
    user_id: int
        ID of the user

    Notes
    -----
    Expects a json object of the form {"groups":[<group_id>, ...]}, and returns
    the amended list of groups as [{"id":<gid>, "name":<group_name>},..]

    """
    user = User.query.filter(User.id == user_id).first_or_404()
    user_group = [g for g in user.groups if g.user_group][0]
    groups = request.get_json()["groups"]
    print(groups)
    groups = [
        Group.query.filter(Group.id == group["id"]).first_or_404() for group in groups
    ]
    user.groups = groups + [user_group]
    db.session.add(user)
    db.session.commit()
    return jsonify(
        [
            {"id": group.id, "name": group.name}
            for group in user.groups
            if not group.user_group
        ]
    )


@blueprint.route("/tokens")
@login_required
@admin_permission.require(http_exception=401)
def list_all_tokens():
    """
    Get all the tokens.
    """
    return jsonify(
        [
            {
                "id": token.id,
                "name": token.name,
                "token": token.token,
                "expires": token.expires,
                "server_name": token.server.name,
                "username": token.owner.username,
            }
            for token in Token.query.all()
        ]
    )
