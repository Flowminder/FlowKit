# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import datetime
from functools import reduce

from flask import Blueprint, jsonify, request

from flask_login import current_user, login_required
from sqlalchemy import select

from flowauth.jwt import generate_token

from .invalid_usage import InvalidUsage
from .models import *

blueprint = Blueprint(__name__.split(".").pop(), __name__)


@blueprint.route("/servers")
@login_required
def list_my_servers():
    """
    Get a list of all the servers the logged in user has access to.

    Notes
    -----
    Produces a list of json objects with "id" and "server_name" fields.
    """
    # is this the sqlalchemy way to do this?
    servers = {role.server for role in current_user.roles}
    return jsonify(
        sorted(
            [{"id": server.id, "server_name": server.name} for server in servers],
            key=lambda s: s["server_name"],
        )
    )


@blueprint.route("/servers/<server_id>")
@login_required
def my_access(server_id):
    """
    Get the api endpoints the logged in user is allowed to use on this
    server, and the latest datetime their token can expire.

    Parameters
    ----------
    server_id: int
        Id of the server to check access on

    Notes
    -----
    Produces a json object with "allowed_claims" and "latest_expiry" keys.
    "allowed_claims" will be an object with claim names as keys, and the
    available rights nested below.

    """
    server = Server.query.filter(Server.id == server_id).first_or_404()
    allowed_claims = current_user.allowed_claims(server)
    return jsonify(
        {
            "allowed_claims": allowed_claims,
            "latest_expiry": current_user.latest_token_expiry(server),
        }
    )


@blueprint.route("/tokens/<server>")
@login_required
def list_my_tokens(server):
    """Get a list of all the logged in user's tokens on a specific server."""
    server = Server.query.filter(Server.id == server).first_or_404()
    return jsonify(
        [
            {
                "id": token.id,
                "name": token.name,
                "token": token.decrypted_token,
                "expires": token.expiry,
                "server_name": token.server.name,
                "username": token.user.username,
            }
            for token in TokenHistory.query.filter(
                TokenHistory.user == current_user, TokenHistory.server == server
            )
        ]
    )


@blueprint.route("/tokens/<server_id>", methods=["POST"])
@login_required
def add_token(server_id):

    """
    Generate a new token for a server.

    Parameters
    ----------
    server: int
        ID of the server.

    Notes
    -----
    Expects json with "name", and "roles" keys, where "name" is a string,
    and roles is a list of role names.

    Responds with a json object {"token":<token_string>, "id":<token_id>}.

    """
    server = Server.query.filter(Server.id == server_id).first_or_404()
    json = request.get_json()

    current_app.logger.debug("New token request", request=json)
    if "name" not in json:
        raise InvalidUsage("No name.", payload={"bad_field": "name"})
    if "roles" not in json:
        raise InvalidUsage("No roles.", payload={"bad_field": "roles"})

    user_roles = db.session.execute(
        select(Role)
        .join(User.roles)
        .filter(User.id == current_user.id)
        .filter(Role.server_id == server_id)
    ).all()
    user_roles = [row[0] for row in user_roles]

    roles = []
    for requested_role in json["roles"]:
        try:
            this_role = next(
                filter(lambda x: x.name == requested_role["name"], user_roles)
            )
        except StopIteration:
            raise Unauthorized(
                f"Role '{requested_role['name']}' is not permitted for the current user"
            )
        roles.append(
            {
                "name": this_role.name,
                "latest_token_expiry": this_role.latest_token_expiry,
                "scopes": [s.name for s in this_role.scopes],
            }
        )

    expiry = reduce(
        lambda prev, cur: prev if prev < cur else cur,
        (role.pop("latest_token_expiry") for role in roles),
    )
    lifetime = expiry - datetime.datetime.now()
    latest_lifetime = current_user.latest_token_expiry(server)
    if expiry > latest_lifetime:
        raise InvalidUsage("Token lifetime too long", payload={"bad_field": "expiry"})

    if expiry < datetime.datetime.now():
        raise Unauthorized(f"Token for {current_user.username} expired")

    token_string = generate_token(
        flowapi_identifier=server.name,
        username=current_user.username,
        private_key=current_app.config["PRIVATE_JWT_SIGNING_KEY"],
        lifetime=lifetime,
        roles={role["name"]: sorted(role["scopes"]) for role in roles},
    )

    history_entry = TokenHistory(
        name=json["name"],
        user_id=current_user.id,
        server_id=server.id,
        expiry=expiry,
        token=token_string,
    )

    db.session.add(history_entry)
    db.session.commit()

    return jsonify({"token": token_string})
