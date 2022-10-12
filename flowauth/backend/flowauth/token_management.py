# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import datetime
from functools import reduce

from flask import Blueprint, jsonify, request

from flask_login import current_user, login_required
from sqlalchemy import select

from flowauth.jwt import generate_token

from .invalid_usage import InvalidUsage, Unauthorized
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
        # I think I might also rewrite this bit as just iterating through the requested roles and
        # doing Role.query.filter(Role.id == requested_role).first_or_404()?
        try:
            this_role = Role.query.filter(
                Role.name == requested_role["name"]
            ).first_or_404()
        except StopIteration:
            raise Unauthorized(
                f"Role '{requested_role['name']}' is not permitted for the current user"
            )
        roles.append(this_role)
    # For each role, we want to check
    # The role expiry date doesn't beat the server expiry date
    # The role longest lifetime doesn't beat the server longest lifetime
    # If you request token with a role with a expiry past the server final expiry, then issue the token with the server's final expiry
    # feature todo: flag this to the user

    token_expiry = min(server.next_expiry(), min(rr.next_expiry() for rr in roles))

    current_app.logger.debug("token_expiry")

    token_string = generate_token(
        flowapi_identifier=server.name,
        username=current_user.username,
        private_key=current_app.config["PRIVATE_JWT_SIGNING_KEY"],
        lifetime=token_expiry - datetime.datetime.now(),
        roles={role.name: sorted([ss.name for ss in role.scopes]) for role in roles},
    )

    history_entry = TokenHistory(
        name=json["name"],
        user_id=current_user.id,
        server_id=server.id,
        expiry=token_expiry,
        token=token_string,
    )

    db.session.add(history_entry)
    db.session.commit()

    return jsonify({"token": token_string})
