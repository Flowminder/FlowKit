# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flask import Blueprint, jsonify, request

from flask_login import current_user, login_required
from flowauth.jwt import generate_token

from .invalid_usage import InvalidUsage
from .models import *

blueprint = Blueprint(__name__, __name__)


@blueprint.route("/groups")
@login_required
def list_my_groups():
    """
    Get a list of the groups the logged in user is a member of.

    Notes
    -----
    Returns a list of json objects with "id" and "group_name" keys.
    """
    return jsonify(
        [{"id": group.id, "group_name": group.name} for group in current_user.groups]
    )


@blueprint.route("/servers")
@login_required
def list_my_servers():
    """
    Get a list of all the servers the logged in user has access to.

    Notes
    -----
    Produces a list of json objects with "id" and "server_name" fields.
    """
    servers = set()
    for group in current_user.groups:
        for server_token_limit in group.server_token_limits:
            servers.add(server_token_limit.server)

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


@blueprint.route("/tokens")
@login_required
def list_all_my_tokens():
    """Get a list of all the logged in user's tokens."""
    return jsonify(
        [
            {
                "id": token.id,
                "name": token.name,
                "token": token.decrypted_token,
                "expires": token.expires,
                "server_name": token.server.name,
                "username": token.owner.username,
            }
            for token in Token.query.filter(Token.owner == current_user)
        ]
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
                "expires": token.expires,
                "server_name": token.server.name,
                "username": token.owner.username,
            }
            for token in Token.query.filter(
                Token.owner == current_user, Token.server == server
            )
        ]
    )


@blueprint.route("/tokens/<server>", methods=["POST"])
@login_required
def add_token(server):
    """
    Generate a new token for a server.

    Parameters
    ----------
    server: int
        ID of the server.

    Notes
    -----
    Expects json with "name", "expiry" and "claims" keys, where "name" is a string,
    expiry is a datetime string of the form ""%Y-%m-%dT%H:%M:%S.%fZ" (e.g. 2018-01-01T00:00:00.0Z),
    and claims is a nested dict of the form {<claim_name>:{<right_name>:<bool>}}.

    Responds with a json object {"token":<token_string>, "id":<token_id>}.

    """
    server = Server.query.filter(Server.id == server).first_or_404()
    json = request.get_json()

    current_app.logger.debug("New token request", request=json)

    if "name" not in json:
        raise InvalidUsage("No name.", payload={"bad_field": "name"})
    expiry = datetime.datetime.strptime(json["expiry"], "%Y-%m-%dT%H:%M:%S.%fZ")
    lifetime = expiry - datetime.datetime.now()
    latest_lifetime = current_user.latest_token_expiry(server)
    if expiry > latest_lifetime:
        raise InvalidUsage("Token lifetime too long", payload={"bad_field": "expiry"})
    allowed_claims = current_user.allowed_claims(server)

    current_app.logger.debug("New token request", allowed_claims=allowed_claims)
    for claim in json["claims"]:
        if claim not in allowed_claims:
            raise Unauthorized(f"You do not have access to {claim} on {server.name}")

    token_string = generate_token(
        username=current_user.username,
        flowapi_identifier=server.name,
        lifetime=lifetime,
        claims=json["claims"],
        private_key=current_app.config["PRIVATE_JWT_SIGNING_KEY"],
    )

    token = Token(
        name=json["name"],
        token=token_string,
        expires=expiry,
        owner=current_user,
        server=server,
    )
    db.session.add(token)
    db.session.commit()
    return jsonify({"token": token_string, "id": token.id})
