# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from cryptography.hazmat.primitives import serialization
from flask import Blueprint, jsonify, request

from flask_login import login_required
from flask_principal import Permission, RoleNeed

from flowauth.models import Token

blueprint = Blueprint(__name__.split(".").pop(), __name__)
admin_permission = Permission(RoleNeed("admin"))


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
                "token": token.decrypted_token,
                "expires": token.expires,
                "server_name": token.server.name,
                "username": token.owner.username,
            }
            for token in Token.query.all()
        ]
    )


@blueprint.route("/public_key")
@login_required
@admin_permission.require(http_exception=401)
def get_public_key():
    """
    Get the public key which can be used to verify tokens for this
    flowauth server.
    """
    key = (
        current_app.config["PRIVATE_JWT_SIGNING_KEY"]
        .public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode()
        .strip()
    )
    current_app.logger.debug("Got public key", key=key)
    return jsonify({"public_key": key})
