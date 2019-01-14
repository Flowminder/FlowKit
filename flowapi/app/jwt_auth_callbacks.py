# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import uuid
from flask_jwt_extended import JWTManager, get_jwt_identity
from flask_jwt_extended.default_callbacks import (
    default_expired_token_callback,
    default_verify_claims_failed_callback,
    default_invalid_token_callback,
    default_revoked_token_callback,
    default_unauthorized_callback,
    default_user_identity_callback,
)
from quart import current_app, request, Response


def register_logging_callbacks(jwt: JWTManager):
    """
    Register callbacks which emit logs to the app's access log and return
    the result from the default callback.

    Registers callbacks for:
    
    - expired tokens
    - tampered tokens
    - invalid tokens
    - revoked tokens
    - blacklisted tokens
    - unauthorised access

    Parameters
    ----------
    jwt : JWTManager
        JWT manager to register callbacks for

    Returns
    -------
    JWTManager
        The JWT manager wth the registered callbacks

    """

    jwt.expired_token_loader(expired_token_callback)
    jwt.claims_verification_failed_loader(claims_verification_failed_callback)
    jwt.invalid_token_loader(invalid_token_callback)
    jwt.revoked_token_loader(revoked_token_callback)
    jwt.unauthorized_loader(unauthorized_callback)
    return jwt


async def expired_token_callback() -> Response:
    """
    Log that an access attempt was made with an expired token and return
    the result from the default callback.

    Returns
    -------
    Response
    """

    current_app.access_logger.error(
        "EXPIRED_TOKEN",
        route=request.path,
        request_id=request.request_id,
        user=str(get_jwt_identity()),
        src_ip=request.headers.get("Remote-Addr"),
        json_payload=await request.json,
    )

    return default_expired_token_callback()


async def claims_verification_failed_callback() -> Response:
    """
    Log that an access attempt was made with claims that failed verification and return
    the result from the default callback.

    Returns
    -------
    Response
    """
    current_app.access_logger.error(
        "CLAIMS_VERIFICATION_FAILED",
        route=request.path,
        request_id=request.request_id,
        user=str(get_jwt_identity()),
        src_ip=request.headers.get("Remote-Addr"),
        json_payload=await request.json,
    )
    return default_verify_claims_failed_callback()


async def invalid_token_callback(error_string) -> Response:
    """
    Log that an access attempt was made with a token that was invalid and return
    the result from the default callback.

    Parameters
    ----------
    error_string : str
        Reason the token is invalid

    Returns
    -------
    Response

    """
    current_app.access_logger.error(
        "INVALID_TOKEN",
        error_string=error_string,
        route=request.path,
        request_id=request.request_id,
        user=str(get_jwt_identity()),
        src_ip=request.headers.get("Remote-Addr"),
        json_payload=await request.json,
    )
    return default_invalid_token_callback(error_string)


async def revoked_token_callback() -> Response:
    """
    Log that an access attempt was made with a revoked token and return
    the result from the default callback.

    Returns
    -------
    Response
    """
    current_app.access_logger.error(
        "REVOKED_TOKEN",
        route=request.path,
        request_id=request.request_id,
        user=str(get_jwt_identity()),
        src_ip=request.headers.get("Remote-Addr"),
        json_payload=await request.json,
    )
    return default_revoked_token_callback()


async def unauthorized_callback(error_string) -> Response:
    """
    Log that an access attempt was made without a token and return
    the result from the default callback.

    Returns
    -------
    Response
    """
    current_app.access_logger.error(
        "UNAUTHORISED",
        error_string=error_string,
        route=request.path,
        request_id=request.request_id,
        user=str(get_jwt_identity()),
        src_ip=request.headers.get("Remote-Addr"),
        json_payload=await request.json,
    )
    return default_unauthorized_callback(error_string)
