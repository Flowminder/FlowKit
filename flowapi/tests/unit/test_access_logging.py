# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import json
import os

import pytest
from quart import request

from flowapi.jwt_auth_callbacks import (
    invalid_token_callback,
    expired_token_callback,
    claims_verification_failed_callback,
    revoked_token_callback,
)


@pytest.mark.asyncio
async def test_invalid_token(app):
    """
    Test that invalid tokens are logged correctly.

    Parameters
    ----------
    app: tuple
        Pytest fixture providing the flowapi, with a mock for the db
    """
    client, db, log_dir, app = app
    await client.get("/")  # Need to trigger setup

    async with app.test_request_context("GET", "/"):
        request.request_id = "DUMMY_REQUEST_ID"
        await invalid_token_callback("DUMMY_ERROR_STRING")
        with open(os.path.join(log_dir, "flowkit-access.log")) as log_file:
            log_lines = log_file.readlines()
        assert len(log_lines) == 1
        assert json.loads(log_lines[0])["event"] == "INVALID_TOKEN"
        assert json.loads(log_lines[0])["request_id"] == "DUMMY_REQUEST_ID"


@pytest.mark.asyncio
async def test_expired_token(app):
    """
    Test that expired tokens are logged correctly.

    Parameters
    ----------
    app: tuple
        Pytest fixture providing the flowapi, with a mock for the db
    """
    client, db, log_dir, app = app
    await client.get("/")  # Need to trigger setup

    # As of v3.16.0, flask-jwt-extended passes the decoded expired token to the callback
    # (see https://github.com/vimalloc/flask-jwt-extended/releases/tag/3.16.0), so we
    # construct a dummy JSON object here that has the same structure. The details of this
    # structure may or may not be used internally but it can't hurt to have a realistic example.
    dummy_decoded_expired_token = {
        "iat": 1548061881,
        "nbf": 1548061881,
        "jti": "bada4e8a-bf33-4b2f-b02d-88a2c5fad180",
        "exp": 1548061920,
        "identity": "TEST_USER",
        "fresh": True,
        "type": "access",
        "user_claims": {},
    }

    async with app.test_request_context("GET", "/"):
        request.request_id = "DUMMY_REQUEST_ID"
        await expired_token_callback(dummy_decoded_expired_token)
        with open(os.path.join(log_dir, "flowkit-access.log")) as log_file:
            log_lines = log_file.readlines()
        assert len(log_lines) == 1
        assert json.loads(log_lines[0])["event"] == "EXPIRED_TOKEN"
        assert json.loads(log_lines[0])["request_id"] == "DUMMY_REQUEST_ID"


@pytest.mark.asyncio
async def test_claims_verify_fail(app):
    """
    Test that failure to verify claims is logged.

    Parameters
    ----------
    app: tuple
        Pytest fixture providing the flowapi, with a mock for the db
    """
    client, db, log_dir, app = app
    await client.get("/")  # Need to trigger setup

    async with app.test_request_context("GET", "/"):
        request.request_id = "DUMMY_REQUEST_ID"
        await claims_verification_failed_callback()
        with open(os.path.join(log_dir, "flowkit-access.log")) as log_file:
            log_lines = log_file.readlines()
        assert len(log_lines) == 1
        assert json.loads(log_lines[0])["event"] == "CLAIMS_VERIFICATION_FAILED"
        assert json.loads(log_lines[0])["request_id"] == "DUMMY_REQUEST_ID"


@pytest.mark.asyncio
async def test_revoked_token(app):
    """
    Test that revoked tokens are logged.

    Parameters
    ----------
    app: tuple
        Pytest fixture providing the flowapi, with a mock for the db
    """
    client, db, log_dir, app = app
    await client.get("/")  # Need to trigger setup

    async with app.test_request_context("GET", "/"):
        request.request_id = "DUMMY_REQUEST_ID"
        await revoked_token_callback()
        with open(os.path.join(log_dir, "flowkit-access.log")) as log_file:
            log_lines = log_file.readlines()
        assert len(log_lines) == 1
        assert json.loads(log_lines[0])["event"] == "REVOKED_TOKEN"
        assert json.loads(log_lines[0])["request_id"] == "DUMMY_REQUEST_ID"
