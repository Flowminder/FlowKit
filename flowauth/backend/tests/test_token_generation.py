# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from os import environ

import datetime

import pytest
import jwt
from flowauth.token_management import generate_token as flowauth_generate_token
from flowkit_jwt_generator import generate_token as jwt_generator_generate_token

from pytest import approx


@pytest.mark.usefixtures("test_data")
def test_reject_when_claim_not_allowed(client, auth, test_user):
    uid, uname, upass = test_user
    # Log in first
    response, csrf_cookie = auth.login(uname, upass)
    expiry = (datetime.datetime.now() + datetime.timedelta(minutes=2)).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    token_eq = {"name": "TEST_TOKEN", "expiry": expiry, "claims": ["run:DUMMY_ROUTE_A"]}
    response = client.post(
        "/tokens/tokens/1", headers={"X-CSRF-Token": csrf_cookie}, json=token_eq
    )
    assert 401 == response.status_code
    assert (
        b"You do not have access to run:DUMMY_ROUTE_A on DUMMY_SERVER_A"
        in response.get_data()
    )


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_token_generation(client, auth, app, test_user, public_key):
    # Log in first
    uid, uname, upass = test_user
    response, csrf_cookie = auth.login(uname, upass)
    expiry = datetime.datetime.now() + datetime.timedelta(minutes=2)
    token_eq = {
        "name": "DUMMY_TOKEN",
        "expiry": expiry.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "claims": [
            "run:DUMMY_ROUTE_A:aggregation_unit:admin0",
            "get_result:DUMMY_ROUTE_A:aggregation_unit:admin0",
        ],
    }
    response = client.post(
        "/tokens/tokens/1", headers={"X-CSRF-Token": csrf_cookie}, json=token_eq
    )
    assert 200 == response.status_code
    token_json = response.get_json()
    decoded_token = jwt.decode(
        jwt=token_json["token"].encode(),
        key=public_key,
        algorithms=["RS256"],
        audience="DUMMY_SERVER_A",
    )
    assert ["get_result,run:DUMMY_ROUTE_A:aggregation_unit:admin0"] == decoded_token[
        "user_claims"
    ]
    assert "TEST_USER" == decoded_token["identity"]
    assert approx(expiry.timestamp()) == decoded_token["exp"]


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_token_rejected_for_expiry(client, auth, app, test_user):

    # Log in first
    uid, uname, upass = test_user
    response, csrf_cookie = auth.login(uname, upass)
    expiry = datetime.datetime.now() + datetime.timedelta(minutes=10)
    token_eq = {
        "name": "DUMMY_TOKEN",
        "expiry": expiry.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "claims": ["run:DUMMY_ROUTE_A:aggregation_unit:admin0"],
    }
    response = client.post(
        "/tokens/tokens/1", headers={"X-CSRF-Token": csrf_cookie}, json=token_eq
    )
    assert 400 == response.status_code
    assert {
        "code": 400,
        "message": "Token lifetime too long",
        "bad_field": "expiry",
    } == response.get_json()


def test_against_general_generator(app, public_key):
    """Test that the token generator in FlowAuth and the one in flowkit-jwt-generator produce same results."""
    flowauth_token = jwt.decode(
        flowauth_generate_token(
            username="TEST_USER",
            private_key=app.config["PRIVATE_JWT_SIGNING_KEY"],
            lifetime=datetime.timedelta(5),
            flowapi_identifier="TEST_SERVER",
            claims={
                "daily_location": {
                    "permissions": {"run": True},
                    "spatial_aggregation": ["admin3"],
                }
            },
        ),
        key=public_key,
        audience="TEST_SERVER",
    )
    generator_token = jwt.decode(
        jwt_generator_generate_token(
            username="TEST_USER",
            private_key=app.config["PRIVATE_JWT_SIGNING_KEY"],
            lifetime=datetime.timedelta(5),
            flowapi_identifier="TEST_SERVER",
            claims={
                "daily_location": {
                    "permissions": {"run": True},
                    "spatial_aggregation": ["admin3"],
                }
            },
        ),
        key=public_key,
        audience="TEST_SERVER",
    )
    assert generator_token["aud"] == flowauth_token["aud"]
    assert generator_token["user_claims"] == flowauth_token["user_claims"]
    assert generator_token["identity"] == flowauth_token["identity"]
    assert generator_token.keys() == flowauth_token.keys()
