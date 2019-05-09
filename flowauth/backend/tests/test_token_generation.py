# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime

import jwt
import pytest
from flowauth.token_management import generate_token as flowauth_generate_token
from flowkit_jwt_generator import generate_token as jwt_generator_generate_token

from pytest import approx


@pytest.mark.usefixtures("test_data")
def test_reject_when_claim_not_allowed(client, auth):
    # Log in first
    response, csrf_cookie = auth.login("TEST_USER", "DUMMY_PASSWORD")
    expiry = (datetime.datetime.now() + datetime.timedelta(minutes=2)).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    token_eq = {
        "name": "TEST_TOKEN",
        "expiry": expiry,
        "claims": {
            "DUMMY_ROUTE_A": {
                "permissions": {"get_data": True, "run": True},
                "spatial_aggregation": ["admin0"],
            }
        },
    }
    response = client.post(
        "/user/tokens/1", headers={"X-CSRF-Token": csrf_cookie}, json=token_eq
    )
    assert 401 == response.status_code
    assert (
        b"You do not have access to DUMMY_ROUTE_A on DUMMY_SERVER_A"
        in response.get_data()
    )


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_token_generation(client, auth, app):

    # Log in first
    response, csrf_cookie = auth.login("TEST_USER", "DUMMY_PASSWORD")
    expiry = datetime.datetime.now() + datetime.timedelta(minutes=2)
    token_eq = {
        "name": "DUMMY_TOKEN",
        "expiry": expiry.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "claims": {
            "DUMMY_ROUTE_A": {
                "permissions": {"get_result": True, "run": True},
                "spatial_aggregation": ["admin0"],
            }
        },
    }
    response = client.post(
        "/user/tokens/1", headers={"X-CSRF-Token": csrf_cookie}, json=token_eq
    )
    assert 200 == response.status_code
    token_json = response.get_json()
    decoded_token = jwt.decode(
        token_json["token"].encode(),
        "DUMMY_SERVER_A_KEY",
        algorithms=["HS256"],
        audience="DUMMY_SERVER_A",
    )
    assert {
        "DUMMY_ROUTE_A": {
            "permissions": {"get_result": True, "run": True},
            "spatial_aggregation": ["admin0"],
        }
    } == decoded_token["user_claims"]
    assert "TEST_USER" == decoded_token["identity"]
    assert approx(expiry.timestamp()) == decoded_token["exp"]


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_token_rejected_for_expiry(client, auth, app):

    # Log in first
    response, csrf_cookie = auth.login("TEST_USER", "DUMMY_PASSWORD")
    expiry = datetime.datetime.now() + datetime.timedelta(minutes=10)
    token_eq = {
        "name": "DUMMY_TOKEN",
        "expiry": expiry.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "claims": {
            "DUMMY_ROUTE_A": {
                "permissions": {"get_result": True, "run": True},
                "spatial_aggregation": ["admin0"],
            }
        },
    }
    response = client.post(
        "/user/tokens/1", headers={"X-CSRF-Token": csrf_cookie}, json=token_eq
    )
    assert 400 == response.status_code
    assert {
        "code": 400,
        "message": "Token lifetime too long",
        "bad_field": "expiry",
    } == response.get_json()


@pytest.mark.usefixtures("test_data_with_access_rights")
def test_token_rejected_for_bad_right_claim(client, auth, app):

    # Log in first
    response, csrf_cookie = auth.login("TEST_USER", "DUMMY_PASSWORD")
    expiry = datetime.datetime.now() + datetime.timedelta(minutes=1)
    token_eq = {
        "name": "DUMMY_TOKEN",
        "expiry": expiry.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "claims": {
            "DUMMY_ROUTE_A": {
                "permissions": {"get_result": True, "run": True, "poll": True},
                "spatial_aggregation": ["admin0"],
            }
        },
    }
    response = client.post(
        "/user/tokens/1", headers={"X-CSRF-Token": csrf_cookie}, json=token_eq
    )
    assert 401 == response.status_code
    assert (
        b"You do not have access to DUMMY_ROUTE_A:poll on DUMMY_SERVER_A"
        in response.get_data()
    )


def test_against_general_generator():
    """Test that the token generator in FlowAuth and the one in flowkit-jwt-generator produce same results."""
    assert flowauth_generate_token(
        username="TEST_USER",
        secret="SECRET",
        lifetime=datetime.timedelta(5),
        audience="TEST_SERVER",
        claims={
            "daily_location": {
                "permissions": {"run": True},
                "spatial_aggregation": ["admin3"],
            }
        },
    ) == jwt_generator_generate_token(
        username="TEST_USER",
        secret="SECRET",
        lifetime=datetime.timedelta(5),
        audience="TEST_SERVER",
        claims={
            "daily_location": {
                "permissions": {"run": True},
                "spatial_aggregation": ["admin3"],
            }
        },
    )
