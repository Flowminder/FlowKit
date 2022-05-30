# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import datetime
from os import environ

import jwt

import pytest
from flowauth.jwt import decompress_claims
from pytest import approx
from freezegun import freeze_time


@pytest.mark.usefixtures("test_data")
@freeze_time(datetime.datetime(year=2020, month=12, day=31))
def test_reject_when_claim_not_allowed(client, auth, test_user):
    uid, uname, upass = test_user
    # Log in first
    response, csrf_cookie = auth.login(uname, upass)
    expiry = (datetime.datetime.now() + datetime.timedelta(minutes=2)).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    token_eq = {"name": "TEST_TOKEN", "expiry": expiry, "claims": ["run&DUMMY_ROUTE_A"]}
    response = client.post(
        "/tokens/tokens/1", headers={"X-CSRF-Token": csrf_cookie}, json=token_eq
    )
    assert 401 == response.status_code
    assert (
        b"You do not have access to run&DUMMY_ROUTE_A on DUMMY_SERVER_A"
        in response.get_data()
    )


@pytest.mark.usefixtures("test_data_with_access_rights")
@freeze_time(datetime.datetime(year=2020, month=12, day=31))
def test_token_generation(
    client, auth, app, test_user_with_roles, public_key, test_servers
):

    # Log in first
    uid, uname, upass = test_user_with_roles
    response, csrf_cookie = auth.login(uname, upass)
    assert response.status_code == 200

    expiry = datetime.datetime(year=2020, month=12, day=31) + datetime.timedelta(
        minutes=2
    )
    token_req = {
        "name": "DUMMY_TOKEN",
        "expiry": expiry.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "claims": [
            "run",
            "get_result",
            "dummy_query:admin_level_1",
        ],
    }
    response = client.post(
        "/tokens/tokens/1", headers={"X-CSRF-Token": csrf_cookie}, json=token_req
    )
    assert response.status_code == 200

    token_json = response.get_json()
    decoded_token = jwt.decode(
        jwt=token_json["token"].encode(),
        key=public_key,
        algorithms=["RS256"],
        audience="DUMMY_SERVER_A",
    )
    assert decompress_claims(decoded_token["scopes"]) == [
        "dummy_query:admin_level_1,get_result,run"
    ]
    assert "TEST_USER" == decoded_token["sub"]
    assert approx(expiry.timestamp()) == decoded_token["exp"]


@pytest.mark.usefixtures("test_data_with_access_rights")
@freeze_time(datetime.datetime(year=2020, month=12, day=31))
def test_token_rejected_for_expiry(client, auth, app, test_user):

    # Log in first
    uid, uname, upass = test_user
    response, csrf_cookie = auth.login(uname, upass)
    expiry = datetime.datetime.now() + datetime.timedelta(minutes=10)
    token_eq = {
        "name": "DUMMY_TOKEN",
        "expiry": expiry.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "claims": ["run&DUMMY_ROUTE_A.aggregation_unit.admin0"],
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
