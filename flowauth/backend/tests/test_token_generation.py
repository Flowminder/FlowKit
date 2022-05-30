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

from flowauth.models import db, User


@freeze_time(datetime.datetime(year=2020, month=12, day=31))
def test_reject_when_claim_not_allowed(
    client, auth, app, test_user, test_roles, test_scopes
):
    with app.app_context():
        uid, uname, upass = test_user
        runner_role, reader_role = test_roles

        # Give test_user some roles on DUMMY_SERVER_A
        test_user = db.session.execute(db.select(User).where(User.id == uid)).scalar()
        test_user.roles.append(reader_role)

        # Log in first
        response, csrf_cookie = auth.login(uname, upass)
        expiry = (datetime.datetime.now() + datetime.timedelta(minutes=2)).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        token_eq = {
            "name": "TEST_TOKEN",
            "expiry": expiry,
            "claims": ["run", "dummy_query:admin_level_1"],
        }
        response = client.post(
            "/tokens/tokens/1", headers={"X-CSRF-Token": csrf_cookie}, json=token_eq
        )
        assert 401 == response.status_code
        assert (
            b"No roles for TEST_USER permit the requested scopes."
            in response.get_data()
        )


@pytest.mark.usefixtures("test_data_with_access_rights")
@freeze_time(datetime.datetime(year=2020, month=12, day=31))
def test_token_generation(
    client, auth, app, test_user_with_roles, public_key, test_servers
):
    with app.app_context():
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


# @freeze_time("2020-12-31")
def test_token_rejected_for_expiry(client, auth, app, test_user_with_roles, public_key):
    with app.app_context():
        with freeze_time("2020-12-31") as frozentime:
            # Log in first
            uid, uname, upass = test_user_with_roles
            response, csrf_cookie = auth.login(uname, upass)
            expiry = datetime.datetime.now() + datetime.timedelta(minutes=10)
            token_eq = {
                "name": "DUMMY_TOKEN",
                "expiry": expiry.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "claims": ["run"],
            }
            response = client.post(
                "/tokens/tokens/1", headers={"X-CSRF-Token": csrf_cookie}, json=token_eq
            )
            assert 200 == response.status_code

            frozentime.tick(datetime.timedelta(minutes=20))

            response = client.post(
                "/tokens/tokens/1", headers={"X-CSRF-Token": csrf_cookie}, json=token_eq
            )
            assert response.status_code == 401
            # Should this be a jwt-specific error?
            assert {
                "code": 401,
                "message": "Token for TEST_USER expired",
            } == response.json
