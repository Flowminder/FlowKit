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
        runner_role, reader_role, reader_b_role = test_roles

        # Give test_user some roles on DUMMY_SERVER_A
        test_user = db.session.execute(db.select(User).where(User.id == uid)).scalar()
        test_user.roles.append(reader_role)

        # Log in first
        response, csrf_cookie = auth.login(uname, upass)
        token_req = {
            "name": "TEST_TOKEN",
            "roles": [{"name": "runner"}],
        }
        response = client.post(
            "/tokens/tokens/1", headers={"X-CSRF-Token": csrf_cookie}, json=token_req
        )
        assert 401 == response.status_code
        assert (
            "Role 'runner' is not permitted for the current user"
            == response.get_json()["message"]
        )

        token_req = {"name": "TEST_TOKEN", "roles": [{"name": "reader"}]}
        # Testing attempting reader on second server
        response = client.post(
            "/tokens/tokens/2", headers={"X-CSRF-Token": csrf_cookie}, json=token_req
        )
        assert 401 == response.status_code


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

        # Expiry from token roles
        expiry = datetime.datetime(year=2020, month=12, day=31) + datetime.timedelta(
            minutes=5
        )
        token_req = {
            "name": "DUMMY_TOKEN",
            "roles": [{"name": "runner"}, {"name": "reader"}],
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
        assert decompress_claims(decoded_token["user_claims"]) == {
            "reader": ["get_result"],
            "runner": ["dummy_agg_unit:dummy_query:dummy_query", "get_result", "run"],
        }

        assert "TEST_USER" == decoded_token["sub"]
        assert approx(expiry.timestamp()) == decoded_token["exp"]


@pytest.mark.usefixtures("test_data_with_access_rights")
@freeze_time(datetime.datetime(year=2020, month=12, day=31))
def test_token_list_includes_assigned_roles(
    client, auth, app, test_user_with_roles, test_servers
):
    """The token list endpoint exposes the roles that were assigned at mint time."""
    with app.app_context():
        uid, uname, upass = test_user_with_roles
        response, csrf_cookie = auth.login(uname, upass)
        assert response.status_code == 200

        token_req = {
            "name": "DUMMY_TOKEN",
            "roles": [{"name": "runner"}, {"name": "reader"}],
        }
        mint_response = client.post(
            "/tokens/tokens/1", headers={"X-CSRF-Token": csrf_cookie}, json=token_req
        )
        assert mint_response.status_code == 200

        list_response = client.get(
            "/tokens/tokens/1", headers={"X-CSRF-Token": csrf_cookie}
        )
        assert list_response.status_code == 200
        tokens = list_response.get_json()
        assert len(tokens) == 1
        assert tokens[0]["name"] == "DUMMY_TOKEN"
        role_names = sorted(role["name"] for role in tokens[0]["roles"])
        assert role_names == ["reader", "runner"]
        for role in tokens[0]["roles"]:
            assert isinstance(role["id"], int)


@pytest.mark.usefixtures("test_data_with_access_rights")
@freeze_time(datetime.datetime(year=2020, month=12, day=31))
def test_token_renewal_creates_new_row_with_same_name_and_roles(
    client, auth, app, test_user_with_roles, public_key
):
    """Renewing a token mints a fresh JWT with the same name and roles, and
    leaves the original TokenHistory row alone."""
    with app.app_context():
        uid, uname, upass = test_user_with_roles
        response, csrf_cookie = auth.login(uname, upass)

        mint = client.post(
            "/tokens/tokens/1",
            headers={"X-CSRF-Token": csrf_cookie},
            json={
                "name": "DUMMY_TOKEN",
                "roles": [{"name": "runner"}, {"name": "reader"}],
            },
        )
        assert mint.status_code == 200
        original_token_string = mint.get_json()["token"]

        listed = client.get(
            "/tokens/tokens/1", headers={"X-CSRF-Token": csrf_cookie}
        ).get_json()
        original_id = listed[0]["id"]

        renew = client.post(
            f"/tokens/tokens/{original_id}/renew",
            headers={"X-CSRF-Token": csrf_cookie},
            json={},
        )
        assert renew.status_code == 200
        renewed = renew.get_json()
        assert "token" in renewed
        assert renewed["token"] != original_token_string
        assert renewed["id"] != original_id

        listed_after = client.get(
            "/tokens/tokens/1", headers={"X-CSRF-Token": csrf_cookie}
        ).get_json()
        assert len(listed_after) == 2
        for entry in listed_after:
            assert entry["name"] == "DUMMY_TOKEN"
            assert sorted(r["name"] for r in entry["roles"]) == ["reader", "runner"]


@pytest.mark.usefixtures("test_data_with_access_rights")
@freeze_time(datetime.datetime(year=2020, month=12, day=31))
def test_token_renewal_rejects_other_users_tokens(
    client, auth, app, test_user_with_roles, test_admin
):
    """A user cannot renew a token they don't own."""
    with app.app_context():
        uid, uname, upass = test_user_with_roles
        _, csrf_cookie = auth.login(uname, upass)
        client.post(
            "/tokens/tokens/1",
            headers={"X-CSRF-Token": csrf_cookie},
            json={"name": "DUMMY_TOKEN", "roles": [{"name": "reader"}]},
        )
        other_id = client.get(
            "/tokens/tokens/1", headers={"X-CSRF-Token": csrf_cookie}
        ).get_json()[0]["id"]

        auth.logout()
        admin_id, admin_name, admin_pw = test_admin
        _, admin_csrf = auth.login(admin_name, admin_pw)
        renew = client.post(
            f"/tokens/tokens/{other_id}/renew",
            headers={"X-CSRF-Token": admin_csrf},
            json={},
        )
        assert renew.status_code == 401


@pytest.mark.usefixtures("test_data_with_access_rights")
@freeze_time(datetime.datetime(year=2020, month=12, day=31))
def test_token_renewal_rejects_when_role_revoked(
    client, auth, app, test_user_with_roles
):
    """If a role has been removed from the user since the token was minted,
    renewal must fail rather than silently issuing a token they can no longer
    legitimately request."""
    with app.app_context():
        uid, uname, upass = test_user_with_roles
        _, csrf_cookie = auth.login(uname, upass)
        client.post(
            "/tokens/tokens/1",
            headers={"X-CSRF-Token": csrf_cookie},
            json={
                "name": "DUMMY_TOKEN",
                "roles": [{"name": "runner"}, {"name": "reader"}],
            },
        )
        token_id = client.get(
            "/tokens/tokens/1", headers={"X-CSRF-Token": csrf_cookie}
        ).get_json()[0]["id"]

        from flowauth.models import db, User, Role

        user = db.session.execute(db.select(User).where(User.id == uid)).scalar()
        runner = db.session.execute(
            db.select(Role).where(Role.name == "runner")
        ).scalar()
        user.roles.remove(runner)
        db.session.commit()

        renew = client.post(
            f"/tokens/tokens/{token_id}/renew",
            headers={"X-CSRF-Token": csrf_cookie},
            json={},
        )
        assert renew.status_code == 401
        assert "runner" in renew.get_json()["message"]


@pytest.mark.usefixtures("test_data_with_access_rights")
@freeze_time(datetime.datetime(year=2020, month=12, day=31))
def test_token_honours_requested_lifetime(
    client, auth, app, test_user_with_roles, public_key
):
    """A user-supplied lifetime_minutes shorter than the cap is honoured."""
    with app.app_context():
        uid, uname, upass = test_user_with_roles
        response, csrf_cookie = auth.login(uname, upass)

        token_req = {
            "name": "DUMMY_TOKEN",
            "roles": [{"name": "reader"}],
            "lifetime_minutes": 2,
        }
        response = client.post(
            "/tokens/tokens/1", headers={"X-CSRF-Token": csrf_cookie}, json=token_req
        )
        assert response.status_code == 200

        decoded = jwt.decode(
            jwt=response.get_json()["token"].encode(),
            key=public_key,
            algorithms=["RS256"],
            audience="DUMMY_SERVER_A",
        )
        expected_expiry = datetime.datetime(
            year=2020, month=12, day=31
        ) + datetime.timedelta(minutes=2)
        assert approx(expected_expiry.timestamp()) == decoded["exp"]


@pytest.mark.usefixtures("test_data_with_access_rights")
@freeze_time(datetime.datetime(year=2020, month=12, day=31))
def test_token_rejects_lifetime_above_cap(client, auth, app, test_user_with_roles):
    """A lifetime longer than the role/server cap is rejected."""
    with app.app_context():
        uid, uname, upass = test_user_with_roles
        response, csrf_cookie = auth.login(uname, upass)

        # Cap is 5 minutes (set in test_roles fixture); ask for 10.
        token_req = {
            "name": "DUMMY_TOKEN",
            "roles": [{"name": "reader"}],
            "lifetime_minutes": 10,
        }
        response = client.post(
            "/tokens/tokens/1", headers={"X-CSRF-Token": csrf_cookie}, json=token_req
        )
        assert response.status_code == 400
        assert response.get_json()["bad_field"] == "lifetime_minutes"


@pytest.mark.usefixtures("test_data_with_access_rights")
@freeze_time(datetime.datetime(year=2020, month=12, day=31))
def test_token_mint_with_no_absolute_caps(
    client, auth, app, test_user_with_roles, public_key, test_servers, test_roles
):
    """When server and roles have NULL latest_token_expiry, mint succeeds and
    the token's expiry is bounded only by longest_token_life_minutes."""
    from flowauth.models import db, Server, Role

    with app.app_context():
        server_a = db.session.get(Server, 1)
        server_a.latest_token_expiry = None
        for role in db.session.execute(
            db.select(Role).where(Role.server_id == 1)
        ).scalars():
            role.latest_token_expiry = None
        db.session.commit()

        uid, uname, upass = test_user_with_roles
        response, csrf_cookie = auth.login(uname, upass)

        token_req = {
            "name": "DUMMY_TOKEN",
            "roles": [{"name": "reader"}],
        }
        response = client.post(
            "/tokens/tokens/1", headers={"X-CSRF-Token": csrf_cookie}, json=token_req
        )
        assert response.status_code == 200
        decoded = jwt.decode(
            jwt=response.get_json()["token"].encode(),
            key=public_key,
            algorithms=["RS256"],
            audience="DUMMY_SERVER_A",
        )
        # longest_token_life_minutes is 24 * 60 * 2 = 2880
        expected_expiry = datetime.datetime(
            year=2020, month=12, day=31
        ) + datetime.timedelta(minutes=24 * 60 * 2)
        assert approx(expected_expiry.timestamp()) == decoded["exp"]


def test_token_rejected_for_expiry(client, auth, app, test_user_with_roles, public_key):
    with app.app_context():
        with freeze_time("2020-12-31") as frozentime:
            # Log in first
            uid, uname, upass = test_user_with_roles
            response, csrf_cookie = auth.login(uname, upass)
            print(csrf_cookie)
            token_eq = {
                "name": "DUMMY_TOKEN",
                "roles": [{"name": "reader"}],
            }
            response = client.post(
                "/tokens/tokens/1", headers={"X-CSRF-Token": csrf_cookie}, json=token_eq
            )
            assert 200 == response.status_code

            frozentime.tick(datetime.timedelta(minutes=10))
            # Re-login to avoid the csrf cookie timing out
            login_response, new_csrf_cookie = auth.login(uname, upass)
            assert login_response.status_code == 200
            print(new_csrf_cookie)
            bad_response = client.post(
                "/tokens/tokens/1",
                headers={"X-CSRF-Token": new_csrf_cookie},
                json=token_eq,
            )
            assert bad_response.status_code == 401
            # Should this be a jwt-specific error?
            assert {
                "code": 401,
                "message": "Token for TEST_USER expired",
            } == bad_response.json
