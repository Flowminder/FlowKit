# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from time import sleep

from functools import partial

import datetime
from collections import namedtuple
from itertools import product

import pyotp
import pytest
from flowauth.main import create_app
from flowauth.models import (
    Group,
    GroupServerPermission,
    GroupServerTokenLimits,
    Server,
    ServerCapability,
    Token,
    TwoFactorAuth,
    TwoFactorBackup,
    User,
    db,
)
from flowauth.user_settings import generate_backup_codes, md5

TestUser = namedtuple("TestUser", ["id", "username", "password"])
TestTwoFactorUser = namedtuple(
    "TestTwoFactorUser", TestUser._fields + ("otp_generator", "backup_codes")
)
TestGroup = namedtuple("TestGroup", ["id", "name"])


@pytest.fixture
def app(tmpdir):
    """Per test app"""
    db_path = tmpdir / "db.db"

    app = create_app(
        {
            "TESTING": True,
            "SQLALCHEMY_DATABASE_URI": f"sqlite:///{db_path}",
            "FLOWAUTH_ADMIN_USERNAME": "TEST_ADMIN",
            "FLOWAUTH_ADMIN_PASSWORD": "DUMMY_PASSWORD",
            "DEMO_MODE": False,
        }
    )
    with app.app_context():
        app.test_client().get("/")
    yield app


@pytest.fixture
def client(app):
    """A test client for the app."""
    return app.test_client()


class AuthActions(object):
    def __init__(self, client):
        self._client = client

    def login(self, username="test", password="test"):
        response = self._client.post(
            "signin", json={"username": username, "password": password}
        )
        cookies = response.headers.get_all("Set-Cookie")
        csrf_cookie = [c for c in cookies if c.startswith("X-CSRF")][0][7:-8]
        return response, csrf_cookie

    def two_factor_login(self, *, otp_code, username="test", password="test"):
        response = self._client.post(
            "signin",
            json={
                "username": username,
                "password": password,
                "two_factor_code": otp_code,
            },
        )
        cookies = response.headers.get_all("Set-Cookie")
        csrf_cookie = [c for c in cookies if c.startswith("X-CSRF")][0][7:-8]
        return response, csrf_cookie

    def logout(self):
        return self._client.get("signout")


@pytest.fixture
def test_user(app):
    with app.app_context():
        user = User(username="TEST_USER", password="TEST_USER_PASSWORD")
        ug = Group(name="TEST_USER", user_group=True, members=[user])
        db.session.add(user)
        db.session.add(ug)
        db.session.commit()
        return TestUser(user.id, user.username, "TEST_USER_PASSWORD")


def get_two_factor_code(secret):
    totp = pyotp.totp.TOTP(secret)
    time_remaining = totp.interval - datetime.datetime.now().timestamp() % totp.interval
    if time_remaining < 1:
        sleep(2)
    return totp.now()


@pytest.fixture
def test_two_factor_auth_user(app):
    with app.app_context():
        user = User(username="TEST_FACTOR_USER", password="TEST_USER_PASSWORD")
        ug = Group(name="TEST_FACTOR_USER", user_group=True, members=[user])
        secret = pyotp.random_base32()
        auth = TwoFactorAuth(user=user, enabled=True)
        auth.secret_key = secret
        otp_generator = partial(get_two_factor_code, secret)
        db.session.add(user)
        db.session.add(auth)
        db.session.add(ug)
        db.session.commit()
        backup_codes = generate_backup_codes()
        for code in backup_codes:
            backup = TwoFactorBackup(auth_id=auth.user_id)
            backup.backup_code = code
            db.session.add(backup)
        db.session.commit()
        return TestTwoFactorUser(
            user.id, user.username, "TEST_USER_PASSWORD", otp_generator, backup_codes
        )


@pytest.fixture
def test_admin(app):
    with app.app_context():
        user = User.query.filter(User.username == app.config["ADMIN_USER"]).first()

        return TestUser(user.id, user.username, app.config["ADMIN_PASSWORD"])


@pytest.fixture
def test_group(app):
    with app.app_context():
        group = Group(name="TEST_GROUP")
        db.session.add(group)
        db.session.commit()
        return TestGroup(group.id, group.name)


@pytest.fixture
def test_data(app, test_admin, test_user, test_group):
    with app.app_context():

        test_group = Group.query.filter(Group.id == test_group.id).first()
        # Each user is also a group
        for user in User.query.all():
            test_group.members.append(user)
        db.session.add(test_group)

        # Add some servers
        dummy_server_a = Server(
            name="DUMMY_SERVER_A",
            longest_token_life=2,
            latest_token_expiry=datetime.datetime.now().date()
            + datetime.timedelta(days=365),
        )
        dummy_server_b = Server(
            name="DUMMY_SERVER_B",
            longest_token_life=2,
            latest_token_expiry=datetime.datetime.now().date()
            + datetime.timedelta(days=700),
        )
        db.session.add(dummy_server_a)
        db.session.add(dummy_server_b)

        # Add some things that you can do on the servers
        scs = []
        for action, cap, admin_unit in product(
            ("get_result", "run"),
            ("DUMMY_ROUTE_A", "DUMMY_ROUTE_B"),
            (f"admin{x}" for x in range(4)),
        ):
            cap = f"{action}&{cap}.aggregation_unit.{admin_unit}"
            sc_a = ServerCapability(
                capability=cap,
                server=dummy_server_a,
                capability_hash=md5(cap.encode()).hexdigest(),
                enabled=True,
            )
            scs.append(sc_a)
            db.session.add(sc_a)
            sc_b = ServerCapability(
                capability=cap,
                server=dummy_server_b,
                capability_hash=md5(cap.encode()).hexdigest(),
                enabled=True,
            )
            scs.append(sc_b)
            db.session.add(sc_b)
        # Give test user group permissions on Haiti
        for sc in dummy_server_a.capabilities:
            gsp = GroupServerPermission(group=test_group, server_capability=sc)
            db.session.add(gsp)
        db.session.add(
            GroupServerTokenLimits(
                group=test_group,
                longest_life=2,
                latest_end=datetime.datetime.now().date()
                + datetime.timedelta(days=365),
                server=dummy_server_a,
            )
        )

        # Give test admin a token on server b

        t = Token(
            name="DUMMY_TOKEN",
            token="DUMMY_TOKEN_STRING",
            expires=datetime.datetime.now().date() + datetime.timedelta(days=1),
            owner=User.query.all()[0],
            server=dummy_server_b,
        )
        db.session.add(t)
        db.session.commit()


@pytest.fixture
def auth(client):
    return AuthActions(client)


@pytest.fixture
def test_data_with_access_rights(app, test_data, test_group):
    with app.app_context():
        yield
