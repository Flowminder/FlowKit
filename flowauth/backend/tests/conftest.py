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
    Server,
    TwoFactorAuth,
    TwoFactorBackup,
    User,
    db,
    Scope,
    Role,
)
from flowauth.user_settings import generate_backup_codes, md5

TestUser = namedtuple("TestUser", ["id", "username", "password"])
TestTwoFactorUser = namedtuple(
    "TestTwoFactorUser", TestUser._fields + ("otp_generator", "backup_codes")
)


@pytest.fixture
def test_date(monkeypatch):

    # Adapted from https://stackoverflow.com/questions/20503373/how-to-monkeypatch-pythons-datetime-datetime-now-with-py-test /
    TEST_DATE = datetime.datetime(year=2020, month=12, day=31)

    class test_datetime(datetime.datetime):
        @classmethod
        def now(cls, *args, **kwargs):
            return TEST_DATE

        @classmethod
        def utcnow(cls, *args, **kwargs):
            return cls.now()

    monkeypatch.setattr(datetime, "datetime", test_datetime)
    return TEST_DATE


def test_test_date_fixture(test_date):
    assert datetime.datetime.now() == test_date


@pytest.fixture
def app(tmpdir, test_date):
    """Per test app"""
    db_path = tmpdir / "db.db"
    print(f"DB path: {db_path}")

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
        db.session.add(user)
        db.session.commit()
        return TestUser(user.id, user.username, "TEST_USER_PASSWORD")


@pytest.fixture
def get_two_factor_code():
    def now(secret):
        totp = pyotp.totp.TOTP(secret)
        time_remaining = (
            totp.interval - datetime.datetime.now().timestamp() % totp.interval
        )
        if time_remaining < 1:
            sleep(2)
        return totp.now()

    return now


@pytest.fixture
def test_two_factor_auth_user(app, get_two_factor_code):
    with app.app_context():
        user = User(username="TEST_FACTOR_USER", password="TEST_USER_PASSWORD")
        secret = pyotp.random_base32()
        auth = TwoFactorAuth(user=user, enabled=True)
        auth.secret_key = secret
        otp_generator = partial(get_two_factor_code, secret)
        db.session.add(user)
        db.session.add(auth)
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


@pytest.fixture  # (scope="session")
def test_servers(app):
    with app.app_context():
        # Add some servers
        dummy_server_a = Server(
            name="DUMMY_SERVER_A",
            longest_token_life_minutes=2880,
            latest_token_expiry=datetime.datetime.now().date()
            + datetime.timedelta(days=365),
        )
        dummy_server_b = Server(
            name="DUMMY_SERVER_B",
            longest_token_life_minutes=2880,
            latest_token_expiry=datetime.datetime.now().date()
            + datetime.timedelta(days=700),
        )
        db.session.add(dummy_server_a)
        db.session.add(dummy_server_b)
        db.session.commit()
        return (dummy_server_a, dummy_server_b)


@pytest.fixture  # (scope="session")
def test_scopes(app, test_servers):
    with app.app_context():
        dummy_server_a, dummy_server_b = test_servers
        scopes = [
            read_scope_a := Scope(
                scope="get_result",
                server=dummy_server_a,
            ),
            read_scope_b := Scope(scope="get_result", server=dummy_server_b),
            run_scope := Scope(scope="run", server=dummy_server_a),
            dummy_query_scope := Scope(
                scope="dummy_query:admin_level_1", server=dummy_server_a
            ),
        ]
        db.session.add_all(scopes)
        db.session.commit()
        return scopes


@pytest.fixture  # (scope="session")
def test_roles(app, test_scopes, test_servers):
    read_a, read_b, run, dummy_query = test_scopes
    server_a, server_b = test_servers
    with app.app_context():
        runner = Role(
            name="runner",
            scopes=[run, read_a, dummy_query],
            server=server_a,
            longest_token_life_minutes=2880,
            latest_token_expiry=datetime.datetime.now() + datetime.timedelta(days=365),
        )
        reader = Role(
            name="reader",
            scopes=[read_a],
            server=server_a,
            longest_token_life_minutes=2880,
            latest_token_expiry=datetime.datetime.now() + datetime.timedelta(days=365),
        )
        db.session.add(runner)
        db.session.add(reader)
        db.session.commit()
        return runner, reader


@pytest.fixture  # (scope="session")
def test_user_with_roles(app, test_user, test_roles):
    with app.app_context():
        uid, uname, upass = test_user
        role_a, role_b = test_roles
        test_user_orm = db.session.execute(
            db.select(User).where(User.id == uid)
        ).first()[0]
        test_user_orm.roles += [role_a, role_b]
        db.session.add(test_user_orm)
        db.session.commit()
    return uid, uname, upass


@pytest.fixture
def test_data(app, test_servers, test_admin, test_user, test_roles):
    with app.app_context():

        dummy_server_a, dummy_server_b = test_servers

        test_user_row = db.session.execute(
            db.select(User).where(User.id == test_user.id)
        ).first()

        # Add some things that you can do on the servers
        scs = []
        for scope, admin_unit in product(
            ("DUMMY_ROUTE_A", "DUMMY_ROUTE_B"),
            (f"admin{x}" for x in range(4)),
        ):
            cap = f"{scope}:{admin_unit}"
            sc_a = Scope(
                scope=cap,
                server=dummy_server_a,
            )
            scs.append(sc_a)
            db.session.add(sc_a)
            sc_b = Scope(
                scope=cap,
                server=dummy_server_b,
            )
            scs.append(sc_b)
            db.session.add(sc_b)

        # Give test admin a token on server b

        db.session.commit()


@pytest.fixture
def auth(client):
    return AuthActions(client)


@pytest.fixture
def test_data_with_access_rights(app, test_data):
    with app.app_context():
        yield
