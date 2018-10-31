# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime
import pytest

from flowauth import (
    create_app,
    db,
    User,
    Group,
    Server,
    Capability,
    ServerCapability,
    GroupServerPermission,
    GroupServerTokenLimits,
    Token,
    SpatialAggregationUnit,
)


@pytest.fixture
def app(tmpdir):
    """Per test app"""
    db_path = tmpdir / "db.db"

    app = create_app(
        {"TESTING": True, "SQLALCHEMY_DATABASE_URI": f"sqlite:///{db_path}"}
    )
    with app.app_context():
        db.create_all()
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
        return user.id, user.username, "TEST_USER_PASSWORD"


@pytest.fixture
def test_admin(app):
    with app.app_context():
        user = User(
            username="TEST_ADMIN", password="TEST_ADMIN_PASSWORD", is_admin=True
        )
        ug = Group(name="TEST_ADMIN", user_group=True, members=[user])
        db.session.add(user)
        db.session.add(ug)
        db.session.commit()
        return user.id, user.username, "TEST_ADMIN_PASSWORD"


@pytest.fixture
def test_data(app):
    with app.app_context():
        agg_units = [SpatialAggregationUnit(name=f"admin{x}") for x in range(4)]
        db.session.add_all(agg_units)
        users = [
            User(username="TEST_USER", password="DUMMY_PASSWORD"),
            User(username="TEST_ADMIN", is_admin=True),
        ]
        for user in users:
            user.password = "DUMMY_PASSWORD"

        # Each user is also a group
        groups = [
            Group(name="TEST_USER", user_group=True),
            Group(name="TEST_ADMIN", user_group=True),
            Group(name="TEST_GROUP"),
        ]
        groups[0].members.append(users[0])
        groups[1].members.append(users[1])
        for user in users:
            groups[2].members.append(user)
        for x in users + groups:
            db.session.add(x)
        # Add some things that you can do
        caps = []
        for c in ("DUMMY_ROUTE_A", "DUMMY_ROUTE_B"):
            c = Capability(name=c)
            db.session.add(c)
            caps.append(c)
        # Add some servers
        dummy_server_a = Server(
            name="DUMMY_SERVER_A",
            longest_token_life=2,
            latest_token_expiry=datetime.datetime(2020, 1, 1),
            secret_key="DUMMY_SERVER_A_KEY",
        )
        dummy_server_b = Server(
            name="DUMMY_SERVER_B",
            longest_token_life=2,
            latest_token_expiry=datetime.datetime(2021, 1, 1),
            secret_key="DUMMY_SERVER_B_KEY",
        )
        db.session.add(dummy_server_a)
        db.session.add(dummy_server_b)

        # Add some things that you can do on the servers
        scs = []
        for cap in caps:
            scs.append(
                ServerCapability(
                    capability=cap,
                    server=dummy_server_a,
                    get_result=True,
                    run=True,
                    poll=False,
                )
            )
            scs.append(ServerCapability(capability=cap, server=dummy_server_b))
        for sc in scs:
            sc.spatial_aggregation = agg_units
            db.session.add(sc)
        # Give test user group permissions on Haiti
        for sc in dummy_server_a.capabilities:
            gsp = GroupServerPermission(group=groups[0], server_capability=sc)
            db.session.add(gsp)
        db.session.add(
            GroupServerTokenLimits(
                group=groups[0],
                longest_life=2,
                latest_end=datetime.datetime(2019, 1, 1),
                server=dummy_server_a,
            )
        )

        # Give test admin a token on server b

        t = Token(
            name="DUMMY_TOKEN",
            token="DUMMY_TOKEN_STRING",
            expires=datetime.datetime(2019, 1, 1),
            owner=users[1],
            server=dummy_server_b,
        )
        db.session.add(t)
        db.session.commit()


@pytest.fixture
def test_data_with_access_rights(app, test_data):
    with app.app_context():
        dl_capability = Capability.query.filter(
            Capability.name == "DUMMY_ROUTE_A"
        ).first()
        sc = ServerCapability.query.filter(
            ServerCapability.capability_id == dl_capability.id
        ).first()
        gsp = GroupServerPermission.query.filter(
            GroupServerPermission.server_capability == sc
        ).first()
        admin0_agg_unit = SpatialAggregationUnit.query.filter(
            SpatialAggregationUnit.name == "admin0"
        ).first()
        gsp.get_result = True
        gsp.run = True
        gsp.spatial_aggregation = [admin0_agg_unit]
        db.session.add(gsp)
        db.session.commit()


@pytest.fixture
def auth(client):
    return AuthActions(client)
