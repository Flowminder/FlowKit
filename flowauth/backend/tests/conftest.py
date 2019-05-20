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
        user = User.query.filter(User.username == app.config["ADMIN_USER"]).first()

        return user.id, user.username, app.config["ADMIN_PASSWORD"]


@pytest.fixture
def test_group(app):
    with app.app_context():
        group = Group(name="TEST_GROUP")
        db.session.add(group)
        db.session.commit()
        return group.id, group.name


@pytest.fixture
def test_data(app, test_admin, test_user, test_group):
    with app.app_context():
        agg_units = [SpatialAggregationUnit(name=f"admin{x}") for x in range(4)]
        db.session.add_all(agg_units)

        test_group = Group.query.filter(Group.id == test_group[0]).first()
        # Each user is also a group
        for user in User.query.all():
            test_group.members.append(user)
        db.session.add(test_group)
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
            gsp = GroupServerPermission(group=test_group, server_capability=sc)
            db.session.add(gsp)
        db.session.add(
            GroupServerTokenLimits(
                group=test_group,
                longest_life=2,
                latest_end=datetime.datetime(2020, 1, 1),
                server=dummy_server_a,
            )
        )

        # Give test admin a token on server b

        t = Token(
            name="DUMMY_TOKEN",
            token="DUMMY_TOKEN_STRING",
            expires=datetime.datetime(2019, 1, 1),
            owner=User.query.all()[0],
            server=dummy_server_b,
        )
        db.session.add(t)
        db.session.commit()


@pytest.fixture
def test_data_with_access_rights(app, test_data, test_group):
    with app.app_context():
        dl_capability = Capability.query.filter(
            Capability.name == "DUMMY_ROUTE_A"
        ).first()
        sc = ServerCapability.query.filter(
            ServerCapability.capability_id == dl_capability.id
        ).first()
        gsp = GroupServerPermission.query.filter(
            GroupServerPermission.server_capability == sc
            and GroupServerPermission.group_id == test_group[0]
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
