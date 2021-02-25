from hashlib import md5

import datetime

from flowauth.models import (
    Group,
    GroupServerPermission,
    GroupServerTokenLimits,
    Server,
    ServerCapability,
    User,
    db,
)


def test_disallow_right_on_server_disallows_for_group(app):
    """Test that if a claim is disallowed on a server, it will be disallowed even if previously granted to groups."""
    with app.app_context():
        session = db.session
        user = User(username="TEST_USER", password="TEST_PASSWORD")

        user_group = Group(name="TEST_USER", user_group=True)
        user.groups.append(user_group)

        session.add(user)
        session.add(user_group)
        server = Server(
            name="TEST_SERVER",
            longest_token_life=2880,
            latest_token_expiry=datetime.datetime(2020, 1, 1),
        )
        session.add(server)
        server_capability = ServerCapability(
            server=server,
            capability="get_result&DUMMY_ROUTE",
            enabled=True,
            capability_hash=md5(b"get_result&DUMMY_ROUTE").hexdigest(),
        )
        session.add(server_capability)
        gsp = GroupServerPermission(
            group=user_group, server_capability=server_capability
        )
        session.add(gsp)
        token_limits = GroupServerTokenLimits(
            group=user_group,
            longest_life=1440,
            latest_end=datetime.datetime(2019, 1, 1),
            server=server,
        )
        session.add(token_limits)
        session.commit()
        claims = user.allowed_claims(server)
        assert "get_result&DUMMY_ROUTE" in claims
        session.delete(gsp)
        session.commit()
        claims = user.allowed_claims(server)
        assert [] == claims


def test_token_time_limits_reflect_server_limits(app):
    """Test that if a user's token time limits are bounded to those current on the server."""
    with app.app_context():
        user = User(username="TEST_USER", password="TEST_PASSWORD")
        user_group = Group(name="TEST_USER", user_group=True)
        user.groups.append(user_group)
        server = Server(
            name="TEST_SERVER",
            longest_token_life=2880,
            latest_token_expiry=datetime.datetime(2020, 1, 1),
        )
        token_limits = GroupServerTokenLimits(
            group=user_group,
            longest_life=2880,
            latest_end=datetime.datetime(2020, 1, 1),
            server=server,
        )
        db.session.add(user)
        db.session.add(user_group)
        db.session.add(server)
        db.session.add(token_limits)
        db.session.commit()
        limits = user.token_limits(server)
        assert 2880 == limits["longest_life"]
        assert datetime.datetime(2020, 1, 1) == limits["latest_end"]
        server.longest_token_life = 10
        limits = user.token_limits(server)
        assert 10 == limits["longest_life"]
        server.latest_token_expiry = datetime.datetime(2019, 1, 1)
        limits = user.token_limits(server)
        assert datetime.datetime(2019, 1, 1) == limits["latest_end"]
