from flowauth import (
    User,
    Group,
    Server,
    datetime,
    Capability,
    ServerCapability,
    GroupServerPermission,
    GroupServerTokenLimits,
)


def test_disallow_right_on_server_disallows_for_group(app):
    """Test that if a claim is disallowed on a server, it will be disallowed even if previously granted to groups."""
    with app.app_context():
        user = User(username="TEST_USER", password="TEST_PASSWORD")
        user_group = Group(name="TEST_USER", user_group=True)
        user.groups.append(user_group)
        server = Server(
            name="TEST_SERVER",
            longest_token_life=2880,
            latest_token_expiry=datetime.datetime(2020, 1, 1),
        )
        capability = Capability(name="TEST_ROUTE")
        server_capability = ServerCapability(
            server=server, capability=capability, poll=True
        )
        gsp = GroupServerPermission(
            group=user_group, server_capability=server_capability, poll=True
        )
        token_limits = GroupServerTokenLimits(
            group=user_group,
            longest_life=1440,
            latest_end=datetime.datetime(2019, 1, 1),
            server=server,
        )
        claims = user.allowed_claims(server)
        assert claims["TEST_ROUTE"]["permissions"]["poll"]
        server_capability.poll = False
        claims = user.allowed_claims(server)
        assert {} == claims


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
        limits = user.token_limits(server)
        assert 2880 == limits["longest_life"]
        assert datetime.datetime(2020, 1, 1) == limits["latest_end"]
        server.longest_token_life = 10
        limits = user.token_limits(server)
        assert 10 == limits["longest_life"]
        server.latest_token_expiry = datetime.datetime(2019, 1, 1)
        limits = user.token_limits(server)
        assert datetime.datetime(2019, 1, 1) == limits["latest_end"]
