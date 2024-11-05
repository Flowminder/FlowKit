# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from hashlib import md5

import datetime

import pytest

from flowauth.models import (
    Server,
    User,
    db,
    Role,
)


def test_disallow_right_on_server_disallows_for_role(
    app, test_servers, test_scopes, test_roles, test_user_with_roles
):
    """Tests that a scope being removed from the server propogates to associated roles"""
    with app.app_context():
        runner_role, reader_role, _ = test_roles
        reader_scope, _, runner_scope, _ = test_scopes
        db.session.add_all([reader_role, reader_scope])
        assert reader_role.is_allowed(["get_result"])
        db.session.delete(reader_scope)
        db.session.commit()
        assert not reader_role.is_allowed(["get_result"])


def test_token_time_limits_reflect_server_limits(app):
    """Test that if a user's token time limits are bounded to those current on the server."""
    with app.app_context():
        user = User(username="TEST_USER", password="TEST_PASSWORD")
        server = Server(
            name="TEST_SERVER",
            longest_token_life_minutes=2880,
            latest_token_expiry=datetime.datetime(2020, 1, 1),
        )
        token_limit_role = Role(
            name="token_limit_test",
            longest_token_life_minutes=2880,
            latest_token_expiry=datetime.datetime(2020, 1, 1),
            server=server,
        )
        user.roles.append(token_limit_role)
        db.session.add(user)
        db.session.add(token_limit_role)
        db.session.add(server)
        db.session.commit()
        limits = user.token_limits(server)
        assert 2880 == limits["longest_life"]
        assert datetime.datetime(2020, 1, 1) == limits["latest_end"]
        server.longest_token_life_minutes = 600
        limits = user.token_limits(server)
        assert 600 == limits["longest_life"]
        server.latest_token_expiry = datetime.datetime(2019, 1, 1)
        limits = user.token_limits(server)
        assert datetime.datetime(2019, 1, 1) == limits["latest_end"]
