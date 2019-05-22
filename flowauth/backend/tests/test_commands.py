# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowauth import add_admin_command
from flowauth.models import User, db, init_db_command, demodata, Group


def test_add_admin(app):
    """
    Test that we can add an admin.
    """
    with app.app_context():
        runner = app.test_cli_runner()
        result = runner.invoke(
            add_admin_command, ["DUMMY_ADMINISTRATOR", "DUMMY_ADMINISTATOR_PASSWORD"]
        )
        assert (
            User.query.filter(User.username == "DUMMY_ADMINISTRATOR").first().is_admin
        )


def test_add_admin_promotes(app):
    """
    Test that we can promote an existing user to admin.
    """
    with app.app_context():
        user = User(username="DUMMY_ADMINISTRATOR")
        user.password = "DUMMY_PASSWORD"
        original_password_hash = user.password
        db.session.add(user)
        db.session.commit()
        runner = app.test_cli_runner()
        result = runner.invoke(
            add_admin_command, ["DUMMY_ADMINISTRATOR", "DUMMY_ADMINISTATOR_PASSWORD"]
        )
        user = User.query.filter(User.username == "DUMMY_ADMINISTRATOR").first()
        assert user.is_admin
        assert user.password != original_password_hash


def test_init_db(app):
    """
    DB shouldn't get reinitialised if already built.
    """
    with app.app_context():
        runner = app.test_cli_runner()
        result = runner.invoke(init_db_command)
        assert len(User.query.all()) > 0


def test_init_db_force(app):
    """
    DB should be wiped clean if force is true.
    """
    with app.app_context():
        runner = app.test_cli_runner()
        result = runner.invoke(init_db_command, ["--force"])
        assert len(User.query.all()) == 0


def test_demo_data(app):
    """
    DB should contain demo data..
    """
    with app.app_context():
        runner = app.test_cli_runner()
        result = runner.invoke(demodata)
        assert len(User.query.all()) == 2
        assert len(Group.query.all()) == 3
