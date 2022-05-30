# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowauth.cli import (
    add_admin_command,
    demo_data,
    init_db_command,
)  # , add_role_command
from flowauth.models import User, db, Role, Scope, Server


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
            add_admin_command, ["DUMMY_ADMINISTRATOR", "DUMMY_ADMINISTRATOR_PASSWORD"]
        )
        assert result.exit_code == 0
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
        result = runner.invoke(
            add_admin_command, ["DUMMY_ADMINISTRATOR", "DUMMY_ADMINISTRATOR_PASSWORD"]
        )
        result = runner.invoke(init_db_command)
        assert len(User.query.all()) > 0


def test_init_db_force(app):
    """
    DB should be wiped clean if force is true.
    """
    with app.app_context():
        runner = app.test_cli_runner()
        result = runner.invoke(init_db_command)
        result = runner.invoke(
            add_admin_command, ["DUMMY_ADMINISTRATOR", "DUMMY_ADMINISTRATOR_PASSWORD"]
        )
        assert len(User.query.all()) > 0
        app.config["DB_IS_SET_UP"].clear()
        result = runner.invoke(init_db_command, ["--force"])
        assert len(User.query.all()) == 0


def test_demo_data_only_sets_up_once(app, caplog):
    """
    Demo data should only be created once.
    """
    with app.app_context():
        runner = app.test_cli_runner()
        app.config["DB_IS_SET_UP"].clear()
        result = runner.invoke(demo_data)
        result = runner.invoke(demo_data)
        assert len(User.query.all()) == 2
        assert len(Scope.query.all()) == 3
    assert "Database already set up by another worker, skipping." in caplog.text


def test_db_init_only_runs_once(app, caplog):
    """
    Shouldn't attempt to init the db if something else has.
    """
    with app.app_context():
        runner = app.test_cli_runner()
        app.config["DB_IS_SET_UP"].set()
        result = runner.invoke(init_db_command)

    assert "Database already set up by another worker, skipping." in caplog.text


def test_demo_data(app):
    """
    DB should contain demo data..
    """
    with app.app_context():
        runner = app.test_cli_runner()
        app.config["DB_IS_SET_UP"].clear()
        result = runner.invoke(demo_data)
        assert len(User.query.all()) == 2
        assert len(Role.query.all()) == 2
        assert len(Server.query.all()) == 1
        assert len(Scope.query.all()) == 3


# def test_add_role(app):
#    """
#   Test that we can use the CLI to add a role
#    """
#    with app.app_context():
#        runner = app.test_cli_runner()
#        app.config["DB_IS_SET_UP"].set()
#        result = runner.invoke(add_role_command, ["DUMMY_ROLE", "DUMMY_SCOPE"])
#        assert len(Role.query.all()) == 1
