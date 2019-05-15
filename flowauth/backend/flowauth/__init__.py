# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import flask
import flask_login
from flask import Flask, make_response
from flask_login import LoginManager, current_user
from flask_principal import Principal, identity_loaded, UserNeed, RoleNeed
from flask_wtf.csrf import CSRFProtect, generate_csrf, CSRFError
from cryptography.fernet import Fernet

from .config import get_config
from .invalid_usage import InvalidUsage
from .models import *
from .admin import blueprint as admin_blueprint
from .token_management import blueprint as token_management_blueprint
from .login import blueprint as login_blueprint
from .spatial_aggregation import blueprint as aggregation_unit_blueprint


def create_app(test_config=None):
    app = Flask(__name__)

    app.config.from_mapping(get_config())

    if test_config is not None:  # load the test config if passed in
        app.config.update(test_config)

    # Connect the db
    db.init_app(app)

    # Set up flask-login
    login_manager = LoginManager()
    login_manager.init_app(app)

    # Set up flask-principal for roles management
    principals = Principal()
    principals.init_app(app)

    # Set up csrf protection
    csrf = CSRFProtect()
    csrf.init_app(app)
    csrf.exempt(login_blueprint)

    app.register_blueprint(login_blueprint)
    app.register_blueprint(admin_blueprint, url_prefix="/admin")
    app.register_blueprint(token_management_blueprint, url_prefix="/user")
    app.register_blueprint(
        aggregation_unit_blueprint, url_prefix="/spatial_aggregation"
    )

    @app.after_request
    def set_xsrf_cookie(response):
        """
        Sets the csrf token used by csrf protect as a cookie to allow usage with
        react.
        """
        response.set_cookie("X-CSRF", generate_csrf())
        return response

    @app.errorhandler(CSRFError)
    def handle_csrf_error(e):
        """
        CSRF errors are interpreted as an access denied.
        """
        return "CSRF error", 401

    @app.errorhandler(InvalidUsage)
    def handle_invalid_usage(error):
        response = flask.jsonify(error.to_dict())
        response.status_code = error.status_code
        return response

    @app.before_request
    def before_request():
        """
        Make sessions expire after 20 minutes of inactivity.
        """
        flask.session.permanent = True
        app.permanent_session_lifetime = datetime.timedelta(minutes=20)
        flask.session.modified = True
        flask.g.user = flask_login.current_user

    @login_manager.user_loader
    def load_user(userid):
        """Helper for flask-login."""
        return User.query.filter(User.id == userid).first()

    @identity_loaded.connect_via(app)
    def on_identity_loaded(sender, identity):
        """Helper for flask-principal."""
        # Set the identity user object
        identity.user = current_user

        # Add the UserNeed to the identity
        if hasattr(current_user, "id"):
            identity.provides.add(UserNeed(current_user.id))

        try:
            if current_user.is_admin:
                identity.provides.add(RoleNeed("admin"))
        except AttributeError:
            pass  # Definitely not an admin

    @app.cli.command("get-fernet")
    def make_flowauth_fernet_key():
        """
        Generate a new Fernet key for symmetric encryption of data at
        rest.
        """
        print(f'FLOWAUTH_FERNET_KEY="{Fernet.generate_key().decode()}"')

    # Add flask <command> CLI commands
    app.cli.add_command(demodata)
    app.cli.add_command(init_db_command)
    app.cli.add_command(add_admin)
    return app
