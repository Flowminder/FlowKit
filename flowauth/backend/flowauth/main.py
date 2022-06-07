import datetime
import logging
import sys
import uuid
from functools import partial

import flask
import structlog
from flask import Flask, current_app, request

import simplejson
from flask_login import LoginManager, current_user
from flask_principal import Principal, RoleNeed, UserNeed, identity_loaded
from flask_wtf.csrf import CSRFError, CSRFProtect, generate_csrf
from flowauth.invalid_usage import InvalidUsage
from flowauth.util import request_context_processor

try:
    from uwsgidecorators import lock
except ImportError:
    # Psuedo-lock function for if not running under uwsgi
    def lock(func):
        return func


logger = logging.getLogger("flowauth")
logger.setLevel(logging.DEBUG)

structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        request_context_processor,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(serializer=simplejson.dumps),
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=structlog.threadlocal.wrap_dict(dict),
    cache_logger_on_first_use=True,
)


def connect_logger():
    log_level = current_app.config["LOG_LEVEL"]
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    logger.addHandler(ch)

    current_app.logger = structlog.wrap_logger(logger)
    current_app.logger.info("Started")


def connect_audit_logger():
    channel = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "{date}; Request: {request_id}; Event {message}", style="{"
    )


def set_xsrf_cookie(response):
    """
    Sets the csrf token used by csrf protect as a cookie to allow usage with
    react.
    """
    response.set_cookie("X-CSRF", generate_csrf())
    return response


def handle_csrf_error(e):
    """
    CSRF errors are interpreted as an access denied.
    """
    return "CSRF error", 401


def handle_invalid_usage(error):
    response = flask.jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


def before_request():
    """
    Make sessions expire after 20 minutes of inactivity.
    """
    flask.session.permanent = True
    current_app.permanent_session_lifetime = datetime.timedelta(minutes=20)
    flask.session.modified = True
    flask.g.user = current_user
    request.id = str(uuid.uuid4())


def load_user(userid):
    """Helper for flask-login."""
    from flowauth.models import User

    return User.query.filter(User.id == userid).first()


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


def create_app(test_config=None):
    from flowauth.config import get_config
    from flowauth.models import db
    from flowauth.cli import (
        init_db_command,
        add_admin_command,
        make_flowauth_fernet_key,
        demo_data,
    )
    from .admin import blueprint as admin_blueprint
    from .users import blueprint as users_blueprint
    from .servers import blueprint as servers_blueprint
    from .token_management import blueprint as token_management_blueprint
    from .login import blueprint as login_blueprint
    from .user_settings import blueprint as user_settings_blueprint
    from .version import blueprint as version_blueprint

    app = Flask(__name__)

    app.config.from_mapping(get_config())

    # Connect the logger
    app.before_first_request(connect_logger)

    if test_config is not None:
        # load the test config if passed in
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
    csrf.exempt(version_blueprint)

    app.register_blueprint(login_blueprint)
    app.register_blueprint(admin_blueprint, url_prefix="/admin")
    app.register_blueprint(servers_blueprint, url_prefix="/admin")
    app.register_blueprint(users_blueprint, url_prefix="/admin")
    app.register_blueprint(token_management_blueprint, url_prefix="/tokens")
    app.register_blueprint(user_settings_blueprint, url_prefix="/user")

    app.register_blueprint(version_blueprint)

    if app.config["DEMO_MODE"]:  # Create demo data
        from flowauth.models import make_demodata

        app.before_first_request(make_demodata)
    else:
        # Initialise the database
        from flowauth.models import init_db
        from flowauth.models import add_admin

        app.before_first_request(lock(partial(init_db, force=app.config["RESET_DB"])))
        # Create an admin user

        app.before_first_request(
            lock(
                partial(
                    add_admin,
                    username=app.config["ADMIN_USER"],
                    password=app.config["ADMIN_PASSWORD"],
                )
            )
        )

    app.before_first_request(
        app.config["DB_IS_SET_UP"].wait
    )  # Cause workers to wait for db to set up

    app.after_request(set_xsrf_cookie)
    app.errorhandler(CSRFError)(handle_csrf_error)
    app.errorhandler(InvalidUsage)(handle_invalid_usage)
    app.before_request(before_request)
    login_manager.user_loader(load_user)
    identity_loaded.connect_via(app)(on_identity_loaded)
    # Add flask <command> CLI commands
    app.cli.add_command(demo_data)
    app.cli.add_command(init_db_command)
    app.cli.add_command(add_admin_command)
    app.cli.add_command(make_flowauth_fernet_key)
    return app
