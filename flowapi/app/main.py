# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import uuid

from pathlib import Path

import quart.flask_patch
from quart import Quart, request
import asyncpg
import logging
import os
import zmq
from logging.handlers import TimedRotatingFileHandler
from zmq.asyncio import Context

from .jwt_auth_callbacks import register_logging_callbacks
from .run_query import blueprint as run_query_blueprint
from .geography import blueprint as geography_blueprint
from flask_jwt_extended import JWTManager

import structlog

structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)


def getsecret(key: str, default: str) -> str:
    """
    Get a value from docker secrets (i.e. read it from a file in
    /run/secrets), return a default if the file is not there.

    Parameters
    ----------
    key: str
        Name of the secret.
    default: str
        Default value to return if the file does not exist

    Returns
    -------
    str
        Value in the file, or default
    """
    try:
        with open(Path("/run/secrets") / key, "r") as fin:
            return fin.read().strip()
    except FileNotFoundError:
        return default


def create_app():
    app = Quart(__name__)
    app.config["JWT_SECRET_KEY"] = getsecret(
        "JWT_SECRET_KEY", os.getenv("JWT_SECRET_KEY")
    )
    jwt = JWTManager(app)

    log_root = os.getenv("LOG_DIRECTORY", "/var/log/flowapi/")

    @app.before_first_request
    async def connect_logger():
        log_level = logging.getLevelName(os.getenv("LOG_LEVEL", "error").upper())
        logger = logging.getLogger("flowapi")
        logger.setLevel(log_level)
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        logger.addHandler(ch)
        app.log = structlog.wrap_logger(logger)

        # Logger for authentication

        logger = logging.getLogger("flowkit-access")
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        logger.addHandler(ch)

        fh = TimedRotatingFileHandler(
            os.path.join(log_root, "flowkit-access.log"), when="midnight"
        )
        fh.setLevel(logging.INFO)
        logger.addHandler(fh)
        app.access_logger = structlog.wrap_logger(logger)

        # Logger for all queries run or accessed

        logger = logging.getLogger("flowkit-query")
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        logger.addHandler(ch)

        fh = TimedRotatingFileHandler(
            os.path.join(log_root, "query-runs.log"), when="midnight"
        )
        fh.setLevel(logging.INFO)
        logger.addHandler(fh)
        app.query_run_logger = structlog.wrap_logger(logger)

    @app.before_request
    async def add_uuid():
        request.request_id = str(uuid.uuid4())
        app.log.debug("Added request id.", request_id=request.request_id)

    @app.before_request
    async def connect_zmq():
        context = Context.instance()
        #  Socket to talk to server
        app.log.debug("Connecting to FlowMachine server", request_id=request.request_id)
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://{os.getenv('SERVER')}:5555")
        request.socket = socket
        app.log.debug("Connected.", request_id=request.request_id)

    @app.teardown_request
    def close_zmq(exc):
        app.log.debug(
            "Closing connection to FlowMachine server…", request_id=request.request_id
        )
        try:
            request.socket.close()
            app.log.debug("Closed socket.", request_id=request.request_id)
        except AttributeError:
            app.log.debug("No socket to close.", request_id=request.request_id)

    @app.before_first_request
    async def create_db():
        dsn = f'postgres://{getsecret("API_DB_USER", os.getenv("DB_USER"))}:{getsecret("API_DB_PASS", os.getenv("DB_PASS"))}@{os.getenv("DB_HOST")}:{os.getenv("FLOWDB_PORT", 5432)}/flowdb'
        app.pool = await asyncpg.create_pool(dsn, max_size=20)

    @app.route("/")
    async def root():
        return ""

    app.register_blueprint(run_query_blueprint, url_prefix="/api/0")
    app.register_blueprint(geography_blueprint, url_prefix="/api/0")

    register_logging_callbacks(jwt)

    return app
