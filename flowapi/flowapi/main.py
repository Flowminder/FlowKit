# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import uuid


import quart.flask_patch
from quart import Quart, request, current_app
import asyncpg
import logging
import zmq
from zmq.asyncio import Context

from flowapi.config import get_config
from flowapi.jwt_auth_callbacks import register_logging_callbacks
from flowapi.query_endpoints import blueprint as query_endpoints_blueprint
from flowapi.geography import blueprint as geography_blueprint
from flask_jwt_extended import JWTManager

import structlog

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ],
    context_class=dict,
    cache_logger_on_first_use=True,
)


async def connect_logger():
    log_level = current_app.config["LOG_LEVEL"]
    logger = logging.getLogger("flowapi")
    logger.setLevel(log_level)
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    logger.addHandler(ch)
    # Debug logger. Quart doesn't allow us to override current_app.logger, but won't use it by default.
    current_app.flowapi_logger = structlog.wrap_logger(logger, name="flowapi-debug")
    current_app.flowapi_logger.debug("Started")

    # Logger for authentication

    current_app.access_logger = structlog.get_logger(name="flowapi-access")

    # Logger for all queries run or accessed

    current_app.query_run_logger = structlog.get_logger(name="flowapi-query")


async def connect_zmq():
    context = Context.instance()
    #  Socket to talk to server
    current_app.flowapi_logger.debug("Connecting to FlowMachine server…")
    socket = context.socket(zmq.REQ)
    socket.connect(
        f"tcp://{current_app.config['FLOWMACHINE_SERVER']}:{current_app.config['FLOWMACHINE_PORT']}"
    )
    request.socket = socket
    current_app.flowapi_logger.debug("Connected.")


async def add_uuid():
    request.request_id = str(uuid.uuid4())


def close_zmq(exc):
    current_app.flowapi_logger.debug("Closing connection to FlowMachine server…")
    try:
        request.socket.close()
        current_app.flowapi_logger.debug("Closed socket.")
    except AttributeError:
        current_app.flowapi_logger.debug("No socket to close.")


async def create_db():
    dsn = current_app.config["FLOWDB_DSN"]
    current_app.pool = await asyncpg.create_pool(dsn, max_size=20)


def create_app():
    app = Quart(__name__)

    app.config.from_mapping(get_config())

    jwt = JWTManager(app)
    app.before_first_request(connect_logger)
    app.before_first_request(create_db)
    app.before_request(add_uuid)
    app.before_request(connect_zmq)
    app.teardown_request(close_zmq)

    @app.route("/")
    async def root():
        return ""

    app.register_blueprint(query_endpoints_blueprint, url_prefix="/api/0")
    app.register_blueprint(geography_blueprint, url_prefix="/api/0")

    register_logging_callbacks(jwt)

    return app
