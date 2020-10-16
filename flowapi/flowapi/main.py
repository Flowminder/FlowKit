# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from functools import partial

import rapidjson

import uuid
import sys

from quart import Quart, request, current_app, has_request_context
import asyncpg
import logging
import zmq
from zmq.asyncio import Context

from flowapi.config import get_config
from flowapi.jwt_auth_callbacks import register_logging_callbacks
from flowapi.query_endpoints import blueprint as query_endpoints_blueprint
from flowapi.geography import blueprint as geography_blueprint
from flowapi.api_spec import blueprint as spec_blueprint
from quart_jwt_extended import JWTManager, get_jwt_identity

import structlog

from flowapi.user_model import user_loader_callback


root_logger = logging.getLogger("flowapi")
root_logger.setLevel(logging.DEBUG)


def add_request_data(_, __, event_dict):
    """
    Processor which adds request data to log entries if available in this context.
    """
    if has_request_context():
        event_dict = dict(
            **event_dict,
            request=dict(
                request_id=getattr(request, "request_id", None),
                path=request.path,
                src_ip=request.headers.get("Remote-Addr"),
                user=str(get_jwt_identity()),
            ),
        )
    return event_dict


structlog.configure(
    processors=[
        add_request_data,
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(serializer=rapidjson.dumps),
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    cache_logger_on_first_use=True,
)


async def connect_logger():
    log_level = current_app.config["FLOWAPI_LOG_LEVEL"]
    logger = root_logger.getChild("debug")
    logger.setLevel(log_level)
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    logger.addHandler(ch)
    # Debug logger. Quart doesn't allow us to override current_app.logger, but won't use it by default.
    current_app.flowapi_logger = structlog.wrap_logger(logger)
    current_app.flowapi_logger.debug("Started")

    # Logger for authentication

    logger = root_logger.getChild("access")
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    logger.addHandler(ch)
    # Debug logger. Quart doesn't allow us to override current_app.logger, but won't use it by default.
    current_app.access_logger = structlog.wrap_logger(logger)

    # Logger for all queries run or accessed
    logger = root_logger.getChild("query")
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    logger.addHandler(ch)
    current_app.query_run_logger = structlog.wrap_logger(logger)


async def connect_zmq():
    context = Context.instance()
    #  Socket to talk to server
    current_app.flowapi_logger.debug("Connecting to FlowMachine server…")
    socket = context.socket(zmq.REQ)
    socket.connect(
        f"tcp://{current_app.config['FLOWMACHINE_HOST']}:{current_app.config['FLOWMACHINE_PORT']}"
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
    current_app.db_conn_pool = await asyncpg.create_pool(dsn, max_size=20)


def create_app():
    app = Quart(__name__)

    app.config.from_mapping(get_config())

    jwt = JWTManager(app)
    app.before_serving(connect_logger)
    app.before_serving(create_db)
    app.before_request(add_uuid)
    app.before_request(connect_zmq)
    app.teardown_request(close_zmq)

    @app.route("/")
    async def root():
        return ""

    app.register_blueprint(query_endpoints_blueprint, url_prefix="/api/0")
    app.register_blueprint(geography_blueprint, url_prefix="/api/0")
    app.register_blueprint(spec_blueprint, url_prefix="/api/0/spec")

    register_logging_callbacks(jwt)
    jwt.user_loader_callback_loader(user_loader_callback)

    return app
