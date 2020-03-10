# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
This module provides initial setup routines for flowmachine. From a user
perspective, only the `connect` method is relevant.

From a developer perspective, this is where one-time operations
should live - for example configuring loggers.
"""
import warnings
from contextlib import contextmanager

import redis
import structlog
from concurrent.futures import ThreadPoolExecutor
from typing import Tuple, Optional

from redis import StrictRedis

import flowmachine
from flowmachine.core import Connection
from flowmachine.core.context import bind_context, context, get_db
from flowmachine.core.errors import NotConnectedError
from flowmachine.core.logging import set_log_level
from get_secret_or_env_var import environ, getenv

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


@contextmanager
def connections(
    *,
    log_level: Optional[str] = None,
    flowdb_port: Optional[int] = None,
    flowdb_user: Optional[str] = None,
    flowdb_password: Optional[str] = None,
    flowdb_host: Optional[str] = None,
    flowdb_connection_pool_size: Optional[int] = None,
    flowdb_connection_pool_overflow: Optional[int] = None,
    redis_host: Optional[str] = None,
    redis_port: Optional[int] = None,
    redis_password: Optional[str] = None,
    conn: Optional[Connection] = None,
) -> None:
    """
    Context manager which connects flowmachine to a database, and performs initial set-up routines.
    You may provide a Settings object here, which can specify the database
    you wish to connect to, logging behaviour, available tables and so on.

    After connecting, you should use `flowmachine.core.context.get_db()` to access
    the database connection, `flowmachine.core.context.get_redis()` to get the redis
    connection, and `flowmachine.core.context.get_executor()` to get the threadpool.

    Parameters
    ----------
    log_level : str, default "error"
        Level to log at
    flowdb_port : int, default 9000
        Port number to connect to flowdb
    flowdb_user : str, default "flowmachine"
        Name of user to connect to flowdb as
    flowdb_password : str
        Password to connect to flowdb
    flowdb_host : str, default "localhost"
        Hostname of flowdb server
    flowdb_connection_pool_size : int, default 5
        Default number of database connections to use
    flowdb_connection_pool_overflow : int, default 1
        Number of extra database connections to allow
    redis_host : str, default "localhost"
        Hostname for redis server.
    redis_port : int, default 6379
        Port the redis server is available on
    redis_password : str
        Password for the redis instance
    conn : flowmachine.core.Connection
        Optionally provide an existing Connection object to use, overriding any the db options specified here.

    Notes
    -----
    All parameters can also be provided as environment variables.
    If a parameter is provided, and an environment variable is set,
    then the provided value is used. If neither is provided, the defaults as given
    in the docstring are used.

    Parameters can _also_ be set using Docker secrets, in which case a file with the name
    of the parameter in upper case should be present at /run/secrets/THE_PARAM.
    If a secret is available, the secret takes precedence over both the environment variable, and
    the default.
    """
    with context(
        *_do_connect(
            log_level=log_level,
            flowdb_port=flowdb_port,
            flowdb_user=flowdb_user,
            flowdb_password=flowdb_password,
            flowdb_host=flowdb_host,
            flowdb_connection_pool_size=flowdb_connection_pool_size,
            flowdb_connection_pool_overflow=flowdb_connection_pool_overflow,
            redis_host=redis_host,
            redis_port=redis_port,
            redis_password=redis_password,
            conn=conn,
        )
    ):
        yield


def connect(
    *,
    log_level: Optional[str] = None,
    flowdb_port: Optional[int] = None,
    flowdb_user: Optional[str] = None,
    flowdb_password: Optional[str] = None,
    flowdb_host: Optional[str] = None,
    flowdb_connection_pool_size: Optional[int] = None,
    flowdb_connection_pool_overflow: Optional[int] = None,
    redis_host: Optional[str] = None,
    redis_port: Optional[int] = None,
    redis_password: Optional[str] = None,
    conn: Optional[Connection] = None,
) -> None:
    """
    Connects flowmachine to a database, and performs initial set-up routines.
    You may provide a Settings object here, which can specify the database
    you wish to connect to, logging behaviour, available tables and so on.

    After connecting, you should use `flowmachine.core.context.get_db()` to access
    the database connection, `flowmachine.core.context.get_redis()` to get the redis
    connection, and `flowmachine.core.context.get_executor()` to get the threadpool.

    Parameters
    ----------
    log_level : str, default "error"
        Level to log at
    flowdb_port : int, default 9000
        Port number to connect to flowdb
    flowdb_user : str, default "flowmachine"
        Name of user to connect to flowdb as
    flowdb_password : str
        Password to connect to flowdb
    flowdb_host : str, default "localhost"
        Hostname of flowdb server
    flowdb_connection_pool_size : int, default 5
        Default number of database connections to use
    flowdb_connection_pool_overflow : int, default 1
        Number of extra database connections to allow
    redis_host : str, default "localhost"
        Hostname for redis server.
    redis_port : int, default 6379
        Port the redis server is available on
    redis_password : str
        Password for the redis instance
    conn : flowmachine.core.Connection
        Optionally provide an existing Connection object to use, overriding any the db options specified here.

    Notes
    -----
    All parameters can also be provided as environment variables.
    If a parameter is provided, and an environment variable is set,
    then the provided value is used. If neither is provided, the defaults as given
    in the docstring are used.

    Parameters can _also_ be set using Docker secrets, in which case a file with the name
    of the parameter in upper case should be present at /run/secrets/THE_PARAM.
    If a secret is available, the secret takes precedence over both the environment variable, and
    the default.
    """
    try:
        get_db()
        warnings.warn("FlowMachine already started. Overwriting existing context.")
    except NotConnectedError:
        pass
    bind_context(
        *_do_connect(
            log_level=log_level,
            flowdb_port=flowdb_port,
            flowdb_user=flowdb_user,
            flowdb_password=flowdb_password,
            flowdb_host=flowdb_host,
            flowdb_connection_pool_size=flowdb_connection_pool_size,
            flowdb_connection_pool_overflow=flowdb_connection_pool_overflow,
            redis_host=redis_host,
            redis_port=redis_port,
            redis_password=redis_password,
            conn=conn,
        )
    )


def _do_connect(
    *,
    log_level: Optional[str] = None,
    flowdb_port: Optional[int] = None,
    flowdb_user: Optional[str] = None,
    flowdb_password: Optional[str] = None,
    flowdb_host: Optional[str] = None,
    flowdb_connection_pool_size: Optional[int] = None,
    flowdb_connection_pool_overflow: Optional[int] = None,
    redis_host: Optional[str] = None,
    redis_port: Optional[int] = None,
    redis_password: Optional[str] = None,
    conn: Optional[Connection] = None,
) -> Tuple[Connection, ThreadPoolExecutor, StrictRedis]:
    """
    Connects flowmachine to a database, and performs initial set-up routines.
    You may provide a Settings object here, which can specify the database
    you wish to connect to, logging behaviour, available tables and so on.

    Parameters
    ----------
    log_level : str, default "error"
        Level to log at
    flowdb_port : int, default 9000
        Port number to connect to flowdb
    flowdb_user : str, default "flowmachine"
        Name of user to connect to flowdb as
    flowdb_password : str
        Password to connect to flowdb
    flowdb_host : str, default "localhost"
        Hostname of flowdb server
    flowdb_connection_pool_size : int, default 5
        Default number of database connections to use
    flowdb_connection_pool_overflow : int, default 1
        Number of extra database connections to allow
    redis_host : str, default "localhost"
        Hostname for redis server.
    redis_port : int, default 6379
        Port the redis server is available on
    redis_password : str
        Password for the redis instance
    conn : flowmachine.core.Connection
        Optionally provide an existing Connection object to use, overriding any the db options specified here.

    Returns
    -------
    Connection

    Notes
    -----
    All parameters can also be provided as environment variables.
    If a parameter is provided, and an environment variable is set,
    then the provided value is used. If neither is provided, the defaults as given
    in the docstring are used.

    Parameters can _also_ be set using Docker secrets, in which case a file with the name
    of the parameter in upper case should be present at /run/secrets/THE_PARAM.
    If a secret is available, the secret takes precedence over both the environment variable, and
    the default.
    """
    try:
        log_level = (
            getenv("FLOWMACHINE_LOG_LEVEL", "error") if log_level is None else log_level
        )
        flowdb_port = int(
            getenv("FLOWDB_PORT", "9000") if flowdb_port is None else flowdb_port
        )
        flowdb_user = (
            getenv("FLOWMACHINE_FLOWDB_USER", "flowmachine")
            if flowdb_user is None
            else flowdb_user
        )

        flowdb_password = (
            environ["FLOWMACHINE_FLOWDB_PASSWORD"]
            if flowdb_password is None
            else flowdb_password
        )
        flowdb_host = (
            getenv("FLOWDB_HOST", "localhost") if flowdb_host is None else flowdb_host
        )
        flowdb_connection_pool_size = (
            int(getenv("DB_CONNECTION_POOL_SIZE", "5"))
            if flowdb_connection_pool_size is None
            else flowdb_connection_pool_size
        )
        flowdb_connection_pool_overflow = int(
            getenv("DB_CONNECTION_POOL_OVERFLOW", "1")
            if flowdb_connection_pool_overflow is None
            else flowdb_connection_pool_overflow
        )

        redis_host = (
            getenv("REDIS_HOST", "localhost") if redis_host is None else redis_host
        )
        redis_port = int(
            getenv("REDIS_PORT", "6379") if redis_port is None else redis_port
        )
        redis_password = (
            environ["REDIS_PASSWORD"] if redis_password is None else redis_password
        )
    except KeyError as e:
        raise ValueError(
            f"You must provide a secret named {e.args[0]}, set an environment variable named {e.args[0]}, or provide the value as a parameter."
        )

    set_log_level("flowmachine.debug", log_level)

    if conn is None:
        conn = Connection(
            host=flowdb_host,
            port=flowdb_port,
            user=flowdb_user,
            password=flowdb_password,
            database="flowdb",
            pool_size=flowdb_connection_pool_size,
            overflow=flowdb_connection_pool_overflow,
        )

    redis_connection = redis.StrictRedis(
        host=redis_host, port=redis_port, password=redis_password
    )
    thread_pool = ThreadPoolExecutor(flowdb_connection_pool_size)
    conn.available_dates

    print(f"FlowMachine version: {flowmachine.__version__}")

    print(
        f"Flowdb running on: {flowdb_host}:{flowdb_port}/flowdb (connecting user: {flowdb_user})"
    )
    return conn, thread_pool, redis_connection
