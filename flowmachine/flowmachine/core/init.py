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

import logging
import os
import warnings
from concurrent.futures import ThreadPoolExecutor
from logging.handlers import TimedRotatingFileHandler

import redis

import flowmachine
from typing import Union
from flowmachine.utils import getsecret
from . import Connection, Query

import structlog

logger = structlog.get_logger(__name__)


def connect(
    log_level: Union[str, None] = None,
    write_log_file: Union[bool, None] = None,
    db_port: Union[int, None] = None,
    db_user: Union[str, None] = None,
    db_pass: Union[str, None] = None,
    db_host: Union[str, None] = None,
    db_connection_pool_size: Union[int, None] = None,
    db_connection_pool_overflow: Union[int, None] = None,
    redis_host: Union[str, None] = None,
    redis_port: Union[int, None] = None,
    redis_password: Union[str, None] = None,
    conn: Union[Connection, None] = None,
) -> Connection:
    """
    Connects flowmachine to a database, and performs initial set-up routines.
    You may provide a Settings object here, which can specify the database
    you wish to connect to, logging behaviour, available tables and so on.

    Parameters
    ----------
    log_level : str, default "error"
        Level to log at
    write_log_file : bool, default False
        If True, logging output will be written to the file 'flowmachine-debug.log'
        in the directory '/var/log/flowmachine/' (or the directory given by the
        environment variable 'LOG_DIRECTORY' if it is set). Log files are rotated
        at midnight.
    db_port : int, default 9000
        Port number to connect to flowdb
    db_user : str, default "analyst"
        Name of user to connect to flowdb as
    db_pass : str, default "foo"
        Password to connect to flowdb
    db_host : str, default "localhost"
        Hostname of flowdb server
    db_connection_pool_size : int, default 5
        Default number of database connections to use
    db_connection_pool_overflow : int, default 1
        Number of extra database connections to allow
    redis_host : str, default "localhost"
        Hostname for redis server.
    redis_port : int, default 6379
        Port the redis server is available on
    redis_password : str, default "fm_redis"
        Password for the redis instance
    conn : flowmachine.core.Connection
        Optionally provide an existing Connection object to use, overriding any the db options specified here.

    Returns
    -------
    Connection

    Notes
    -----
    All parameters can also be provided as environment variables, named the same
    but in uppercase, e.g. `env LOG_LEVEL=error` instead of `connect(log_level="error")`.
    If a parameter is provided, and an environment variable is set,
    then the provided value is used. If neither is provided, the defaults as given
    in the docstring are used.

    Parameters can _also_ be set using Docker secrets, in which case a file with the name
    of the parameter in upper case should be present at /run/secrets/THE_PARAM.
    If a secret is available, the secret takes precedence over both the environment variable, and
    the default.
    """

    log_level = (
        getsecret("LOG_LEVEL", os.getenv("LOG_LEVEL", "error"))
        if log_level is None
        else log_level
    )
    write_log_file = (
        (
            "TRUE"
            == getsecret("WRITE_LOG_FILE", os.getenv("WRITE_LOG_FILE", "FALSE")).upper()
        )
        if write_log_file is None
        else write_log_file
    )
    db_port = int(
        getsecret("FLOWDB_PORT", os.getenv("FLOWDB_PORT", 9000))
        if db_port is None
        else db_port
    )
    db_user = (
        getsecret("FLOWDB_USER", os.getenv("FLOWDB_USER", "analyst"))
        if db_user is None
        else db_user
    )
    db_pass = (
        getsecret("FLOWDB_PASS", os.getenv("FLOWDB_PASS", "foo"))
        if db_pass is None
        else db_pass
    )
    db_host = (
        getsecret("FLOWDB_HOST", os.getenv("FLOWDB_HOST", "localhost"))
        if db_host is None
        else db_host
    )
    db_connection_pool_size = (
        int(
            getsecret(
                "DB_CONNECTION_POOL_SIZE", os.getenv("DB_CONNECTION_POOL_SIZE", 5)
            )
        )
        if db_connection_pool_size is None
        else db_connection_pool_size
    )
    db_connection_pool_overflow = int(
        getsecret(
            "DB_CONNECTION_POOL_OVERFLOW", os.getenv("DB_CONNECTION_POOL_OVERFLOW", 1)
        )
        if db_connection_pool_overflow is None
        else db_connection_pool_overflow
    )

    redis_host = (
        getsecret("REDIS_HOST", os.getenv("REDIS_HOST", "localhost"))
        if redis_host is None
        else redis_host
    )
    redis_port = int(
        getsecret("REDIS_PORT", os.getenv("REDIS_PORT", 6379))
        if redis_port is None
        else redis_port
    )
    redis_pw = (
        getsecret("REDIS_PASSWORD_FILE", os.getenv("REDIS_PASSWORD", "fm_redis"))
        if redis_password is None
        else redis_password
    )

    try:
        Query.connection
        warnings.warn("FlowMachine already started. Ignoring.")
    except AttributeError:
        _init_logging(log_level, write_log_file)
        if conn is None:
            conn = Connection(
                host=db_host,
                port=db_port,
                user=db_user,
                password=db_pass,
                database="flowdb",
                pool_size=db_connection_pool_size,
                overflow=db_connection_pool_overflow,
            )
        Query.connection = conn

        Query.redis = redis.StrictRedis(
            host=redis_host, port=redis_port, password=redis_pw
        )
        _start_threadpool(thread_pool_size=db_connection_pool_size)

        print(f"FlowMachine version: {flowmachine.__version__}")

        print(
            f"Flowdb running on: {db_host}:{db_port}/flowdb (connecting user: {db_user})"
        )
    return Query.connection


def _init_logging(log_level, write_log_file):
    """

    Parameters
    ----------
    log_level : str
        Level to emit logs at
    write_log_file : bool
        If True, logging output will be written to the file 'flowmachine-debug.log'
        in the directory '/var/log/flowmachine/' (or the directory given by the
        environment variable 'LOG_DIRECTORY' if it is set). Log files are rotated
        at midnight.

    Returns
    -------

    """
    try:
        log_level = logging.getLevelName(log_level.upper())
        log_level + 1
    except (AttributeError, TypeError):
        log_level = logging.ERROR
    true_log_level = logging.getLevelName(log_level)
    logger = logging.getLogger("flowmachine")
    logger.setLevel(true_log_level)
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    logger.addHandler(ch)
    logger.info(f"Logger created with level {true_log_level}")
    if write_log_file:
        log_root = os.getenv("LOG_DIRECTORY", "/var/log/flowmachine/")
        if not os.path.exists(log_root):
            logger.info(
                f"Creating log_root directory because it does not exist: {log_root}"
            )
            os.makedirs(log_root)
        log_file = os.path.join(log_root, "flowmachine-debug.log")
        fh = TimedRotatingFileHandler(log_file, when="midnight")
        fh.setLevel(true_log_level)
        logger.addHandler(fh)
        logger.info(f"Added log file handler, logging to {log_file}")


def _start_threadpool(*, thread_pool_size=None):
    """
    Start the threadpool flowmachine uses for executing queries
    asynchronously.

    Parameters
    ----------
    thread_pool_size : int
        Size of thread pool to use.

    See Also
    --------
    ThreadPoolExecutor

    """
    Query.thread_pool_executor = ThreadPoolExecutor(thread_pool_size)
