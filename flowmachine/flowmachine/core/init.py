# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
This module provides initial setup routines for flowmachine. From a user
perspective, only the `start` method is relevant.

From a developer perspective, this is where one-time operations
should live - for example creating postgres functions.
"""


import logging
import os
import warnings
from concurrent.futures import ThreadPoolExecutor
from logging.handlers import TimedRotatingFileHandler

import redis

import flowmachine
from flowmachine.utils.utils import getsecret
from . import Connection, Query

logger = logging.getLogger("flowmachine").getChild(__name__)


def connect(
    log_level=None,
    log_file=None,
    db_port=None,
    db_user=None,
    db_pw=None,
    db_host=None,
    db_name=None,
    pool_size=None,
    pool_overflow=None,
    redis_host=None,
    redis_port=None,
    redis_password=None,
    conn=None,
):
    """
    Connects flowmachine to a database, and performs initial set-up routines.
    You may provide a Settings object here, which can specify the database
    you wish to connect to, logging behaviour, available tables and so on.

    Parameters
    ----------
    log_level : str, default "error"
        Level to log at
    log_file : str, default False
        Path to a file to write logs to, log files are rotated at midnight.
    db_port : int, default 9000
        Port number to connect to flowdb
    db_user : str, default "analyst"
        Name of user to connect to flowdb as
    db_pw : str, default "foo"
        Password to connect to flowdb
    db_host : str, default "localhost"
        Hostname of flowdb server
    db_name : str, default "flowdb"
        Name of database to connect to.
    pool_size : int, default 5
    pool_overflow : int, default 1
    redis_host : str, default "localhost"
        Hostname for redis server.
    redis_port : int, default 6379
        Port the redis server is available on
    conn : flowmachine.core.Connection
        Optionally provide an existing Connection object to use, overriding any the db options specified here.

    Returns
    -------
    None

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
    log_file = (
        getsecret("LOG_FILE", os.getenv("LOG_FILE", False))
        if log_file is None
        else log_file
    )
    db_port = int(
        getsecret("DB_PORT", os.getenv("DB_PORT", 9000)) if db_port is None else db_port
    )
    db_user = (
        getsecret("DB_USER", os.getenv("DB_USER", "analyst"))
        if db_user is None
        else db_user
    )
    db_pw = getsecret("DB_PW", os.getenv("DB_PW", "foo")) if db_pw is None else db_pw
    db_host = (
        getsecret("DB_HOST", os.getenv("DB_HOST", "localhost"))
        if db_host is None
        else db_host
    )
    db_name = (
        getsecret("DB_NAME", os.getenv("DB_NAME", "flowdb"))
        if db_name is None
        else db_name
    )
    pool_size = (
        int(getsecret("POOL_SIZE", os.getenv("POOL_SIZE", 5)))
        if pool_size is None
        else pool_size
    )
    pool_overflow = int(
        getsecret("POOL_OVERFLOW", os.getenv("POOL_OVERFLOW", 1))
        if pool_overflow is None
        else pool_overflow
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
        _init_logging(log_level, log_file)
        if conn is None:
            conn = Connection(
                db_port, db_user, db_pw, db_host, db_name, pool_size, pool_overflow
            )
        Query.connection = conn

        Query.redis = redis.StrictRedis(
            host=redis_host, port=redis_port, password=redis_pw
        )
        _start_threadpool(pool_size)

        print(f"FlowMachine version: {flowmachine.__version__}")

        print(
            f"Flowdb running on: {db_host}:{db_port}/{db_name} (connecting user: {db_user})"
        )
    return Query.connection


def _init_logging(log_level, log_file):
    """

    Parameters
    ----------
    log_level : str
        Level to emit logs at
    log_file : str
        Path to file to write logs to

    Returns
    -------

    """
    try:
        log_level = logging.getLevelName(log_level.upper())
        log_level + 1
    except (AttributeError, TypeError):
        log_level = logging.ERROR
    true_log_level = logging.getLevelName(log_level)
    formatter = logging.Formatter(
        "%(asctime)s %(name)s %(levelname)s %(threadName)s %(message)s"
    )
    logger = logging.getLogger("flowmachine")
    logger.setLevel(true_log_level)
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.info(f"Logger created with level {true_log_level}")
    if log_file:
        fh = TimedRotatingFileHandler(log_file, when="midnight")
        fh.setLevel(true_log_level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        logger.info(f"Added log file handler, logging to {log_file}")


def _start_threadpool(pool_size=None):
    """
    Start the threadpool flowmachine uses for executing queries
    asynchronously.

    Parameters
    ----------
    pool_size : int
        Size of thread pool to use.

    See Also
    --------
    ThreadPoolExecutor

    """
    Query.tp = ThreadPoolExecutor(pool_size)
