# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Context variables for Flowmachine to talk to FlowDB and Redis, and a common thread pool for
managing queries.
"""

from contextvars import ContextVar, copy_context
from concurrent.futures import Executor, Future
from contextlib import contextmanager
from typing import Callable, NamedTuple

from redis import StrictRedis

from flowmachine.core import Connection
from flowmachine.core.errors import NotConnectedError

try:
    db
except NameError:
    db = ContextVar("db")

try:
    redis_connection
except NameError:
    redis_connection = ContextVar("redis")
try:
    executor
except NameError:
    executor = ContextVar("executor")
try:
    action_request
except NameError:
    action_request = ContextVar("action_request")

_jupyter_context = (
    dict()
)  # Required as a workaround for https://github.com/ipython/ipython/issues/11565

_is_notebook = False
try:
    shell = get_ipython().__class__.__name__
    if shell == "ZMQInteractiveShell":
        _is_notebook = True  # Jupyter notebook or qtconsole
    elif shell == "TerminalInteractiveShell":
        _is_notebook = False  # Terminal running IPython
    else:
        _is_notebook = False  # Other type (?)
except NameError:
    _is_notebook = False  # Probably standard Python interpreter


def get_action_request() -> NamedTuple:
    """
    Get the current action request if there is one.

    Returns
    -------
    NamedTuple
    """
    return action_request.get()


def get_db() -> Connection:
    """
    Get the current context's database connection.

    Returns
    -------
    Connection

    Raises
    ------
    NotConnectedError
        If there is not a connection for this context
    """
    try:
        if _is_notebook:
            return db.get(_jupyter_context["db"])
        else:
            return db.get()
    except (LookupError, KeyError):
        raise NotConnectedError


def get_redis() -> StrictRedis:
    """
    Get the current context's redis client.

    Returns
    -------
    StrictRedis

    Raises
    ------
    NotConnectedError
        If there is not a redis client for this context
    """
    try:
        if _is_notebook:
            return redis_connection.get(_jupyter_context["redis_connection"])
        else:
            return redis_connection.get()
    except (LookupError, KeyError):
        raise NotConnectedError


def get_executor() -> Executor:
    """
    Get the current context's executor pool.

    Returns
    -------
    Executor

    Raises
    ------
    NotConnectedError
        If there is not a pool for this context
    """
    try:
        if _is_notebook:
            return executor.get(_jupyter_context["executor"])
        else:
            return executor.get()
    except (LookupError, KeyError):
        raise NotConnectedError


def submit_to_executor(func: Callable, *args, **kwargs) -> Future:
    """
    Submit a callable to the current context's executor pool and
    get back a future to monitor execution.

    Parameters
    ----------
    func : Callable
        Callable to be executed
    args
        Positional arguments to func
    kwargs
        Keyword arguments to func

    Returns
    -------
    Future

    """
    current_context = copy_context()
    return get_executor().submit(current_context.run, func, *args, **kwargs)


def bind_context(
    connection: Connection, executor_pool: Executor, redis_conn: StrictRedis
):
    """
    Set the current context's connection, executor and redis connection, replacing
    any that were previously set.

    Parameters
    ----------
    connection : Connection
        Connection to set
    executor_pool : Executor
        Executor to be the new pool
    redis_conn : StrictRedis
        Redis client

    """
    if _is_notebook:
        global _jupyter_context
        _jupyter_context["db"] = connection
        _jupyter_context["executor"] = executor_pool
        _jupyter_context["redis_connection"] = redis_conn
    else:
        db.set(connection)
        executor.set(executor_pool)
        redis_connection.set(redis_conn)


@contextmanager
def action_request_context(action: NamedTuple):
    action_request_token = action_request.set(action)
    try:
        yield
    finally:
        action_request.reset(action_request_token)


@contextmanager
def context(connection: Connection, executor_pool: Executor, redis_conn: StrictRedis):
    """
    Context manager which can be used to temporarily provide a connection, redis client
    and pool.

    Parameters
    ----------
    connection : Connection
        Connection which will be used within this context
    executor_pool : Executor
        Executor pool which will be used within this context
    redis_conn : StrictRedis
        Redis client which will be used within this context
    """
    db_token = db.set(connection)
    redis_token = redis_connection.set(redis_conn)
    executor_token = executor.set(executor_pool)
    try:
        yield
    finally:
        db.reset(db_token)
        redis_connection.reset(redis_token)
        executor.reset(executor_token)
