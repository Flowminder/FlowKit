from contextvars import ContextVar, copy_context
from concurrent.futures import Executor, Future
from contextlib import contextmanager

from redis import StrictRedis

from flowmachine.core import Connection
from flowmachine.core.errors import NotConnectedError

db = ContextVar("db")
redis_connection = ContextVar("redis")
executor = ContextVar("executor")


def get_db() -> Connection:
    try:
        return db.get()
    except LookupError:
        raise NotConnectedError


def get_redis() -> StrictRedis:
    try:
        return redis_connection.get()
    except LookupError:
        raise NotConnectedError


def get_executor() -> Executor:
    try:
        return executor.get()
    except LookupError:
        raise NotConnectedError


def submit_to_executor(func, *args, **kwargs) -> Future:
    current_context = copy_context()
    return get_executor().submit(current_context.run, func, *args, **kwargs)


def bind_context(
    connection: Connection, executor_pool: Executor, redis_conn: StrictRedis
):
    db.set(connection)
    executor.set(executor_pool)
    redis_connection.set(redis_conn)


@contextmanager
def context(connection: Connection, executor_pool: Executor, redis_conn: StrictRedis):
    db_token = db.set(connection)
    redis_token = redis_connection.set(redis_conn)
    executor_token = executor.set(executor_pool)
    try:
        yield
    finally:
        db.reset(db_token)
        redis_connection.reset(redis_token)
        executor.reset(executor_token)
