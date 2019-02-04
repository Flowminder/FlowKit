import logging
import os
import pytest

import flowmachine
from flowmachine.core import Connection, Query
from flowmachine.core.cache import reset_cache


logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def zmq_host():
    """
    Return the host on which zmq is running. This is either the value of
    the environment variable FLOWMACHINE_ZMQ_HOST or else 'localhost'.
    """
    return os.getenv("FLOWMACHINE_ZMQ_HOST", "localhost")


@pytest.fixture(scope="session")
def zmq_port():
    """
    Return the port on which zmq is running. This is either the value of
    the environment variable FLOWMACHINE_ZMQ_PORT or else 5555.
    """
    return os.getenv("FLOWMACHINE_ZMQ_PORT", "5555")


@pytest.fixture(scope="session")
def zmq_url(zmq_host, zmq_port):
    """
    Return the URL where to connect to zeromq when running the tests.
    This is currently hard-coded to "tcp://localhost:5555".
    """
    return f"tcp://{zmq_host}:{zmq_port}"


@pytest.fixture(scope="session")
def redis():
    """
    Return redis instance to use when running the tests.

    Currently this is hardcoded to Query.redis but this
    fixture avoids hard-coding it in all our tests.
    """
    return Query.redis


@pytest.fixture(scope="session")
def fm_conn():
    """
    Returns a flowmachine Connection object which

    Returns
    -------
    flowmachine.core.connection.Connection
    """
    FLOWDB_HOST = os.getenv("FLOWDB_HOST", "localhost")
    FLOWDB_PORT = os.getenv("FLOWDB_PORT", "9000")
    conn_str = f"postgresql://flowdb:flowflow@{FLOWDB_HOST}:{FLOWDB_PORT}/flowdb"

    fm_conn = Connection(conn_str=conn_str)
    flowmachine.connect(conn=fm_conn)

    yield fm_conn

    fm_conn.close()


@pytest.fixture(scope="function", autouse=True)
def reset_flowdb_and_redis(fm_conn):
    """
    Reset flowdb into a pristine state (by resetting the cache schema)
    and delete any existing keys from redis.

    This fixture is automatically run before every test so that each
    test has a clean database to work with.
    """
    print("[DDD] Resetting flowdb and redis into a pristine state")
    reset_cache_schema(fm_conn)
    delete_all_redis_keys(redis_instance=Query.redis)


def delete_all_redis_keys(redis_instance):
    """
    Delete all keys from the given redis instance.
    """
    redis_instance.flushall()


def reset_cache_schema(fm_conn):
    """
    Reset the cache schema in flowdb by removing any tables for cached queries,
    and truncating the internal tables 'cache.cached' and 'cache.dependencies'
    so that they are empty.
    """
    print("[DDD] Recreating cache schema... ", end="", flush=True)
    reset_cache(fm_conn)
    print("Done.")
