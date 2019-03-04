# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from datetime import timedelta
from multiprocessing import Process
from time import sleep

import pytest
import os

import requests

import flowmachine


from flowmachine.core import Connection, Query
from flowmachine.core.cache import reset_cache
import flowmachine.core.server.server
import quart.flask_patch


@pytest.fixture(scope="session", autouse=True)
def flowmachine_server():
    """
    Starts a flowmachine server in a separate thread for the tests to talk to.
    """
    fm_thread = Process(target=flowmachine.core.server.server.main)
    fm_thread.start()
    yield
    fm_thread.terminate()
    sleep(2)  # Wait a moment to make sure coverage of subprocess finishes being written


@pytest.fixture(scope="session", autouse=True)
def flowapi_server():
    import hypercorn.__main__

    import os
    import sys

    sys.path.insert(
        0,
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "flowapi")),
    )

    api_thread = Process(
        target=hypercorn.__main__.main,
        args=(["--bind", "0.0.0.0:9090", "app.main:create_app()"],),
    )
    api_thread.start()
    sleep(2)
    yield
    api_thread.terminate()
    sleep(2)  # Wait a moment to make sure coverage of subprocess finishes being written


@pytest.fixture
def api_host():
    """
    Fixture for getting the url of the available API server.
    Primarily to allow the tests to run locally, or in a docker container.

    Returns
    -------
    str
        URL of running API container
    """
    in_docker_host = "http://flowapi:9090"
    try:
        requests.get(in_docker_host, verify=False)
        return in_docker_host
    except requests.ConnectionError:
        return "http://localhost:9090"


@pytest.fixture
def access_token_builder():
    """
    Fixture which builds short-life access tokens.

    Returns
    -------
    function
        Function which returns a token encoding the specified claims.
    """
    from .utils import make_token

    secret = os.getenv("JWT_SECRET_KEY")
    if secret is None:
        raise EnvironmentError("JWT_SECRET_KEY environment variable not set.")

    def token_maker(claims):
        return make_token("test", secret, timedelta(seconds=90), claims)

    return token_maker


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
    This is constructed as "tcp://<zmq_host>:<zmq_port>", where the
    host and port are provided by the `zmq_host` and `zmq_port`
    fixtures (which read the values from the environment variables
    `FLOWMACHINE_ZMQ_HOST` and `FLOWMACHINE_ZMQ_PORT`, respectively).
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
