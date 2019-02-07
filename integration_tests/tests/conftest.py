# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from datetime import timedelta

import pytest
import os

import requests
from .utils import make_token

import flowmachine
from flowmachine.core import Connection, Query
from flowmachine.core.cache import reset_cache


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
    secret = os.getenv("JWT_SECRET_KEY")
    if secret is None:
        raise EnvironmentError("JWT_SECRET_KEY environment variable not set.")

    def token_maker(claims):
        return make_token("test", secret, timedelta(seconds=90), claims)

    return token_maker


@pytest.fixture(scope="session")
def zmq_url():
    """
    Return the URL where to connect to zeromq when running the tests.

    By default this is "tcp://localhost:5555" but the tests can be run
    against a flowmachine at a different URL by setting the environment
    variable FLOWMACHINE_ZMQ_URL.
    """
    host = os.getenv("FLOWMACHINE_ZMQ_HOST", "localhost")
    port = os.getenv("FLOWMACHINE_ZMQ_PORT", "5555")
    return f"tcp://{host}:{port}"


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
