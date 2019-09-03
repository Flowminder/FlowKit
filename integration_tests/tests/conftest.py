# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from approvaltests.reporters.generic_diff_reporter_factory import (
    GenericDiffReporterFactory,
)
from multiprocessing import Process
from time import sleep

import pytest
import os
import pandas as pd
import zmq

import flowmachine
from flowmachine.core import Connection, Query
from flowmachine.core.cache import reset_cache
import flowmachine.core.server.server
import quart.flask_patch

here = os.path.dirname(os.path.abspath(__file__))
flowkit_toplevel_dir = os.path.join(here, "..", "..")


@pytest.fixture(scope="session")
def logging_config():
    """
    Fixture which configures logging for flowmachine and flowapi.
    Sets the log level to debug.
    """
    from _pytest.monkeypatch import MonkeyPatch

    mpatch = MonkeyPatch()
    mpatch.setenv("FLOWMACHINE_LOG_LEVEL", "debug")
    mpatch.setenv("FLOWAPI_LOG_LEVEL", "debug")
    yield
    mpatch.undo()


@pytest.fixture(scope="session", autouse=True)
def autostart_flowmachine_server(logging_config):
    """
    Starts a flowmachine server in a separate process for the tests to talk to.
    """
    disable_autostart_servers = (
        os.getenv(
            "FLOWKIT_INTEGRATION_TESTS_DISABLE_AUTOSTART_SERVERS", "FALSE"
        ).upper()
        == "TRUE"
    )
    if disable_autostart_servers:
        yield  # need to yield something from either branch of the if statement
    else:
        fm_thread = Process(target=flowmachine.core.server.server.main)
        fm_thread.start()
        yield
        fm_thread.terminate()
        sleep(
            2
        )  # Wait a moment to make sure coverage of subprocess finishes being written


@pytest.fixture(params=["true", "false"])
def start_flowmachine_server_with_or_without_dependency_caching(
    request, logging_config, monkeypatch
):
    """
    Starts a FlowMachine server in a separate process, with function scope
    (i.e. a server will be started and stopped for each test that uses this fixture).
    Tests using this fixture will run twice: once with dependency caching disabled,
    and again with dependency caching enabled.
    """
    # Ensure this server runs on a different port from the session-scoped server
    main_zmq_port = os.getenv("FLOWMACHINE_PORT", "5555")
    monkeypatch.setenv("FLOWMACHINE_PORT", str(int(main_zmq_port) + 1))
    # Turn dependency caching on or off
    monkeypatch.setenv("FLOWMACHINE_SERVER_DISABLE_DEPENDENCY_CACHING", request.param)
    # Start the server
    fm_thread = Process(target=flowmachine.core.server.server.main)
    fm_thread.start()
    yield
    fm_thread.terminate()
    sleep(2)  # Wait a moment to make sure coverage of subprocess finishes being written


@pytest.fixture(scope="session", autouse=True)
def autostart_flowapi_server(logging_config, flowapi_port):
    """
    Starts a FlowAPI server in a separate process for the tests to talk to.
    """
    disable_autostart_servers = (
        os.getenv(
            "FLOWKIT_INTEGRATION_TESTS_DISABLE_AUTOSTART_SERVERS", "FALSE"
        ).upper()
        == "TRUE"
    )
    if disable_autostart_servers:
        yield  # need to yield something from either branch of the if statement
    else:
        import hypercorn.__main__

        api_thread = Process(
            target=hypercorn.__main__.main,
            args=(["--bind", f"0.0.0.0:{flowapi_port}", "flowapi.main:create_app()"],),
        )
        api_thread.start()
        sleep(2)
        yield
        api_thread.terminate()
        sleep(
            2
        )  # Wait a moment to make sure coverage of subprocess finishes being written


@pytest.fixture(scope="session")
def flowapi_host():
    """
    Return the host on which flowapi is running. This is either the value of
    the environment variable FLOWAPI_HOST or else 'localhost'.
    """
    return os.getenv("FLOWAPI_HOST", "localhost")


@pytest.fixture(scope="session")
def flowapi_port():
    """
    Return the port on which flowapi is running. This is either the value of
    the environment variable FLOWAPI_PORT or else 9090.
    """
    return os.getenv("FLOWAPI_PORT", "9090")


@pytest.fixture
def flowapi_url(flowapi_host, flowapi_port):
    """
    Fixture for getting the url where FlowAPI is running. This is
    constructed as "http://<flowapi_host>:<flowapi_port>", where the
    host and port are provided by the `floawpi_host` and `flowapi_port`
    fixtures (which read the values from the environment variables
    `FLOWAPI_HOST` and `FLOWAPI_PORT`, respectively).
    """
    return f"http://{flowapi_host}:{flowapi_port}"


@pytest.fixture(scope="session")
def zmq_host():
    """
    Return the host on which zmq is running. This is either the value of
    the environment variable FLOWMACHINE_HOST or else 'localhost'.
    """
    return os.getenv("FLOWMACHINE_HOST", "localhost")


@pytest.fixture(scope="session")
def zmq_port():
    """
    Return the port on which zmq is running. This is either the value of
    the environment variable FLOWMACHINE_PORT or else 5555.
    """
    return os.getenv("FLOWMACHINE_PORT", "5555")


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
    POSTGRES_USER = os.getenv("POSTGRES_USER", "flowdb")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "flowflow")
    FLOWDB_HOST = os.getenv("FLOWDB_HOST", "localhost")
    FLOWDB_PORT = os.getenv("FLOWDB_PORT", "9000")
    conn_str = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{FLOWDB_HOST}:{FLOWDB_PORT}/flowdb"

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
    reset_cache_schema(fm_conn, redis_instance=Query.redis)
    delete_all_redis_keys(redis_instance=Query.redis)


def delete_all_redis_keys(redis_instance):
    """
    Delete all keys from the given redis instance.
    """
    redis_instance.flushall()


def reset_cache_schema(fm_conn, redis_instance):
    """
    Reset the cache schema in flowdb by removing any tables for cached queries,
    and truncating the internal tables 'cache.cached' and 'cache.dependencies'
    so that they are empty.
    """
    print("Killing any queries still running...")
    with fm_conn.engine.begin():  # Kill any running queries
        fm_conn.engine.execute(
            f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND application_name='{fm_conn.app_name}';"
        )
    print("[DDD] Recreating cache schema... ", end="", flush=True)
    reset_cache(fm_conn, redis_instance, protect_table_objects=False)
    print("Done.")


@pytest.fixture
def get_dataframe(fm_conn):
    yield lambda query: pd.read_sql_query(query.get_query(), con=fm_conn.engine)


@pytest.fixture(scope="session")
def diff_reporter():
    diff_reporter_factory = GenericDiffReporterFactory()
    diff_reporter_factory.load(
        os.path.join(flowkit_toplevel_dir, "approvaltests_diff_reporters.json")
    )
    return diff_reporter_factory.get_first_working()
