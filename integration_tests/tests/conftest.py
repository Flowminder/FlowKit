# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from approvaltests.reporters.generic_diff_reporter_factory import (
    GenericDiffReporterFactory,
)
from datetime import timedelta
from multiprocessing import Process
from time import sleep

import pytest
import os
import requests
import zmq

import flowmachine
from flowmachine.core import Connection, Query
from flowmachine.core.cache import reset_cache
import flowmachine.core.server.server
import quart.flask_patch


@pytest.fixture(scope="session")
def logging_config(tmpdir_factory):
    """
    Fixture which configures logging for flowmachine and flowapi.
    Creates a temporary directory for log files to be written to, and sets the log level to debug.
    """
    tmpdir = tmpdir_factory.mktemp("logs")
    from _pytest.monkeypatch import MonkeyPatch

    mpatch = MonkeyPatch()
    mpatch.setenv("LOG_DIRECTORY", str(tmpdir))
    mpatch.setenv("LOG_LEVEL", "debug")
    print(f"Logs will be written to {tmpdir}")
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


@pytest.fixture(scope="session", autouse=True)
def autostart_flowapi_server(logging_config):
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
            args=(["--bind", "0.0.0.0:9090", "flowapi.main:create_app()"],),
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
def zmq_url(zmq_host, zmq_port):
    """
    Return the URL where to connect to zeromq when running the tests.
    This is constructed as "tcp://<zmq_host>:<zmq_port>", where the
    host and port are provided by the `zmq_host` and `zmq_port`
    fixtures (which read the values from the environment variables
    `FLOWMACHINE_HOST` and `FLOWMACHINE_PORT`, respectively).
    """
    return f"tcp://{zmq_host}:{zmq_port}"


@pytest.fixture
def send_zmq_message_and_receive_reply(zmq_host, zmq_port):
    def send_zmq_message_and_receive_reply_impl(msg):
        """
        Helper function to send JSON messages to the flowmachine server (via zmq) and receive a reply.

        This is mainly useful for interactive testing and debugging.

        Parameters
        ----------
        msg : dict
            Dictionary representing a valid zmq message.

        Example
        -------
        >>> msg = {"action": "ping"}
        >>> send_zmq_message_and_receive_reply(msg)
        {"status": "success", "msg": "pong"}
        """

        context = zmq.Context.instance()
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://{zmq_host}:{zmq_port}")
        print(f"Sending message: {msg}")
        socket.send_json(msg)
        reply = socket.recv_json()
        return reply

    return send_zmq_message_and_receive_reply_impl


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


@pytest.fixture(scope="session")
def diff_reporter():
    diff_reporter_factory = GenericDiffReporterFactory()
    return diff_reporter_factory.get("opendiff")
