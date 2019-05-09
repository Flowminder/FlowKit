# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
import os
from json import JSONDecodeError

import asyncpg
import pytest
import zmq
from _pytest.capture import CaptureResult

from flowapi.main import create_app
from asynctest import MagicMock, Mock, CoroutineMock
from zmq.asyncio import Context
from asyncio import Future


def async_return(result):
    """
    Return an object which can be used in an 'await' expression.
    """
    f = Future()
    f.set_result(result)
    return f


@pytest.fixture
def audience():
    """
    Fixture to set the audience for generated tokens to this server.
    """
    return os.environ["FLOWAPI_IDENTIFIER"]


@pytest.fixture
def json_log(capsys):
    def parse_json():
        log_output = capsys.readouterr()
        stdout = []
        stderr = []
        for l in log_output.out.split("\n"):
            if l == "":
                continue
            try:
                stdout.append(json.loads(l))
            except JSONDecodeError:
                stdout.append(l)
        for l in log_output.err.split("\n"):
            if l == "":
                continue
            try:
                stderr.append(json.loads(l))
            except JSONDecodeError:
                stderr.append(l)
        return CaptureResult(stdout, stderr)

    return parse_json


@pytest.fixture
def dummy_zmq_server(monkeypatch):
    """
    A fixture which provides a dummy zero mq
    socket which records the json it is asked
    to send.

    Parameters
    ----------
    monkeypatch

    Yields
    ------
    asynctest.CoroutineMock
        Coroutine mocking for the recv_json method of the socket

    """
    dummy = Mock()
    dummy.return_value.socket.return_value.recv_json = CoroutineMock()

    monkeypatch.setattr(zmq.asyncio.Context, "instance", dummy)
    yield dummy.return_value.socket.return_value.recv_json


@pytest.fixture
def dummy_db_pool(monkeypatch):
    """
    A fixture which provides a mock database connection.

    Yields
    ------
    MagicMock
        The mock db connection that will be used
    """
    dummy = MagicMock()

    # A MagicMock can't be used in an 'await' expression,
    # so we need to set the return value of connection.set_type_codec
    # (awaited in stream_result_as_json())
    dummy.acquire.return_value.__aenter__.return_value.set_type_codec.return_value = async_return(
        Mock()
    )

    async def f(*args, **kwargs):
        return dummy

    monkeypatch.setattr(asyncpg, "create_pool", f)
    yield dummy


@pytest.fixture
def app(monkeypatch, tmpdir, dummy_db_pool):
    monkeypatch.setenv("FLOWAPI_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("FLOWMACHINE_HOST", "localhost")
    monkeypatch.setenv("FLOWMACHINE_PORT", "5555")
    monkeypatch.setenv("FLOWAPI_FLOWDB_USER", "flowapi")
    monkeypatch.setenv("FLOWDB_HOST", "localhost")
    monkeypatch.setenv("FLOWDB_PORT", "5432")
    monkeypatch.setenv("FLOWAPI_FLOWDB_PASSWORD", "foo")
    monkeypatch.setenv("JWT_SECRET_KEY", "secret")
    current_app = create_app()
    yield current_app.test_client(), dummy_db_pool, tmpdir, current_app
