# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
from json import JSONDecodeError

import asyncpg
import pytest
import pytest_asyncio
import zmq
from _pytest.capture import CaptureResult

from flowapi.main import create_app
from asynctest import MagicMock, Mock, CoroutineMock
from zmq.asyncio import Context
from collections import namedtuple

TestApp = namedtuple("TestApp", ["client", "db_pool", "tmpdir", "app", "log_capture"])
CaptureResult = namedtuple("CaptureResult", ["debug", "access"])


@pytest.fixture
def json_log(caplog):
    def parse_json():
        loggers = dict(debug=[], access=[], query=[])
        for logger, level, msg in caplog.record_tuples:
            if msg == "":
                continue
            try:
                parsed = json.loads(msg)
                loggers[parsed["logger"].split(".")[1]].append(parsed)
            except JSONDecodeError:
                loggers["debug"].append(msg)
        return CaptureResult(loggers["debug"], loggers["access"])

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
    # so we need to make connection.set_type_codec a CoroutineMock
    # (awaited in stream_result_as_json())
    dummy.acquire.return_value.__aenter__.return_value.set_type_codec = CoroutineMock()

    async def f(*args, **kwargs):
        return dummy

    monkeypatch.setattr(asyncpg, "create_pool", f)
    yield dummy


@pytest_asyncio.fixture
async def app(monkeypatch, tmpdir, dummy_db_pool, json_log):
    monkeypatch.setenv("FLOWAPI_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("FLOWMACHINE_HOST", "localhost")
    monkeypatch.setenv("FLOWMACHINE_PORT", "5555")
    monkeypatch.setenv("FLOWAPI_FLOWDB_USER", "flowapi")
    monkeypatch.setenv("FLOWDB_HOST", "localhost")
    monkeypatch.setenv("FLOWDB_PORT", "5432")
    monkeypatch.setenv("FLOWAPI_FLOWDB_PASSWORD", "foo")
    current_app = create_app()
    await current_app.startup()
    return TestApp(
        current_app.test_client(), dummy_db_pool, tmpdir, current_app, json_log
    )
