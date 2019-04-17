# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
from json import JSONDecodeError

import asyncpg
import pytest
import zmq
from _pytest.capture import CaptureResult

from flowapi.main import create_app
from asynctest import MagicMock, Mock, CoroutineMock
from datetime import timedelta
from zmq.asyncio import Context
from .utils import make_token
from asyncio import Future


def async_return(result):
    """
    Return an object which can be used in an 'await' expression.
    """
    f = Future()
    f.set_result(result)
    return f


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
def access_token_builder():
    """
    Fixture which builds short-life access tokens.

    Returns
    -------
    function
        Functions which returns a token encoding the specified claims.
    """

    def token_maker(claims):
        return make_token("test", "secret", timedelta(seconds=10), claims)
        # return encode_access_token(
        #     identity="test",
        #     secret="secret",
        #     algorithm="HS256",
        #     expires_delta=timedelta(seconds=10),
        #     fresh=True,
        #     user_claims=claims,
        #     csrf=False,
        #     identity_claim_key="identity",
        #     user_claims_key="user_claims",
        #     json_encoder=JSONEncoder,
        # )

    return token_maker


@pytest.fixture
def app(monkeypatch, tmpdir, dummy_db_pool):
    monkeypatch.setenv("FLOWAPI_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("FLOWMACHINE_HOST", "localhost")
    monkeypatch.setenv("FLOWAPI_FLOWDB_USER", "flowapi")
    monkeypatch.setenv("FLOWDB_HOST", "localhost")
    monkeypatch.setenv("FLOWAPI_FLOWDB_PASSWORD", "foo")
    monkeypatch.setenv("JWT_SECRET_KEY", "secret")
    current_app = create_app()
    yield current_app.test_client(), dummy_db_pool, tmpdir, current_app
