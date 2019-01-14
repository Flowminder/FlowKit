# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import asyncpg
import pytest
import zmq
from .context import app
from app.main import create_app
from asynctest import MagicMock, Mock, CoroutineMock
from datetime import timedelta
from zmq.asyncio import Context
from .utils import make_token


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

    async def f(*args, **kwargs):
        print("Creating dummy db pool.")
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
    monkeypatch.setenv("LOG_DIRECTORY", str(tmpdir))
    monkeypatch.setenv("SERVER", "localhost")
    monkeypatch.setenv("DB_USER", "flowdb")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PASS", "flowflow")
    monkeypatch.setenv("JWT_SECRET_KEY", "secret")
    monkeypatch.setenv("CONFIG", "test")
    current_app = create_app()
    yield current_app.test_client(), dummy_db_pool, tmpdir, current_app
