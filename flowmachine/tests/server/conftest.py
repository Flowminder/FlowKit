# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from unittest.mock import Mock
from asynctest import Mock as AMock

import pytest
import zmq
from flowmachine.core import Query


@pytest.fixture
def dummy_zmq_server(monkeypatch):
    """
    A fixture which provides a dummy zero mq
    socket which records the json it is asked
    to send and monkeypatches the zmq asyncio context
    to return it.

    Parameters
    ----------
    monkeypatch

    Yields
    ------
    asynctest.Mock
        The dummy zeromq socket

    """
    dummy = AMock()
    dummy.socket.return_value = dummy

    def f(*args, **kwargs):
        print("Making dummy zmq.")
        return dummy

    monkeypatch.setattr(zmq.asyncio.Context, "instance", f)
    yield dummy


@pytest.fixture(scope="session", autouse=True)
def flowmachine_connect():
    """Overrides the flowmachine connection fixture to replace all applicable parts with mocks."""
    Query.connection = Mock()
    Query.redis = Mock()
    print("Replacing connections with mocks.")


class DummyRedis:
    """
    Drop-in replacement for redis.
    """

    def __init__(self):
        self._store = {}

    def setnx(self, name, val):
        if name not in self._store:
            self._store[name] = val.encode()

    def eval(self, script, numkeys, name, event):
        current_value = self._store[name]
        try:
            self._store[name] = self._store[event][current_value]
            return self._store[name], current_value
        except KeyError:
            return current_value, None

    def hset(self, key, current, next):
        try:
            self._store[key][current.encode()] = next.encode()
        except KeyError:
            self._store[key] = {current.encode(): next.encode()}

    def set(self, key, value):
        self._store[key] = value.encode()

    def get(self, key):
        return self._store.get(key, None)

    def keys(self):
        return sorted(self._store.keys())


@pytest.fixture(scope="function")
def dummy_redis():
    return DummyRedis()
