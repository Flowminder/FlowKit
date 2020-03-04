# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
from concurrent.futures.thread import ThreadPoolExecutor
from json import JSONDecodeError
from unittest.mock import Mock

from _pytest.capture import CaptureResult
from asynctest import Mock as AMock

import pytest
import zmq
from flowmachine_core.core.context import context, redis_connection
from flowmachine_server.server_config import FlowmachineServerConfig


@pytest.fixture
def json_log(caplog):
    def parse_json():
        loggers = dict(debug=[], query_run_log=[])
        for logger, level, msg in caplog.record_tuples:
            if msg == "":
                continue
            try:
                parsed = json.loads(msg)
                loggers[parsed["logger"].split(".")[1]].append(parsed)
            except JSONDecodeError:
                loggers["debug"].append(msg)
        return CaptureResult(err=loggers["debug"], out=loggers["query_run_log"])

    return parse_json


class DummyRedis:
    """
    Drop-in replacement for redis.
    """

    def __init__(self):
        self._store = {}
        self.allow_flush = True

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

    def flushdb(self):
        if (
            self.allow_flush
        ):  # Set allow_flush attribute to False to simulate concurrent writes
            self._store = {}


@pytest.fixture
def dummy_redis(flowmachine_connect):
    dummy_redis = DummyRedis()
    token = redis_connection.set(dummy_redis)
    print("Replaced redis with dummy redis.")
    yield dummy_redis
    redis_connection.reset(token)


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


@pytest.fixture(autouse=True)
def flowmachine_connect():
    """Overrides the flowmachine-queries-server connection fixture to replace all applicable parts with mocks."""
    with context(Mock(), ThreadPoolExecutor(), Mock(name="connect_redis")):
        print("Replacing connections with mocks.")
        yield


@pytest.fixture(scope="session")
def server_config():
    """
    Returns a FlowmachineServerConfig object, required as a parameter for server functions and action handlers.
    """
    return FlowmachineServerConfig(
        port=5555,
        debug_mode=False,
        store_dependencies=True,
        cache_pruning_frequency=86400,
        cache_pruning_timeout=600,
        server_thread_pool=ThreadPoolExecutor(),
    )
