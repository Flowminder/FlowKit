# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from concurrent.futures.thread import ThreadPoolExecutor
from unittest.mock import Mock
from asynctest import Mock as AMock

import pytest
import zmq
from flowmachine.core.context import context
from flowmachine.core.server.server_config import FlowmachineServerConfig


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
    """Overrides the flowmachine connection fixture to replace all applicable parts with mocks."""
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
        event_loop="asyncio",
    )
