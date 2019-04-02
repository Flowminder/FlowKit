# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
from json import JSONDecodeError

from _pytest.capture import CaptureResult
from unittest.mock import Mock
from asynctest import Mock as AMock

import pytest
import zmq
from flowmachine.core import Query


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
