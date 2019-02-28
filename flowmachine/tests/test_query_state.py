# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the query state machine.
"""
from unittest.mock import Mock

import time

import pytest

import flowmachine
from flowmachine.core import Query
from flowmachine.core.query_state import QueryStateMachine, QueryState


@pytest.mark.parametrize(
    "blocking_state", [QueryState.EXECUTING, QueryState.RESETTING, QueryState.QUEUED]
)
def test_blocks(blocking_state, monkeypatch, dummy_redis):
    """Test that states which alter the executing state of the query block."""
    state_machine = QueryStateMachine(dummy_redis, "DUMMY_QUERY_ID")
    dummy_redis._store[state_machine.state_machine._name] = blocking_state.encode()
    monkeypatch.setattr(
        flowmachine.core.query_state, "_sleep", Mock(side_effect=BlockingIOError)
    )

    with pytest.raises(BlockingIOError):
        state_machine.has_finished_operating()


def test_no_limit_on_blocks(monkeypatch):
    """Test that even with a large number of queries, starting a store op will block calls to get_query."""

    class DummyQuery(Query):
        def ___init__(self, x):
            self.x = x
            super().__init__()

        @property
        def column_names(self):
            return []

        def _make_query(self):
            time.sleep(30)

    flowmachine.connect()
    dummies = [DummyQuery(x) for x in range(50)]
    [dummy.store() for dummy in dummies]
    monkeypatch.setattr(
        flowmachine.core.query_state, "_sleep", Mock(side_effect=BlockingIOError)
    )

    with pytest.raises(BlockingIOError):
        dummies[-1].get_query()


def test_non_blocking(monkeypatch):
    """Test that states which do not alter the executing state of the query don't block."""

    class DummyQuery(Query):
        def ___init__(self, x):
            self.x = x
            super().__init__()

        @property
        def column_names(self):
            return []

        def _make_query(self):
            time.sleep(30)

    flowmachine.connect()
    dummies = [DummyQuery(x) for x in range(50)]
    [dummy.store() for dummy in dummies]
    monkeypatch.setattr(
        flowmachine.core.query_state, "_sleep", Mock(side_effect=BlockingIOError)
    )

    with pytest.raises(BlockingIOError):
        dummies[-1].get_query()


@pytest.mark.parametrize(
    "non_blocking_state, expected_return",
    [
        (QueryState.EXECUTED, True),
        (QueryState.KNOWN, False),
        (QueryState.CANCELLED, False),
        (QueryState.ERRORED, False),
    ],
)
def test_non_blocks(non_blocking_state, expected_return, monkeypatch, dummy_redis):
    """Test that states which don't alter the executing state of the query don't block."""
    state_machine = QueryStateMachine(dummy_redis, "DUMMY_QUERY_ID")
    dummy_redis._store[state_machine.state_machine._name] = non_blocking_state.encode()
    monkeypatch.setattr(
        flowmachine.core.query_state, "_sleep", Mock(side_effect=BlockingIOError)
    )

    assert expected_return == state_machine.has_finished_operating()
