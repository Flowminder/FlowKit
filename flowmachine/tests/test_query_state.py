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
from flowmachine.core.context import get_redis, get_db, get_interpreter_id
from flowmachine.core.errors.flowmachine_errors import (
    QueryCancelledException,
    QueryErroredException,
    QueryResetFailedException,
)
from flowmachine.core.query_manager import set_managing
from flowmachine.core.query_state import QueryStateMachine, QueryState, QueryEvent
import flowmachine.utils


class DummyQuery(Query):
    """
    Simulate a long running query which will error when it actually hits postgres.

    Parameters
    ----------
    dummy_id : str
        Id to distringuish this query
    sleep_time : int, default 5
        Number of seconds to block in _make_query

    """

    def __init__(self, dummy_id, sleep_time=5):

        self.dummy_id = dummy_id
        self.sleep_time = sleep_time
        super().__init__()

    @property
    def column_names(self):
        return []

    def _make_query(self):
        time.sleep(self.sleep_time)


@pytest.mark.parametrize(
    "blocking_state", [QueryState.EXECUTING, QueryState.RESETTING, QueryState.QUEUED]
)
def test_blocks(blocking_state, monkeypatch, dummy_redis):
    """Test that states which alter the executing state of the query block."""
    state_machine = QueryStateMachine(dummy_redis, "DUMMY_QUERY_ID", get_db().conn_id)
    dummy_redis.set(state_machine.state_machine._name, blocking_state)
    monkeypatch.setattr(
        flowmachine.core.query_state, "_sleep", Mock(side_effect=BlockingIOError)
    )

    with pytest.raises(BlockingIOError):
        state_machine.wait_until_complete()


def test_no_limit_on_blocks(monkeypatch):
    """Test that even with a large number of queries, starting a store op will block calls to get_query."""

    monkeypatch.setattr(
        flowmachine.core.query_state, "_sleep", Mock(side_effect=BlockingIOError)
    )
    dummies = [DummyQuery(dummy_id=x) for x in range(50)]
    [dummy.store() for dummy in dummies]

    with pytest.raises(BlockingIOError):
        dummies[-1].get_query()


def test_non_blocking(monkeypatch):
    """Test that states which do not alter the executing state of the query don't block."""

    dummies = [DummyQuery(dummy_id=x) for x in range(50)]
    [dummy.store() for dummy in dummies]
    monkeypatch.setattr(
        flowmachine.core.query_state, "_sleep", Mock(side_effect=BlockingIOError)
    )

    with pytest.raises(BlockingIOError):
        dummies[-1].get_query()


@pytest.mark.parametrize(
    "non_blocking_state, expected_return",
    [
        (QueryState.COMPLETED, True),
        (QueryState.KNOWN, False),
        (QueryState.CANCELLED, False),
        (QueryState.ERRORED, False),
    ],
)
def test_non_blocks(non_blocking_state, expected_return, monkeypatch, dummy_redis):
    """Test that states which don't alter the executing state of the query don't block."""
    state_machine = QueryStateMachine(dummy_redis, "DUMMY_QUERY_ID", get_db().conn_id)
    dummy_redis.set(state_machine.state_machine._name, non_blocking_state)
    monkeypatch.setattr(
        flowmachine.core.query_state, "_sleep", Mock(side_effect=BlockingIOError)
    )

    try:
        state_machine.wait_until_complete()
    except BlockingIOError:
        pytest.fail("Blocked!")


@pytest.mark.parametrize(
    "start_state, succeeds",
    [
        (QueryState.KNOWN, False),
        (QueryState.CANCELLED, True),
        (QueryState.COMPLETED, False),
        (QueryState.ERRORED, False),
        (QueryState.QUEUED, True),
        (QueryState.RESETTING, False),
        (QueryState.EXECUTING, True),
    ],
)
def test_query_cancellation(start_state, succeeds, dummy_redis):
    """Test the cancel method works as expected."""
    state_machine = QueryStateMachine(dummy_redis, "DUMMY_QUERY_ID", get_db().conn_id)
    dummy_redis.set(state_machine.state_machine._name, start_state)
    set_managing("DUMMY_QUERY_ID")
    state_machine.cancel()
    assert succeeds == state_machine.is_cancelled


@pytest.mark.parametrize(
    "fail_event, expected_exception",
    [
        (QueryEvent.CANCEL, QueryCancelledException),
        (QueryEvent.ERROR, QueryErroredException),
    ],
)
def test_store_exceptions(fail_event, expected_exception):
    """Test that exceptions are raised when watching a store op triggered elsewhere."""
    q = DummyQuery(dummy_id=1, sleep_time=5)
    qsm = QueryStateMachine(get_redis(), q.query_id, get_db().conn_id)
    # Mark the query as having begun executing elsewhere
    qsm.enqueue()
    qsm.execute()
    q_fut = q.store()
    qsm.trigger_event(fail_event)
    with pytest.raises(expected_exception):
        raise q_fut.exception()


def test_drop_query_blocks(monkeypatch):
    """Test that resetting a query's cache will block if that's already happening."""
    monkeypatch.setattr(
        flowmachine.core.query, "_sleep", Mock(side_effect=BlockingIOError)
    )
    q = DummyQuery(dummy_id=1, sleep_time=5)
    qsm = QueryStateMachine(get_redis(), q.query_id, get_db().conn_id)
    # Mark the query as in the process of resetting
    qsm.enqueue()
    qsm.execute()
    qsm.finish()
    qsm.reset()
    with pytest.raises(BlockingIOError):
        q.invalidate_db_cache()


def test_drop_query_errors():
    """Test that resetting a query's cache will error if in a state where that isn't possible."""
    q = DummyQuery(dummy_id=1, sleep_time=5)
    qsm = QueryStateMachine(get_redis(), q.query_id, get_db().conn_id)
    # Mark the query as in the process of resetting
    qsm.enqueue()
    qsm.execute()
    with pytest.raises(QueryResetFailedException):
        q.invalidate_db_cache()
