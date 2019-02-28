# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
State machine for queries. Tracks where in a lifecycle a query is to allow
waiting for a query to finish running, and reporting status to the user.
"""

import logging
from enum import Enum

from finist import Finist
from typing import Tuple

from redis import StrictRedis

from flowmachine.utils import _sleep

logger = logging.getLogger("flowmachine").getChild(__name__)


class QueryState(str, Enum):
    """
    Possible states for a query to be in.
    """

    QUEUED = "queued"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    EXECUTING = "executing"
    ERRORED = "is_errored"
    RESETTING = "resetting"
    KNOWN = "known"


class QueryEvent(str, Enum):
    """
    Events that trigger a transition to a new state.
    """

    EXECUTE = "execute"
    FINISH = "finish"
    FINISH_RESET = "finish_reset"
    CANCEL = "cancel"
    RESET = "reset"
    ERROR = "error"
    QUEUE = "queue"


class QueryStateMachine:
    """
    Implements a state machine for a query's lifecycle, backed by redis.

    Each query, once instantiated, is in one of a number of possible states.
    - known, indicating that the query has been created, but not yet run, or queued for storage.
    - queued, which indicates that a query is going to be executed in future
    - executing, for queries which are currently running in FlowDB
    - executed, indicating that the query has finished running successfully
    - is_errored, when a query has been run but failed to succeed
    - cancelled, when execution was terminated by the user
    - resetting, when a previously run query is being purged from cache

    When the query is in a queued, executing, or resetting state, methods which need
    to use the results of the query should wait. The `wait_until_complete` method
    will block while the query is in any of these states.

    The initial state for the query is 'known'.

    Parameters
    ----------
    redis_client : StrictRedis
        Client for redis
    query_id : str
        md5 query identifier

    Notes
    -----
    Creating a new instance of a state machine for a query will not alter the state, as
    the state is persisted in redis.

    """

    def __init__(self, redis_client: StrictRedis, query_id: str):
        self.redis_client = redis_client
        self.query_id = query_id
        self.state_machine = Finist(redis_client, f"{query_id}-state", QueryState.KNOWN)
        self.state_machine.on(QueryEvent.QUEUE, QueryState.KNOWN, QueryState.QUEUED)
        self.state_machine.on(
            QueryEvent.EXECUTE, QueryState.QUEUED, QueryState.EXECUTING
        )
        self.state_machine.on(
            QueryEvent.ERROR, QueryState.EXECUTING, QueryState.ERRORED
        )
        self.state_machine.on(
            QueryEvent.FINISH, QueryState.EXECUTING, QueryState.COMPLETED
        )
        self.state_machine.on(
            QueryEvent.CANCEL, QueryState.QUEUED, QueryState.CANCELLED
        )
        self.state_machine.on(
            QueryEvent.CANCEL, QueryState.EXECUTING, QueryState.CANCELLED
        )
        self.state_machine.on(
            QueryEvent.RESET, QueryState.CANCELLED, QueryState.RESETTING
        )
        self.state_machine.on(
            QueryEvent.RESET, QueryState.ERRORED, QueryState.RESETTING
        )
        self.state_machine.on(
            QueryEvent.RESET, QueryState.COMPLETED, QueryState.RESETTING
        )
        self.state_machine.on(
            QueryEvent.RESET, QueryState.COMPLETED, QueryState.RESETTING
        )
        self.state_machine.on(
            QueryEvent.FINISH_RESET, QueryState.RESETTING, QueryState.KNOWN
        )

    @property
    def current_query_state(self) -> QueryState:
        return QueryState(self.state_machine.state().decode())

    @property
    def is_executing(self) -> bool:
        return self.current_query_state == QueryState.EXECUTING

    @property
    def is_queued(self) -> bool:
        return self.current_query_state == QueryState.QUEUED

    @property
    def is_completed(self) -> bool:
        return self.current_query_state == QueryState.COMPLETED

    @property
    def is_finished_executing(self) -> bool:
        return (self.current_query_state == QueryState.COMPLETED) or (
            self.current_query_state == QueryState.ERRORED
        )

    @property
    def is_errored(self) -> bool:
        return self.current_query_state == QueryState.ERRORED

    @property
    def is_resetting(self) -> bool:
        return self.current_query_state == QueryState.RESETTING

    @property
    def is_known(self) -> bool:
        return self.current_query_state == QueryState.KNOWN

    @property
    def is_cancelled(self) -> bool:
        return self.current_query_state == QueryState.CANCELLED

    def trigger_event(self, event: QueryEvent) -> Tuple[QueryState, bool]:
        """
        Attempts to trigger a state transition - will only transition if a
        transition is possible given the current state of the query.

        Parameters
        ----------
        event: QueryEvent
            Event to trigger

        Returns
        -------
        tuple of QueryState, bool
            Returns a tuple of the new query state, and a bool indicating whether this method
            call caused a transition to that state

        """
        state, trigger_success = self.state_machine.trigger(event)
        return QueryState(state.decode()), trigger_success

    def cancel(self):
        return self.trigger_event(QueryEvent.CANCEL)

    def enqueue(self):
        return self.trigger_event(QueryEvent.QUEUE)

    def error(self):
        return self.trigger_event(QueryEvent.ERROR)

    def execute(self):
        return self.trigger_event(QueryEvent.EXECUTE)

    def finish(self):
        return self.trigger_event(QueryEvent.FINISH)

    def reset(self):
        return self.trigger_event(QueryEvent.RESET)

    def finish_reset(self):
        return self.trigger_event(QueryEvent.FINISH_RESET)

    def wait_until_complete(self):
        """
        Blocks while the query is in any state that makes how to get
        the result of it indeterminate (i.e. currently running, resetting,
        or scheduled to run).

        Returns
        -------
        bool
            True if the query has been executed successfully and is in cache.
        """
        if self.is_executing or self.is_queued or self.is_resetting:
            while not (
                self.is_finished_executing or self.is_cancelled or self.is_known
            ):
                _sleep(1)

        return self.is_completed
