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

    QUEUED = ("queued", "is queued")
    COMPLETED = ("completed", "is ready")
    CANCELLED = ("cancelled", "was cancelled")
    EXECUTING = ("executing", "is currently running")
    ERRORED = ("errored", "finished with an error")
    RESETTING = ("resetting", "is being reset")
    KNOWN = ("known", "is known, but has not yet been run")

    def __new__(cls, name, desc, **kwargs):
        obj = str.__new__(cls, name)
        obj._value_ = name
        obj._description = desc
        return obj

    @property
    def description(self) -> str:
        """

        Returns
        -------
        str
            Long form description of this state suitable for use in error messages.
        """
        return self._description


class QueryEvent(str, Enum):
    """
    Events that trigger a transition to a new state.
    """

    EXECUTE = "execute"
    FINISH = "finish"
    FINISH_RESET = "finish_resetting"
    CANCEL = "cancel"
    RESET = "reset"
    ERROR = "error"
    QUEUE = "queue"


class QueryStateMachine:
    """
    Implements a state machine for a query's lifecycle, backed by redis.

    Each query, once instantiated, is in one of a number of possible states.
    - known, indicating that the query has been created, but not yet run, or queued for storage.
    - queued, which indicates that a query is going to be executed in future (i.e. `store` has been called on it)
    - executing, for queries which are currently running in FlowDB
    - completed, indicating that the query has finished running successfully
    - errored, when a query has been run but failed to succeed
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
        Unique query identifier
    db_id : str
        FlowDB connection id

    Notes
    -----
    Creating a new instance of a state machine for a query will not alter the state, as
    the state is persisted in redis.

    """

    def __init__(self, redis_client: StrictRedis, query_id: str, db_id: str):
        self.query_id = query_id
        must_populate = redis_client.get(f"finist:{db_id}:{query_id}-state") is None
        self.state_machine = Finist(
            redis_client, f"{db_id}:{query_id}-state", QueryState.KNOWN
        )
        if must_populate:  # Need to create the state machine for this query
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
        """

        Returns
        -------
        QueryState
            Current state of the query this state machine refers to
        """
        return QueryState(self.state_machine.state().decode())

    @property
    def is_executing(self) -> bool:
        """
        Returns
        -------
        bool
            True if the query is currently running

        """
        return self.current_query_state == QueryState.EXECUTING

    @property
    def is_queued(self) -> bool:
        """
        Returns
        -------
        bool
            True if the the query's store method has been called and it has not begin running

        """
        return self.current_query_state == QueryState.QUEUED

    @property
    def is_completed(self) -> bool:
        """
        Returns
        -------
        bool
            True if the query ran successfully and is in cache

        """
        return self.current_query_state == QueryState.COMPLETED

    @property
    def is_finished_executing(self) -> bool:
        """
        True if the query state is `completed` or `errored`. I.e., it was previously
        executing and is now finished either successfully or with a failure (but it
        wasn't cancelled manually).
        """
        return (self.current_query_state == QueryState.COMPLETED) or (
            self.current_query_state == QueryState.ERRORED
        )

    @property
    def is_errored(self) -> bool:
        """
        Returns
        -------
        bool
            True if the query failed to run with an error

        """
        return self.current_query_state == QueryState.ERRORED

    @property
    def is_resetting(self) -> bool:
        """
        Returns
        -------
        bool
            True if the query is currently being removed from cache, or recovered from cancellation or error

        """
        return self.current_query_state == QueryState.RESETTING

    @property
    def is_known(self) -> bool:
        """
        Returns
        -------
        bool
            True if the query has not been set running and may be queued to do so

        """
        return self.current_query_state == QueryState.KNOWN

    @property
    def is_cancelled(self) -> bool:
        """
        Returns
        -------
        bool
            True if the query was previously queued or running but was cancelled

        """
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
        """
        Attempt to mark the query as cancelled.

        Returns
        -------
        tuple of QueryState, bool
            Returns a tuple of the new query state, and a bool indicating whether the caller
            triggered the query to be cancelled with this call

        """
        return self.trigger_event(QueryEvent.CANCEL)

    def enqueue(self):
        """
        Attempt to mark the query as queued.

        Returns
        -------
        tuple of QueryState, bool
            Returns a tuple of the new query state, and a bool indicating whether the caller
            triggered the query to be queued with this call

        """
        return self.trigger_event(QueryEvent.QUEUE)

    def raise_error(self):
        """
        Attempt to mark the query as having errored while running.

        Returns
        -------
        tuple of QueryState, bool
            Returns a tuple of the new query state, and a bool indicating whether the caller
            marked the query as erroring with this call

        """
        return self.trigger_event(QueryEvent.ERROR)

    def execute(self):
        """
        Attempt to mark the query as in the process of executing.

        Returns
        -------
        tuple of QueryState, bool
            Returns a tuple of the new query state, and a bool indicating whether the caller
            marked the query as executing with this call

        """
        return self.trigger_event(QueryEvent.EXECUTE)

    def finish(self):
        """
        Attempt to mark the query as completed.

        Returns
        -------
        tuple of QueryState, bool
            Returns a tuple of the new query state, and a bool indicating whether the caller
            marked the query as finished with this call

        """
        return self.trigger_event(QueryEvent.FINISH)

    def reset(self):
        """
        Attempt to mark the query as in the process of resetting.

        Returns
        -------
        tuple of QueryState, bool
            Returns a tuple of the new query state, and a bool indicating whether the caller
            triggered the query to be reset with this call

        """
        return self.trigger_event(QueryEvent.RESET)

    def finish_resetting(self):
        """
        Attempt to mark the query as having finished resetting.

        Returns
        -------
        tuple of QueryState, bool
            Returns a tuple of the new query state, and a bool indicating whether the caller
           marked the reset as complete with this call.

        """
        return self.trigger_event(QueryEvent.FINISH_RESET)

    def wait_until_complete(self, sleep_duration=1):
        """
        Blocks until the query is in a state where its result is determinate
        (i.e., one of "know", "errored", "completed", "cancelled").

        """
        if self.is_executing or self.is_queued or self.is_resetting:
            while not (
                self.is_finished_executing or self.is_cancelled or self.is_known
            ):
                _sleep(sleep_duration)
