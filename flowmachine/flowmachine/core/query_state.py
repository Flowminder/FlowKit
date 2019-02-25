import logging
from enum import Enum
from time import sleep

from finist import Finist
from typing import Tuple

from redis import StrictRedis

logger = logging.getLogger("flowmachine").getChild(__name__)


class QueryState(Enum):
    QUEUED = "queued"
    EXECUTED = "executed"
    CANCELLED = "cancelled"
    EXECUTING = "executing"
    ERRORED = "errored"
    RESETTING = "resetting"
    KNOWN = "known"


class QueryEvent(Enum):
    EXECUTE = "execute"
    FINISH = "finish"
    FINISH_RESET = "finish_reset"
    CANCEL = "cancel"
    RESET = "reset"
    ERROR = "error"
    QUEUE = "queue"


class QueryStateMachine:
    def __init__(self, redis_client: StrictRedis, query_id: str):
        self.redis_client = redis_client
        self.query_id = query_id
        self.state_machine = Finist(
            redis_client, f"{query_id}-state", QueryState.KNOWN.value
        )
        self.state_machine.on(
            QueryEvent.QUEUE.value, QueryState.KNOWN.value, QueryState.QUEUED.value
        )
        self.state_machine.on(
            QueryEvent.EXECUTE.value,
            QueryState.QUEUED.value,
            QueryState.EXECUTING.value,
        )
        self.state_machine.on(
            QueryEvent.FINISH.value,
            QueryState.EXECUTING.value,
            QueryState.EXECUTED.value,
        )
        self.state_machine.on(
            QueryEvent.CANCEL.value, QueryState.QUEUED.value, QueryState.CANCELLED.value
        )
        self.state_machine.on(
            QueryEvent.CANCEL.value,
            QueryState.EXECUTING.value,
            QueryState.CANCELLED.value,
        )
        self.state_machine.on(
            QueryEvent.RESET.value,
            QueryState.CANCELLED.value,
            QueryState.RESETTING.value,
        )
        self.state_machine.on(
            QueryEvent.RESET.value, QueryState.ERRORED.value, QueryState.RESETTING.value
        )
        self.state_machine.on(
            QueryEvent.RESET.value,
            QueryState.EXECUTED.value,
            QueryState.RESETTING.value,
        )
        self.state_machine.on(
            QueryEvent.RESET.value,
            QueryState.EXECUTED.value,
            QueryState.RESETTING.value,
        )
        self.state_machine.on(
            QueryEvent.FINISH_RESET.value,
            QueryState.RESETTING.value,
            QueryState.KNOWN.value,
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
    def is_executed_without_error(self) -> bool:
        return self.current_query_state == QueryState.EXECUTED

    @property
    def is_executed(self) -> bool:
        return (self.current_query_state == QueryState.EXECUTED) or (
            self.current_query_state == QueryState.ERRORED
        )

    @property
    def errored(self) -> bool:
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
        state, trigger_success = self.state_machine.trigger(event.value)
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

    def block_while_executing(self):
        if self.is_executing or self.is_queued or self.is_resetting:
            while not self.is_executed:
                sleep(1)
        return self.is_executed_without_error
