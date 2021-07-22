from typing import List

import datetime
import structlog
from contextlib import contextmanager

from flowmachine.core.context import get_redis, get_interpreter_id

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


@contextmanager
def managing(query_id: str, db_id: str):
    set_managing(query_id, db_id)
    yield
    unset_managing(query_id, db_id)


def set_managing(query_id: str, db_id: str) -> None:
    logger.debug("Setting manager.", query_id=query_id, db_id=db_id)
    get_redis().hset("manager", f"{db_id}-{query_id}", get_interpreter_id())
    get_redis().hset(
        f"managing:{get_interpreter_id()}",
        f"{db_id}-{query_id}",
        datetime.datetime.now().isoformat(),
    )
    logger.debug("Set manager.", query_id=query_id, db_id=db_id)


def unset_managing(query_id: str, db_id: str) -> None:
    logger.debug("Releasing manager.", query_id=query_id, db_id=db_id)
    get_redis().hdel("manager", f"{db_id}-{query_id}")
    get_redis().hdel(f"managing:{get_interpreter_id()}", f"{db_id}-{query_id}")
    logger.debug("Released manager.", query_id=query_id, db_id=db_id)


def get_managed() -> List[str]:
    return [
        k.decode()
        for k in get_redis().hgetall(f"managing:{get_interpreter_id()}").keys()
    ]


def get_manager(query_id: str, connection_id: str) -> str:
    get_redis().hget("manager", f"{connection_id}-{query_id}").decode()


def release_managed() -> None:
    from flowmachine.core.query_state import QueryStateMachine

    logger.error("Releasing managed queries.")
    for query_id in get_managed():
        conn_id, query_id = query_id.split("-")
        qsm = QueryStateMachine(
            db_id=conn_id,
            query_id=query_id,
            redis_client=get_redis(),
        )
        qsm.raise_error()
        qsm.cancel()
        qsm.reset()
        qsm.finish_resetting()
        unset_managing(query_id, conn_id)
