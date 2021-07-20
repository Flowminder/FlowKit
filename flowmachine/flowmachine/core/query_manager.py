import datetime
import structlog
from contextlib import contextmanager

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)

from flowmachine.core.context import get_redis, get_interpreter_id


@contextmanager
def managing(query_id):
    set_managing(query_id)
    yield
    unset_managing(query_id)


def set_managing(query_id):
    logger.debug("Setting manager.", query_id=query_id)
    get_redis().hset("manager", query_id, get_interpreter_id())
    get_redis().hset(
        f"managing:{get_interpreter_id()}",
        query_id,
        datetime.datetime.now().isoformat(),
    )
    logger.debug("Set manager.", query_id=query_id)


def unset_managing(query_id):
    logger.debug("Releasing manager.", query_id=query_id)
    get_redis().hdel("manager", query_id)
    get_redis().hdel(f"managing:{get_interpreter_id()}", query_id)
    logger.debug("Released manager.", query_id=query_id)


def get_managed():
    return get_redis().hgetall(f"managing:{get_interpreter_id()}")


def get_manager(query_id):
    get_redis().hget("manager", query_id)
