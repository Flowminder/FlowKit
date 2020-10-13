# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
import rapidjson
import structlog
import sys
from flowmachine.core.context import get_action_request

__all__ = ["init_logging", "set_log_level"]

FLOWKIT_LOGGERS_HAVE_BEEN_INITIALISED = False


def action_request_processor(_, __, event_dict):
    try:
        action_request = get_action_request()
        event_dict = dict(
            **event_dict,
            action_request=dict(
                request_id=action_request.request_id,
                action=action_request.action,
            ),
        )
    except LookupError:
        pass
    return event_dict


def init_logging():
    """
    Initialise root logger 'flowmachine' and sub-logger 'flowmachine.debug',
    and configure structlog so that it passes any messages on to the standard
    library loggers.
    """
    global FLOWKIT_LOGGERS_HAVE_BEEN_INITIALISED
    if FLOWKIT_LOGGERS_HAVE_BEEN_INITIALISED:
        # Only initialise loggers once, to avoid adding multiple
        # handlers and accidentally re-setting the log level.
        return

    root_logger = logging.getLogger("flowmachine")
    root_logger.setLevel(logging.DEBUG)

    debug_logger = logging.getLogger("flowmachine").getChild("debug")
    debug_logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    debug_logger.addHandler(ch)

    # Logger for all queries run or accessed (used by flowmachine server)
    query_run_log = logging.getLogger("flowmachine").getChild("query_run_log")
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    query_run_log.addHandler(ch)

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            action_request_processor,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(serializer=rapidjson.dumps),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    FLOWKIT_LOGGERS_HAVE_BEEN_INITIALISED = True


def set_log_level(logger_name: str, log_level: str) -> None:
    """
    Set log level for logger `flowmachine.debug`.

    Parameters
    ----------
    logger_name : str
        Name of the logger for which to set the log level.
    log_level : str
        Level to emit logs at. This must be one of the standard
        pre-defined log levels ("DEBUG", "INFO", "ERROR", etc.).
        If an invalid log level is passed, "ERROR" will be used.

    Returns
    -------
    None
    """
    user_provided_log_level = log_level.upper()
    logger = logging.getLogger(logger_name)
    try:
        logger.setLevel(user_provided_log_level)
        true_log_level = user_provided_log_level
        user_provided_log_level_is_valid = True
    except ValueError:
        true_log_level = "ERROR"
        user_provided_log_level_is_valid = False
        logger.setLevel(true_log_level)

    for handler in logger.handlers:
        handler.setLevel(true_log_level)

    if not user_provided_log_level_is_valid:
        logger.error(
            f"Invalid user-provided log level: '{user_provided_log_level}', using '{true_log_level}' instead."
        )
    logger.info(f"Log level for logger '{logger_name}' set to '{true_log_level}'.")
