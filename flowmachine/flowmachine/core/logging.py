# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import structlog
import rapidjson
import logging

__all__ = ["init_logging", "set_log_level"]


def init_logging():
    """
    Initialise root logger 'flowmachine' and sub-logger 'flowmachine.debug',
    and configure structlog so that it passes any messages on to the standard
    library loggers.
    """
    root_logger = logging.getLogger("flowmachine")
    root_logger.setLevel(logging.DEBUG)

    debug_logger = logging.getLogger("flowmachine").getChild("debug")
    debug_logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    debug_logger.addHandler(ch)
    debug_logger.info(f"Debug logger created.")

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
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


def set_log_level(log_level):
    """
    Set logging level for logger `flowmachine.debug`.

    Parameters
    ----------
    log_level : str
        Level to emit logs at

    Returns
    -------
    None
    """
    try:
        log_level = logging.getLevelName(log_level.upper())
        log_level + 1
    except (AttributeError, TypeError):
        log_level = logging.ERROR
    true_log_level = logging.getLevelName(log_level)
    logger = logging.getLogger("flowmachine").getChild("debug")
    logger.setLevel(true_log_level)
    for h in logger.handlers:
        h.setLevel(log_level)
    logger.info(f"Logging level for logger 'flowmachine.debug' set to {true_log_level}")
