# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from contextvars import copy_context

import asyncio
from concurrent.futures import Executor

import structlog
from functools import partial
from typing import Optional

import flowmachine
from flowmachine.core import Connection
from flowmachine.core.cache import shrink_below_size
from flowmachine.core.context import get_db, get_executor
from flowmachine.core.server.server_config import get_server_config

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


async def watch_and_shrink_cache(
    *,
    flowdb_connection: "Connection",
    pool: Executor,
    sleep_time: int = 86400,
    timeout: Optional[int] = 600,
    loop: bool = True,
    size_threshold: int = None,
    dry_run: bool = False,
    protected_period: Optional[int] = None,
) -> None:
    """
    Background task to periodically trigger a shrink of the cache.

    Parameters
    ----------
    flowdb_connection : Connection
        Flowdb connection to check dates on
    pool : Executor
        Executor to run the date check with
    sleep_time : int, default 86400
        Number of seconds to sleep for between checks
    timeout : int or None, default 600
        Seconds to wait for a cache shrink to complete before cancelling it
    loop : bool, default True
        Set to false to return after the first check
    size_threshold : int, default None
        Optionally override the maximum cache size set in flowdb.
    dry_run : bool, default False
        Set to true to just report the objects that would be removed and not remove them
    protected_period : int, default None
        Optionally specify a number of seconds within which cache entries are excluded. If None,
        the value stored in cache.cache_config will be used.Set to a negative number to ignore cache protection
        completely.

    Returns
    -------
    None

    """
    shrink_func = partial(
        shrink_below_size,
        connection=flowdb_connection,
        size_threshold=size_threshold,
        dry_run=dry_run,
        protected_period=protected_period,
    )
    while True:
        logger.debug("Checking if cache should be shrunk.")

        try:  # Set the shrink function running with a copy of the current execution context (db conn etc) in background thread
            await asyncio.wait_for(
                asyncio.get_running_loop().run_in_executor(
                    pool, copy_context().run, shrink_func
                ),
                timeout=timeout,
            )
        except (TimeoutError, asyncio.exceptions.TimeoutError):
            logger.error(
                f"Failed to complete cache shrink within {timeout}s. Trying again in {sleep_time}s."
            )
        if not loop:
            break
        await asyncio.sleep(sleep_time)


def main():
    # Read config options from environment variables
    config = get_server_config()
    # Connect to flowdb
    flowmachine.connect()

    if config.debug_mode:
        logger.info(
            "Enabling asyncio's debugging mode.",
            sleep_time=config.cache_pruning_frequency,
            timeout=config.cache_pruning_timeout,
        )

    logger.info(
        "Starting cache cleanup service.",
    )
    # Run loop which periodically checks if the cache can/should be resized
    asyncio.run(
        watch_and_shrink_cache(
            flowdb_connection=get_db(),
            pool=get_executor(),
            sleep_time=config.cache_pruning_frequency,
            timeout=config.cache_pruning_timeout,
        ),
        debug=config.debug_mode,
    )  # note: asyncio.run() requires Python 3.7+


if __name__ == "__main__":
    main()
