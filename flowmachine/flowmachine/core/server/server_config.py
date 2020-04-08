# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
from concurrent.futures.thread import ThreadPoolExecutor

from typing import NamedTuple


def get_env_as_bool(env_var: str) -> bool:
    """
    Return a boolean true if the named env var is set to 'true' with any casing, and
    return False if it is set to anything else or not set at all.

    Parameters
    ----------
    env_var : str
        Name of the environment variable

    Returns
    -------
    bool

    """
    try:
        return "true" == os.environ[env_var].lower()
    except (KeyError, AttributeError):
        return False


class FlowmachineServerConfig(NamedTuple):
    """
    A namedtuple for passing server config options within the Flowmachine server.

    Attributes
    ----------
    port : int
        Port on which to listen for messages
    debug_mode : bool
        True to enable asyncio's debugging mode
    store_dependencies : bool
        If True, store a query's dependencies when running the query
    cache_pruning_frequency : int
        Number of seconds to wait between cache shrinks (if negative, no shrinks will ever be done).
    cache_pruning_timeout : int
        Maximum number of seconds to wait for a cache pruning operation to complete.
    server_thread_pool : ThreadPoolExecutor
        Server's threadpool for managing blocking tasks
    """

    port: int
    debug_mode: bool
    store_dependencies: bool
    cache_pruning_frequency: int
    cache_pruning_timeout: int
    server_thread_pool: ThreadPoolExecutor


def get_server_config() -> FlowmachineServerConfig:
    """
    Read config options from environment variables.

    Returns
    -------
    FlowMachineServerConfig
        A namedtuple containing the config options
    """
    port = int(os.getenv("FLOWMACHINE_PORT", 5555))
    debug_mode = get_env_as_bool("FLOWMACHINE_SERVER_DEBUG_MODE")
    store_dependencies = not get_env_as_bool(
        "FLOWMACHINE_SERVER_DISABLE_DEPENDENCY_CACHING"
    )
    cache_pruning_frequency = int(
        os.getenv("FLOWMACHINE_CACHE_PRUNING_FREQUENCY", 86400)
    )
    cache_pruning_timeout = int(os.getenv("FLOWMACHINE_CACHE_PRUNING_TIMEOUT", 600))
    thread_pool_size = os.getenv("FLOWMACHINE_SERVER_THREADPOOL_SIZE", None)
    try:
        thread_pool_size = int(thread_pool_size)
    except (TypeError, ValueError):
        thread_pool_size = None  # Not an int

    return FlowmachineServerConfig(
        port=port,
        debug_mode=debug_mode,
        store_dependencies=store_dependencies,
        cache_pruning_frequency=cache_pruning_frequency,
        cache_pruning_timeout=cache_pruning_timeout,
        server_thread_pool=ThreadPoolExecutor(max_workers=thread_pool_size),
    )
