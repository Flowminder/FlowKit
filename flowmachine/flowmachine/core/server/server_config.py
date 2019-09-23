# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
from typing import NamedTuple


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
    """

    port: int
    debug_mode: bool
    store_dependencies: bool


def get_server_config() -> FlowmachineServerConfig:
    """
    Read config options from environment variables.

    Returns
    -------
    FlowMachineServerConfig
        A namedtuple containing the config options
    """
    port = int(os.getenv("FLOWMACHINE_PORT", 5555))
    debug_mode = "true" == os.getenv("FLOWMACHINE_SERVER_DEBUG_MODE", "false").lower()
    store_dependencies = not (
        "true"
        == os.getenv("FLOWMACHINE_SERVER_DISABLE_DEPENDENCY_CACHING", "false").lower()
    )

    return FlowmachineServerConfig(
        port=port, debug_mode=debug_mode, store_dependencies=store_dependencies
    )
