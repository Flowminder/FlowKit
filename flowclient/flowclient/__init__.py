# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

from .client import (
    Connection,
    connect,
    daily_location,
    modal_location,
    modal_location_from_dates,
    flows,
    get_result,
    get_result_by_query_id,
    get_status,
    query_is_ready,
    run_query,
)

__all__ = [
    "Connection",
    "connect",
    "daily_location",
    "modal_location",
    "modal_location_from_dates",
    "flows",
    "get_result",
    "get_result_by_query_id",
    "get_status",
    "query_is_ready",
    "run_query",
]
