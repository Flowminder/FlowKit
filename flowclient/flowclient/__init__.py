# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
FlowClient is a Python client to FlowAPI.
"""

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

from .client import (
    Connection,
    connect,
    daily_location_spec,
    modal_location_spec,
    modal_location_from_dates_spec,
    get_geography,
    get_result,
    get_result_by_query_id,
    get_status,
    query_is_ready,
    run_query,
    get_available_dates,
    radius_of_gyration_spec,
    unique_location_counts_spec,
    subscriber_degree_spec,
    topup_amount_spec,
    topup_balance_spec,
    event_count_spec,
    displacement_spec,
    pareto_interactions_spec,
    nocturnal_events_spec,
    handset_spec,
    random_sample_spec,
)
from . import aggregates

__all__ = [
    "aggregates",
    "Connection",
    "connect",
    "daily_location_spec",
    "modal_location_spec",
    "modal_location_from_dates_spec",
    "get_geography",
    "get_result",
    "get_result_by_query_id",
    "get_status",
    "query_is_ready",
    "run_query",
    "get_available_dates",
    "radius_of_gyration_spec",
    "unique_location_counts_spec",
    "subscriber_degree_spec",
    "topup_amount_spec",
    "topup_balance_spec",
    "event_count_spec",
    "displacement_spec",
    "pareto_interactions_spec",
    "nocturnal_events_spec",
    "handset_spec",
    "random_sample_spec",
]
