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
    daily_location,
    modal_location,
    modal_location_from_dates,
    location_event_counts,
    flows,
    meaningful_locations_aggregate,
    meaningful_locations_between_dates_od_matrix,
    meaningful_locations_between_label_od_matrix,
    get_geography,
    get_result,
    get_result_by_query_id,
    get_status,
    query_is_ready,
    run_query,
    get_available_dates,
    unique_subscriber_counts,
    location_introversion,
    total_network_objects,
    aggregate_network_objects,
    spatial_aggregate,
    joined_spatial_aggregate,
    radius_of_gyration,
    unique_location_counts,
    subscriber_degree,
    topup_amount,
    topup_balance,
    event_count,
    displacement,
    pareto_interactions,
    nocturnal_events,
    handset,
    random_sample,
)

__all__ = [
    "Connection",
    "connect",
    "daily_location",
    "modal_location",
    "modal_location_from_dates",
    "location_event_counts",
    "meaningful_locations_between_dates_od_matrix",
    "meaningful_locations_between_label_od_matrix",
    "meaningful_locations_aggregate",
    "flows",
    "get_geography",
    "get_result",
    "get_result_by_query_id",
    "get_status",
    "query_is_ready",
    "run_query",
    "get_available_dates",
    "unique_subscriber_counts",
    "location_introversion",
    "total_network_objects",
    "aggregate_network_objects",
    "spatial_aggregate",
    "joined_spatial_aggregate",
    "radius_of_gyration",
    "unique_location_counts",
    "subscriber_degree",
    "topup_amount",
    "topup_balance",
    "event_count",
    "displacement",
    "pareto_interactions",
    "nocturnal_events",
    "handset",
    "random_sample",
]
