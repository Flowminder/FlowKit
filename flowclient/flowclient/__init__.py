# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
FlowClient is a Python client to FlowAPI.
"""

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

from flowclient.api_query import APIQuery
from .connection import Connection
from flowclient.client import connect

from flowclient.async_api_query import ASyncAPIQuery
from .async_connection import ASyncConnection
from flowclient.async_client import connect_async


from .client import (
    get_geography,
    get_result,
    get_result_by_query_id,
    get_geojson_result,
    get_status,
    query_is_ready,
    run_query,
    get_available_dates,
)
from .query_specs import (
    daily_location_spec,
    modal_location_spec,
    modal_location_from_dates_spec,
    radius_of_gyration_spec,
    unique_location_counts_spec,
    topup_balance_spec,
    subscriber_degree_spec,
    topup_amount_spec,
    event_count_spec,
    displacement_spec,
    pareto_interactions_spec,
    nocturnal_events_spec,
    handset_spec,
    random_sample_spec,
    unique_locations_spec,
    most_frequent_location_spec,
    total_active_periods_spec,
    location_visits_spec,
    majority_location_spec,
    coalesced_location_spec,
    mobility_classification_spec,
)
from . import aggregates
from .aggregates import (
    location_event_counts,
    meaningful_locations_aggregate,
    meaningful_locations_between_label_od_matrix,
    meaningful_locations_between_dates_od_matrix,
    flows,
    inflows,
    outflows,
    unique_subscriber_counts,
    location_introversion,
    total_network_objects,
    aggregate_network_objects,
    spatial_aggregate,
    joined_spatial_aggregate,
    histogram_aggregate,
    active_at_reference_location_counts,
    unmoving_at_reference_location_counts,
    unmoving_counts,
    consecutive_trips_od_matrix,
    trips_od_matrix,
    labelled_spatial_aggregate,
    labelled_flows,
)

__all__ = [
    "aggregates",
    "connect_async",
    "connect",
    "get_geography",
    "get_result",
    "get_result_by_query_id",
    "get_geojson_result",
    "get_status",
    "query_is_ready",
    "run_query",
    "get_available_dates",
    "APIQuery",
    "ASyncAPIQuery",
    "location_event_counts",
    "meaningful_locations_aggregate",
    "meaningful_locations_between_label_od_matrix",
    "meaningful_locations_between_dates_od_matrix",
    "flows",
    "inflows",
    "outflows",
    "unique_subscriber_counts",
    "location_introversion",
    "total_network_objects",
    "aggregate_network_objects",
    "spatial_aggregate",
    "joined_spatial_aggregate",
    "histogram_aggregate",
    "active_at_reference_location_counts",
    "unique_locations_spec",
    "unmoving_at_reference_location_counts",
    "unmoving_counts",
    "consecutive_trips_od_matrix",
    "trips_od_matrix",
    "labelled_spatial_aggregate",
    "labelled_flows",
]
