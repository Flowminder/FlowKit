# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from .aggregate_network_objects import AggregateNetworkObjectsSchema
from .dfs_metric_total_amount import DFSTotalMetricAmountSchema
from .dummy_query import DummyQuerySchema
from .flows import FlowsSchema
from .geography import GeographySchema
from .joined_spatial_aggregate import JoinedSpatialAggregateSchema
from .location_event_counts import LocationEventCountsSchema
from .location_introversion import LocationIntroversionSchema
from .meaningful_locations import (
    MeaningfulLocationsAggregateSchema,
    MeaningfulLocationsBetweenLabelODMatrixSchema,
    MeaningfulLocationsBetweenDatesODMatrixSchema,
)
from .spatial_aggregate import SpatialAggregateSchema
from .total_network_objects import TotalNetworkObjectsSchema
from .unique_subscriber_counts import UniqueSubscriberCountsSchema

schemas = {
    "dummy_query": DummyQuerySchema,
    "flows": FlowsSchema,
    "meaningful_locations_aggregate": MeaningfulLocationsAggregateSchema,
    "meaningful_locations_between_label_od_matrix": MeaningfulLocationsBetweenLabelODMatrixSchema,
    "meaningful_locations_between_dates_od_matrix": MeaningfulLocationsBetweenDatesODMatrixSchema,
    "geography": GeographySchema,
    "location_event_counts": LocationEventCountsSchema,
    "unique_subscriber_counts": UniqueSubscriberCountsSchema,
    "location_introversion": LocationIntroversionSchema,
    "total_network_objects": TotalNetworkObjectsSchema,
    "aggregate_network_objects": AggregateNetworkObjectsSchema,
    "dfs_metric_total_amount": DFSTotalMetricAmountSchema,
    "spatial_aggregate": SpatialAggregateSchema,
    "joined_spatial_aggregate": JoinedSpatialAggregateSchema,
}
