# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowmachine.core.server.query_schemas.active_at_reference_location_counts import (
    ActiveAtReferenceLocationCountsSchema,
)
from flowmachine.core.server.query_schemas.aggregate_network_objects import (
    AggregateNetworkObjectsSchema,
)
from flowmachine.core.server.query_schemas.consecutive_trips_od_matrix import (
    ConsecutiveTripsODMatrixSchema,
)
from flowmachine.core.server.query_schemas.dfs_metric_total_amount import (
    DFSTotalMetricAmountSchema,
)
from flowmachine.core.server.query_schemas.dummy_query import DummyQuerySchema
from flowmachine.core.server.query_schemas.flows import FlowsSchema
from flowmachine.core.server.query_schemas.geography import GeographySchema
from flowmachine.core.server.query_schemas.histogram_aggregate import (
    HistogramAggregateSchema,
)
from flowmachine.core.server.query_schemas.joined_spatial_aggregate import (
    JoinedSpatialAggregateSchema,
)
from flowmachine.core.server.query_schemas.location_event_counts import (
    LocationEventCountsSchema,
)
from flowmachine.core.server.query_schemas.location_introversion import (
    LocationIntroversionSchema,
)
from flowmachine.core.server.query_schemas.meaningful_locations import (
    MeaningfulLocationsAggregateSchema,
    MeaningfulLocationsBetweenLabelODMatrixSchema,
    MeaningfulLocationsBetweenDatesODMatrixSchema,
)
from flowmachine.core.server.query_schemas.spatial_aggregate import (
    SpatialAggregateSchema,
)
from flowmachine.core.server.query_schemas.total_network_objects import (
    TotalNetworkObjectsSchema,
)
from flowmachine.core.server.query_schemas.trips_od_matrix import TripsODMatrixSchema
from flowmachine.core.server.query_schemas.unique_subscriber_counts import (
    UniqueSubscriberCountsSchema,
)
from flowmachine.core.server.query_schemas.unique_visitor_counts import (
    UniqueVisitorCountsSchema,
)
from flowmachine.core.server.query_schemas.unmoving_at_reference_location_counts import (
    UnmovingAtReferenceLocationCountsSchema,
)
from flowmachine.core.server.query_schemas.unmoving_counts import UnmovingCountsSchema

top_level_queries = {
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
    "histogram_aggregate": HistogramAggregateSchema,
    "active_at_reference_location_counts": ActiveAtReferenceLocationCountsSchema,
    "unique_visitor_counts": UniqueVisitorCountsSchema,
    "consecutive_trips_od_matrix": ConsecutiveTripsODMatrixSchema,
    "unmoving_counts": UnmovingCountsSchema,
    "unmoving_at_reference_location_counts": UnmovingAtReferenceLocationCountsSchema,
    "trips_od_matrix": TripsODMatrixSchema,
}
