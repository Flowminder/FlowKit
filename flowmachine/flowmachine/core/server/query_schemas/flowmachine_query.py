# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from functools import lru_cache

from apispec import APISpec
from apispec_oneofschema import MarshmallowPlugin

from flowmachine.core.server.query_schemas.joined_spatial_aggregate import (
    JoinedSpatialAggregateSchema,
)
from flowmachine.core.server.query_schemas.spatial_aggregate import (
    SpatialAggregateSchema,
)
from .histogram_aggregate import HistogramAggregateSchema
from .active_at_reference_location_counts import ActiveAtReferenceLocationCountsSchema
from .consecutive_trips_od_matrix import ConsecutiveTripsODMatrixSchema
from .dummy_query import DummyQuerySchema
from .flows import FlowsSchema
from .outflows import OutflowsSchema
from .inflows import InflowsSchema
from .meaningful_locations import (
    MeaningfulLocationsAggregateSchema,
    MeaningfulLocationsBetweenLabelODMatrixSchema,
    MeaningfulLocationsBetweenDatesODMatrixSchema,
)

from .aggregate_network_objects import AggregateNetworkObjectsSchema

from .geography import GeographySchema
from .location_event_counts import LocationEventCountsSchema
from .trips_od_matrix import TripsODMatrixSchema
from .unique_subscriber_counts import UniqueSubscriberCountsSchema
from .location_introversion import LocationIntroversionSchema
from .total_network_objects import TotalNetworkObjectsSchema
from .dfs_metric_total_amount import DFSTotalMetricAmountSchema
from .unique_visitor_counts import UniqueVisitorCountsSchema
from .unmoving_at_reference_location_counts import (
    UnmovingAtReferenceLocationCountsSchema,
)
from .unmoving_counts import UnmovingCountsSchema
from .labelled_spatial_aggregate import LabelledSpatialAggregateSchema
from .labelled_flows import LabelledFlowsSchema
from .one_of_query import OneOfQuerySchema


class FlowmachineQuerySchema(OneOfQuerySchema):
    """
    Schema for top-level queries exposed through FlowAPI
    """

    query_schemas = (
        DummyQuerySchema,
        FlowsSchema,
        InflowsSchema,
        OutflowsSchema,
        MeaningfulLocationsAggregateSchema,
        MeaningfulLocationsBetweenLabelODMatrixSchema,
        MeaningfulLocationsBetweenDatesODMatrixSchema,
        GeographySchema,
        LocationEventCountsSchema,
        UniqueSubscriberCountsSchema,
        LocationIntroversionSchema,
        TotalNetworkObjectsSchema,
        AggregateNetworkObjectsSchema,
        DFSTotalMetricAmountSchema,
        SpatialAggregateSchema,
        JoinedSpatialAggregateSchema,
        HistogramAggregateSchema,
        ActiveAtReferenceLocationCountsSchema,
        UniqueVisitorCountsSchema,
        ConsecutiveTripsODMatrixSchema,
        UnmovingCountsSchema,
        UnmovingAtReferenceLocationCountsSchema,
        TripsODMatrixSchema,
        LabelledSpatialAggregateSchema,
        LabelledFlowsSchema,
    )


@lru_cache(maxsize=1)
def get_query_schema() -> dict:
    """
    Get a dictionary representation of the FlowmachineQuerySchema api spec.
    This will contain a schema which defines all valid query types that can be run.

    Returns
    -------
    dict

    """
    spec = APISpec(
        title="FlowAPI",
        version="1.0.0",
        openapi_version="3.0.2",
        plugins=[MarshmallowPlugin()],
    )
    spec.components.schema("FlowmachineQuerySchema", schema=FlowmachineQuerySchema)
    return spec.to_dict()["components"]["schemas"]
