# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow_oneofschema import OneOfSchema

from flowmachine.core.server.query_schemas.spatial_aggregate import (
    SpatialAggregateSchema,
    SpatialAggregateExposed,
)
from .dummy_query import DummyQuerySchema, DummyQueryExposed
from .daily_location import DailyLocationSchema, DailyLocationExposed
from .modal_location import ModalLocationSchema, ModalLocationExposed
from .flows import FlowsSchema, FlowsExposed
from .meaningful_locations import (
    MeaningfulLocationsAggregateSchema,
    MeaningfulLocationsAggregateExposed,
    MeaningfulLocationsBetweenLabelODMatrixSchema,
    MeaningfulLocationsBetweenLabelODMatrixExposed,
    MeaningfulLocationsBetweenDatesODMatrixSchema,
    MeaningfulLocationsBetweenDatesODMatrixExposed,
)
from .geography import GeographySchema, GeographyExposed
from .location_event_counts import LocationEventCountsSchema, LocationEventCountsExposed
from .unique_subscriber_counts import (
    UniqueSubscriberCountsSchema,
    UniqueSubscriberCountsExposed,
)
from .location_introversion import (
    LocationIntroversionSchema,
    LocationIntroversionExposed,
)
from .total_network_objects import TotalNetworkObjectsSchema, TotalNetworkObjectsExposed
from .aggregate_network_objects import (
    AggregateNetworkObjectsSchema,
    AggregateNetworkObjectsExposed,
)
from .dfs_metric_total_amount import (
    DFSTotalMetricAmountSchema,
    DFSTotalMetricAmountExposed,
)


class FlowmachineQuerySchema(OneOfSchema):
    type_field = "query_kind"
    type_schemas = {
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
    }
