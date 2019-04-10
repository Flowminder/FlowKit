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
from .dfs_metric_total_amount import (
    DFSTotalMetricAmountSchema,
    DFSTotalMetricAmountExposed,
)


class FlowmachineQuerySchema(OneOfSchema):
    type_field = "query_kind"
    type_schemas = {
        "dummy_query": DummyQuerySchema,
        "daily_location": DailyLocationSchema,
        "modal_location": ModalLocationSchema,
        "flows": FlowsSchema,
        "meaningful_locations_aggregate": MeaningfulLocationsAggregateSchema,
        "meaningful_locations_between_label_od_matrix": MeaningfulLocationsBetweenLabelODMatrixSchema,
        "meaningful_locations_between_dates_od_matrix": MeaningfulLocationsBetweenDatesODMatrixSchema,
        "geography": GeographySchema,
        "location_event_counts": LocationEventCountsSchema,
        "unique_subscriber_counts": UniqueSubscriberCountsSchema,
        "location_introversion": LocationIntroversionSchema,
        "total_network_objects": TotalNetworkObjectsSchema,
        "dfs_metric_total_amount": DFSTotalMetricAmountSchema,
        "spatial_aggregate": SpatialAggregateSchema,
    }

    def get_obj_type(self, obj):
        if isinstance(obj, DummyQueryExposed):
            return "dummy_query"
        elif isinstance(obj, DailyLocationExposed):
            return "daily_location"
        elif isinstance(obj, ModalLocationExposed):
            return "modal_location"
        elif isinstance(obj, FlowsExposed):
            return "flows"
        elif isinstance(obj, MeaningfulLocationsAggregateExposed):
            return "meaningful_locations_aggregate"
        elif isinstance(obj, MeaningfulLocationsBetweenLabelODMatrixExposed):
            return "meaningful_locations_between_label_od_matrix"
        elif isinstance(obj, MeaningfulLocationsBetweenDatesODMatrixExposed):
            return "meaningful_locations_between_dates_od_matrix"
        elif isinstance(obj, GeographyExposed):
            return "geography"
        elif isinstance(obj, LocationEventCountsExposed):
            return "location_event_counts"
        elif isinstance(obj, UniqueSubscriberCountsExposed):
            return "unique_subscriber_counts"
        elif isinstance(obj, LocationIntroversionExposed):
            return "location_introversion"
        elif isinstance(obj, TotalNetworkObjectsExposed):
            return "total_network_objects"
        elif isinstance(obj, DFSTotalMetricAmountExposed):
            return "dfs_metric_total_amount"
        elif isinstance(obj, SpatialAggregateExposed):
            return "spatial_aggregate"
        else:
            raise ValueError(
                f"Object type '{obj.__class__.__name__}' not registered in FlowmachineQuerySchema."
            )
